#include "src/include/rbfm.h"

#include <cstdio>
#include <cstring>
#include <cmath>
#include <iostream>
#include <sstream>

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager &pfm = PagedFileManager::instance();
        return pfm.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        std::vector<bool> isNull = extractNullInformation(data, recordDescriptor);
        unsigned recordSize = getRecordSize(data, recordDescriptor, isNull);
        const char* record = (char*)createRecordStream(data, recordDescriptor, isNull, recordSize);

        unsigned numberOfSlots, freeSpace;
        unsigned offset, length;
        unsigned numPages = fileHandle.getNumberOfPages();
        int pageNum = (int)numPages - 1;
        bool read = false, inserted = false;
        char* page = new char[PAGE_SIZE];

        while (!inserted) {
            // create and append a new page
            if (numPages == 0) {
                // pointer to the start of directory ( --> | ([offset_1][length_1]) | [N][F] )
                unsigned directory = PAGE_SIZE - 4 * sizeof(unsigned); // the left-most directory slot
                memcpy(page, record, recordSize);
                offset = 0; // pointer to the start of the record, initialized at 0;
                length = recordSize;
                numberOfSlots = 1;
                freeSpace = PAGE_SIZE - recordSize - 4 * sizeof(unsigned);
                memcpy(page + directory, &offset, sizeof(int)); // store offset
                memcpy(page + directory + sizeof(int), &length,sizeof(int));
                memcpy(page + directory + 2 * sizeof(unsigned), &numberOfSlots, sizeof(unsigned));
                memcpy(page + directory + 3 * sizeof(unsigned), &freeSpace, sizeof(unsigned));
                fileHandle.appendPage(page);
                inserted = true;
                rid.slotNum = 1;
                pageNum++;
            }
            else {
                fileHandle.readPage(pageNum, page);
                memcpy(&freeSpace, page + PAGE_SIZE - sizeof(unsigned), sizeof(unsigned));
                memcpy(&numberOfSlots, page + PAGE_SIZE - 2 * sizeof(unsigned), sizeof(unsigned));
                unsigned directoryEnd = PAGE_SIZE - 2 * sizeof(unsigned) - numberOfSlots * 2 * sizeof(unsigned);
                unsigned slotToInsert = 0;
                bool canInsert = false;
                // first look through available slots: slotNum start from 1
                for (int i = 0; i < numberOfSlots; i++) {
                    unsigned slotLength;
                    memcpy(&slotLength, page + directoryEnd + i * (sizeof(unsigned) * 2) + sizeof(unsigned), sizeof(unsigned));
                    if (slotLength == 0) {
                        slotToInsert = numberOfSlots - i;
                        break;
                    }
                }
                // if there's space in rid.pageNum, insert there
                if (slotToInsert != 0) {
                    if (freeSpace >= recordSize) {
                        canInsert = true;
                    }
                }
                else if (freeSpace >= recordSize + 2 * sizeof(unsigned)) {
                    canInsert = true;
                }
                if (canInsert) {
                    // locate last byte array and insert after it
                    unsigned endOfRecords = directoryEnd - freeSpace;
                    memcpy(page + endOfRecords , record, recordSize);

                    // prepare to update directory and slot
                    offset = endOfRecords; // offset of the new array is (offset + length) of previous array
                    length = recordSize;
                    numberOfSlots += 1;
                    freeSpace -= (recordSize + 2 * sizeof(unsigned)); // record and slot

                    // update slot
                    if (slotToInsert != 0) {
                        memcpy(page + PAGE_SIZE - 2 * sizeof(unsigned) - slotToInsert * 2 * sizeof(unsigned), &offset, sizeof(int));
                        memcpy(page + PAGE_SIZE - 2 * sizeof(unsigned) - slotToInsert * 2 * sizeof(unsigned) + sizeof(unsigned), &length, sizeof(int));
                        rid.slotNum = slotToInsert;
                    }
                    else {
                        memcpy(page + directoryEnd - 2 * sizeof(unsigned), &offset, sizeof(int));
                        memcpy(page + directoryEnd - sizeof(unsigned), &length, sizeof(int));
                        rid.slotNum = numberOfSlots;
                    }
                    memcpy(page + PAGE_SIZE - 2 * sizeof(unsigned), &numberOfSlots, sizeof(unsigned));
                    memcpy(page + PAGE_SIZE - sizeof(unsigned), &freeSpace, sizeof(unsigned));
                    fileHandle.writePage(pageNum, page);
                    inserted = true;

                } else {
                    //if no enough space in last page, check first page
                    if (pageNum == (fileHandle.getNumberOfPages() - 1) && !read) {
                        pageNum = 0; // start from first page
                        read = true;
                    } //no space in all pages, append a new page
                    else if ((pageNum == fileHandle.getNumberOfPages()) && read) {
                        unsigned directory = PAGE_SIZE - 4 * sizeof(unsigned); // the left-most directory slot
                        memcpy(page, record, recordSize);
                        offset = 0; // pointer to the start of the record, initialized at 0;
                        length = recordSize;
                        numberOfSlots = 1;
                        freeSpace = PAGE_SIZE - recordSize - 4 * sizeof(unsigned);
                        memcpy(page + directory, &offset, sizeof(unsigned)); // store offset
                        memcpy(page + directory + sizeof(unsigned), &length,sizeof(unsigned));
                        memcpy(page + directory + 2 * sizeof(unsigned), &numberOfSlots, sizeof(unsigned));
                        memcpy(page + directory + 3 * sizeof(unsigned), &freeSpace, sizeof(unsigned));
                        fileHandle.appendPage(page);
                        inserted = true;
                        rid.slotNum = 1;
                    }
                    else {
                        pageNum++;
                    }
                }
            }
        }

        rid.pageNum = pageNum;
        delete[] record;
        delete[] page;
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char* page = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, page);
        unsigned offset, length;
        memcpy(&length, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
        // if a record was deleted, slot will have 0
        if (length == 0) {
            return -1;
        }
        memcpy(&offset, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));

        memcpy(data, page + offset, length);
        delete[] page;
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        std::vector<bool> isNull = extractNullInformation(data, recordDescriptor);
        unsigned numFields = recordDescriptor.size();
        int nullIndicatorSize = ceil(static_cast<double>(numFields) / 8.0);
        const char* charData = static_cast<const char*>(data); // for string bytes reading
        charData += nullIndicatorSize;

        unsigned linebreak = 1;

        std::string name;
        int num;
        float real;
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            name = recordDescriptor[i].name;
            if (!isNull[i]) {
                if (recordDescriptor[i].type == TypeVarChar) {
                    int varcharLength = 0;
                    memcpy(&varcharLength, charData, sizeof(int));
                    std::string str(charData + sizeof(int), varcharLength);
                    out << name + ": " << str;
                    charData += sizeof(int) + varcharLength;
                } else if (recordDescriptor[i].type == TypeInt) {
                        memcpy(&num, charData, sizeof(int));
                        out << name + ": " << num;
                        charData += sizeof(int);
                } else if (recordDescriptor[i].type == TypeReal) {
                    memcpy(&real, charData, sizeof(float));
                    out << name + ": " << real;
                    charData += sizeof(float);
                }
            }
            else {out << name << ": NULL";}
            if (linebreak % numFields != 0) {
                out << ", ";
                linebreak++;
            } else {
                out << '\n';
                linebreak = 1;
            }
        }
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        // read page and get record offset and length
        char* page = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, page);
        unsigned offset, length;
        memcpy(&length, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
        // deleting a non-existent record returns error
        if (length == 0) {
            return -1;
        }
        memcpy(&offset, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));

        // find directoryStart: the furthest left the directory goes
        unsigned numberOfSlots, freeSpace;
        memcpy(&numberOfSlots, page + PAGE_SIZE - 2 * sizeof(unsigned), sizeof(unsigned));
        memcpy(&freeSpace, page + PAGE_SIZE - 1 * sizeof(unsigned), sizeof(unsigned));

        // delete record
        unsigned directoryEnd = PAGE_SIZE - 2 * sizeof(unsigned) - numberOfSlots * 2 * sizeof(unsigned);
        unsigned shiftSize = directoryEnd - (offset + length);
        memmove(page + offset, page + offset + length, shiftSize);

        // update directory
        unsigned recordToDeleteOffset = offset;
        unsigned recordToDeleteLength = length;
        char* dirPtr = page + directoryEnd;
        for (int i = 0; i < numberOfSlots; i++) {
            unsigned slotOffset;
            memcpy(&slotOffset, dirPtr + i * (sizeof(unsigned) * 2), sizeof(unsigned));

            if (slotOffset > recordToDeleteOffset) {
                slotOffset -= recordToDeleteLength;
                memcpy(dirPtr + i * (sizeof(unsigned) * 2), &slotOffset, sizeof(unsigned));
            }
        }

        freeSpace += length; // numberOfSlots remain the same
        offset = 0;
        length = 0;

        memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), &length, sizeof(unsigned));
        memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), &offset, sizeof(unsigned));
        memcpy(page + PAGE_SIZE - 2 * sizeof(unsigned), &numberOfSlots, sizeof(unsigned));
        memcpy(page + PAGE_SIZE - 1 * sizeof(unsigned), &freeSpace, sizeof(unsigned));

        fileHandle.writePage(rid.pageNum, page);

        delete[] page;
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        char* page = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, page);
        unsigned offset, length;
        memcpy(&length, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
        // updating a non-existent record returns error
        if (length == 0) {
            return -1;
        }
        memcpy(&offset, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));

        // prepare new record to insert
        std::vector<bool> isNull = extractNullInformation(data, recordDescriptor);
        unsigned recordSize = getRecordSize(data, recordDescriptor, isNull);
        const char* record = (char*)createRecordStream(data, recordDescriptor, isNull, recordSize);

        // find old record
        unsigned numberOfSlots, freeSpace;
        memcpy(&numberOfSlots, page + PAGE_SIZE - 2 * sizeof(unsigned), sizeof(unsigned));
        memcpy(&freeSpace, page + PAGE_SIZE - 1 * sizeof(unsigned), sizeof(unsigned));

        // delete record
        unsigned directoryEnd = PAGE_SIZE - 2 * sizeof(unsigned) - numberOfSlots * 2 * sizeof(unsigned);

        // if recordSize <= length, replace record then compact space,
        if (recordSize <= length) {
            memcpy(page + offset, record, recordSize);
            memmove(page + offset + recordSize, page + offset + length, directoryEnd - (offset + recordSize));
            // adjust directory
            unsigned updatedRecordOffset = offset;
            unsigned lengthDiff = length - recordSize; // how much the new record and old record differ by length
            char* dirPtr = page + directoryEnd;
            for (int i = 0; i < numberOfSlots; i++) {
                unsigned slotOffset;
                memcpy(&slotOffset, dirPtr + i * (sizeof(unsigned) * 2), sizeof(unsigned));
                if (slotOffset > updatedRecordOffset) {
                    slotOffset -= lengthDiff;
                    memcpy(dirPtr + i * (sizeof(unsigned) * 2), &slotOffset, sizeof(unsigned));
                }
            }
        }
        else {
            deleteRecord(fileHandle, recordDescriptor, rid);
            memcpy(&freeSpace, page + PAGE_SIZE - 1 * sizeof(unsigned), sizeof(unsigned));
            if (freeSpace >= recordSize) {

            }
        }

        length = recordSize;








        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

    std::vector<bool> RecordBasedFileManager::extractNullInformation(const void *data, const std::vector<Attribute> &recordDescriptor) {
        unsigned numFields = recordDescriptor.size();
        unsigned nullIndicatorSize = ceil(static_cast<double>(numFields) / 8.0);
        char *nullsIndicator = new char[nullIndicatorSize];
        memcpy(nullsIndicator, (char*)data, nullIndicatorSize);

        std::vector<bool> isNull(numFields, false);
        for (int i = 0; i < numFields; ++i) {
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            unsigned char mask = 1 << (7 - bitIndex);
            if (nullsIndicator[byteIndex] & mask) {
                isNull[i] = true;
            }
        }
        delete[] nullsIndicator;
        return isNull;
    }

    void *RecordBasedFileManager::createRecordStream(const void *data, const std::vector<Attribute> &recordDescriptor, const std::vector<bool> &isNull, unsigned &recordSize) {
        unsigned nullIndicatorSize = ceil(static_cast<double>(recordDescriptor.size()) / 8.0); // Include size for null-indicator bytes
        // Allocate memory for the new record stream
        char *recordStream = new char[recordSize];
        char *currentPointer = recordStream;

        // Copy the null-indicator bytes
        memcpy(currentPointer, (char*)data, nullIndicatorSize);
        currentPointer += nullIndicatorSize;
        char *dataPointer = (char*)data + nullIndicatorSize;
        unsigned fieldSize = 0;
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (!isNull[i]) {
                if (recordDescriptor[i].type == TypeVarChar) {
                    int varcharLength = 0;
                    memcpy(&varcharLength, dataPointer, sizeof(int));
                    fieldSize = sizeof(int) + varcharLength;
                }
                else if (recordDescriptor[i].type == TypeInt) {
                    fieldSize = sizeof(int);
                }
                else if (recordDescriptor[i].type == TypeReal) {
                    fieldSize = sizeof(float);
                }
                memcpy(currentPointer, dataPointer, fieldSize);
                currentPointer += fieldSize;
                dataPointer += fieldSize;
            }
            // No need for an else branch as NULL fields have no representation in the data stream
        }
        return recordStream;
    }

    unsigned RecordBasedFileManager::getRecordSize(const void *data, const std::vector<Attribute> &recordDescriptor, std::vector<bool> isNull) {
        unsigned nullIndicatorSize = ceil(static_cast<double>(recordDescriptor.size()) / 8.0);
        unsigned recordSize = 0;
        recordSize += nullIndicatorSize;
        char* dataPtr = (char*)data + nullIndicatorSize; // skip past null fields

        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (!isNull[i]) {
                if (recordDescriptor[i].type == TypeVarChar) {
                    int varcharLength = 0;
                    memcpy(&varcharLength, dataPtr, sizeof(int));
                    recordSize += sizeof(int) + varcharLength;
                    dataPtr += sizeof(int) + varcharLength;
                } else if (recordDescriptor[i].type == TypeInt) {
                    recordSize += sizeof(int);
                    dataPtr += sizeof(int);
                } else if (recordDescriptor[i].type == TypeReal) {
                    recordSize += sizeof(float);
                    dataPtr += sizeof(float);
                }
            }
        }
        return recordSize;
    }


} // namespace PeterDB

