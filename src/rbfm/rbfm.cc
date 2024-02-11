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
                memcpy(page + directory, &offset, sizeof(unsigned)); // store offset
                memcpy(page + directory + sizeof(unsigned), &length,sizeof(unsigned));
                memcpy(page + directory + 2 * sizeof(unsigned), &numberOfSlots, sizeof(unsigned));
                memcpy(page + directory + 3 * sizeof(unsigned), &freeSpace, sizeof(unsigned));
                fileHandle.appendPage(page);
                inserted = true;
                pageNum++;
            }
            else {
                fileHandle.readPage(pageNum, page);
                memcpy(&freeSpace, page + PAGE_SIZE - sizeof(unsigned), sizeof(unsigned));
                // if there's space, go to the leftmost entry in the directory and find out where it ends in byte array
                if (freeSpace >= (recordSize + 2 * sizeof(unsigned))) {
                    memcpy(&numberOfSlots, page + PAGE_SIZE - 2 * sizeof(unsigned), sizeof(unsigned));
                    unsigned leftMostEntry = PAGE_SIZE - 2 * sizeof(unsigned) - numberOfSlots * 2 * sizeof(unsigned);
                    memcpy(&offset, page + leftMostEntry, sizeof(unsigned));
                    memcpy(&length, page + leftMostEntry + sizeof(unsigned), sizeof(unsigned));
                    memcpy(page + offset + length , record, recordSize);
                    offset = offset + length; // offset of the new array is (offset + length) of previous array
                    length = recordSize;
                    numberOfSlots += 1;
                    freeSpace -= (recordSize + 2 * sizeof(unsigned)); // record and slot
                    memcpy(page + leftMostEntry - sizeof(unsigned), &length, sizeof(unsigned));
                    memcpy(page + leftMostEntry - 2 * sizeof(unsigned), &offset, sizeof(unsigned));
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
                    }
                    else {
                        pageNum++;
                    }
                }
            }
        }
//        std::ostringstream stream1;
//        std::ostringstream stream2;
//        printRecord(recordDescriptor, data, stream1);
//        std::cout << "got:\n " << stream1.str() << std::endl;
//        printRecord(recordDescriptor, record, stream2);
//        std::cout << "inserted:\n " << stream2.str() << std::endl;

        rid.slotNum = numberOfSlots;
        rid.pageNum = pageNum;
//        printf("inserted in page %d\n", pageNum);
        delete[] record;
        delete[] page;
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char* page = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, page);
        unsigned offset, length;
        memcpy(&offset, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));
        memcpy(&length, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
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
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
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
} // namespace PeterDB

