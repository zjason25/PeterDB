#include "src/include/rbfm.h"

#include <cstdio>
#include <cstring>
#include <cmath>
#include <iostream>


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
        const char* record = (char*)createRecordStream(data, recordDescriptor, isNull);

        unsigned numFields = recordDescriptor.size(); // Number of fields
        int recordSize = ceil(static_cast<double>(numFields) / 8.0); // include null byte size in record

        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (!isNull[i]) {
                if (recordDescriptor[i].type == TypeVarChar) {
                    recordSize += sizeof(int) + recordDescriptor[i].length;
                } else if (recordDescriptor[i].type == TypeInt) {
                    recordSize += sizeof(int);
                } else if (recordDescriptor[i].type == TypeReal) {
                    recordSize += sizeof(float);
                }
            }
        }
        unsigned numberOfSlots;
        unsigned freeSpace;
        int numPages = fileHandle.getNumberOfPages();
        int pageNum = numPages - 1;
        unsigned offset;
        unsigned length;
        bool read = false, inserted = false;
        char* page = new char[PAGE_SIZE * sizeof(char)];

        while (!inserted) {
            // create and append a new page
            if (numPages == 0) {
//                printf("Inserting record on a new page:\n");
                // pointer to the start of directory ( --> | ([offset_1][length_1]) | [N][F] )
                unsigned directory = PAGE_SIZE - 4 * sizeof(unsigned); // the left-most directory slot
                memcpy(page, record, recordSize);
                offset = 0; // pointer to the start of the record, initialized at 0;
                length = recordSize;
                numberOfSlots = 1;
                freeSpace = PAGE_SIZE - recordSize - 4 * sizeof(unsigned);
                memcpy(page + directory, &offset, sizeof(unsigned)); // store offset
                memcpy(page + directory + sizeof(unsigned), &length,sizeof(unsigned));
                memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned)), &numberOfSlots, sizeof(unsigned));
                memcpy(page + (PAGE_SIZE -  sizeof(unsigned)), &freeSpace, sizeof(unsigned));
                fileHandle.appendPage(page);
                inserted = true;
            }
            else {
//                printf("Trying to find space on existing page:\n");
                fileHandle.readPage(pageNum, page);
                memcpy(&freeSpace, &page[PAGE_SIZE - sizeof(unsigned)], sizeof(unsigned));
                // if there's space, go to the leftmost entry in the directory and find out where it ends in byte array
                if (freeSpace >= recordSize) {
//                    printf("Inserting record on an existing page:\n");
                    memcpy(&numberOfSlots, &page[PAGE_SIZE - 2 * sizeof(unsigned)], sizeof(unsigned));
                    unsigned leftMostEntry = PAGE_SIZE - 2 * sizeof(unsigned) - numberOfSlots * 2 * sizeof(unsigned);
                    memcpy(&offset, &page[leftMostEntry], sizeof(unsigned));
                    memcpy(&length, &page[leftMostEntry + sizeof(unsigned)], sizeof(unsigned));

                    memcpy(page + offset + length, record, recordSize);
                    offset = offset + length; // offset of the new array is (offset + length) of previous array
                    length = recordSize;
                    numberOfSlots += 1;
                    freeSpace -= (recordSize + 2 * sizeof(unsigned)); // record and slot
                    memcpy(&page[leftMostEntry - sizeof(unsigned)], &length, sizeof(unsigned));
                    memcpy(&page[leftMostEntry - 2 * sizeof(unsigned)], &offset, sizeof(unsigned));
                    memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned)), &numberOfSlots, sizeof(unsigned));
                    memcpy(page + (PAGE_SIZE - sizeof(unsigned)), &freeSpace, sizeof(unsigned));
                    fileHandle.writePage(pageNum, page);
                    inserted = true;
                } else {
                    //if no enough space in last page, go to first page
                    if (pageNum == (fileHandle.getNumberOfPages() - 1) && !read) {
                        pageNum = -1;
                        read = true;
                    }
                        //no space in all pages, append a new page
                    else if ((pageNum == fileHandle.getNumberOfPages()) && read) {
//                        printf("Appending a new page:\n");
                        unsigned directory = PAGE_SIZE - 4 * sizeof(unsigned); // the left-most directory slot
                        // store the record, which contains null indicator and actual data
                        memcpy(page, record, recordSize);
                        offset = 0; // pointer to the start of the record, initialized at 0;
                        length = recordSize; // length of the record
                        numberOfSlots = 1;
                        freeSpace = PAGE_SIZE - recordSize - 4 * sizeof(unsigned); // entry slot + NF + record_length
                        memcpy(page + directory, &offset, sizeof(unsigned)); // store offset
                        memcpy(page + directory + sizeof(unsigned), &length,sizeof(unsigned));
                        memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned)), &numberOfSlots, sizeof(unsigned));
                        memcpy(page + (PAGE_SIZE - sizeof(unsigned)), &freeSpace, sizeof(unsigned));
                        fileHandle.appendPage(page);
                        inserted = true;
                    }
                }
            }
            pageNum++;
        }
        rid.slotNum = numberOfSlots;
        rid.pageNum = pageNum;
        delete[] record;
        delete[] page;
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        char* page = new char[PAGE_SIZE * sizeof(char)];
        fileHandle.readPage(rid.pageNum, page);
        unsigned offset;
        unsigned length;
        memcpy(&offset, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));
        memcpy(&length, page+(PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
        memcpy(data, page + offset, length);
        delete[] page;
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        std::vector<bool> isNull = extractNullInformation(data, recordDescriptor);
        int nullIndicatorSize = ceil(static_cast<double>(recordDescriptor.size()) / 8.0);
        const char* charData = static_cast<const char*>(data); // for string bytes reading
        charData += nullIndicatorSize;
        unsigned numFields = recordDescriptor.size();
        unsigned linebreak = 1;

        std::string name;
        int num;
        float real;
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            name = recordDescriptor[i].name;
            if (!isNull[i]) {
                if (recordDescriptor[i].type == TypeVarChar) {
                    int varcharLength;
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
        memcpy(nullsIndicator, data, nullIndicatorSize);

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

    void *RecordBasedFileManager::createRecordStream(const void *data, const std::vector<Attribute> &recordDescriptor, const std::vector<bool> &isNull) {
        // Calculate the size of the new record stream
        unsigned numFields = recordDescriptor.size();
        unsigned recordSize = ceil(static_cast<double>(numFields) / 8.0); // Include size for null-indicator bytes
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (!isNull[i]) {
                if (recordDescriptor[i].type == TypeVarChar) {
                    recordSize += sizeof(int) + recordDescriptor[i].length;
                }
                else if (recordDescriptor[i].type == TypeInt) {
                    recordSize += sizeof(int);
                }
                else if (recordDescriptor[i].type == TypeReal) {
                    recordSize += sizeof(float);
                }
            }
        }
        // Allocate memory for the new record stream
        char *recordStream = new char[recordSize];
        char *currentPointer = recordStream;

        // Copy the null-indicator bytes
        const unsigned nullIndicatorSize = ceil(static_cast<double>(numFields) / 8.0);
        memcpy(currentPointer, data, nullIndicatorSize);
        currentPointer += nullIndicatorSize;

        // Set the data pointer after the null-indicator section
        const char *dataPointer = (const char *)data + nullIndicatorSize;
        unsigned fieldSize;
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (!isNull[i]) {
                if (recordDescriptor[i].type == TypeVarChar) {
                    fieldSize = sizeof(int) + recordDescriptor[i].length;
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
        return (void *)recordStream;
    }
} // namespace PeterDB

