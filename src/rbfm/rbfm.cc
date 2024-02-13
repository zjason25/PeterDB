#include "src/include/rbfm.h"

#include <cstdio>
#include <cstring>
#include <cmath>
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iterator>

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
        memcpy(&offset, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));
        memcpy(&length, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));

        // read from a tombstone
        if (length >= TOMBSTONE_MARKER) {
            unsigned pageNum = offset - TOMBSTONE_MARKER;
            unsigned slotNum = length - TOMBSTONE_MARKER;
            fileHandle.readPage(pageNum, page);
            memcpy(&offset, page + (PAGE_SIZE - 2 * sizeof(unsigned) - slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));
            memcpy(&length, page + (PAGE_SIZE - 2 * sizeof(unsigned) - slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
            memcpy(data, page + offset, length);
            delete[] page;
            return 0;
        }
        // if a record was deleted, slot will have 0
        if (length == 0) {
            return -1;
        }

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
        memcpy(&offset, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));
        memcpy(&length, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
        // deleting a non-existent record returns error
        if (length >= TOMBSTONE_MARKER) {
            RID rid_t;
            rid_t.pageNum = offset - TOMBSTONE_MARKER;
            rid_t.slotNum = length - TOMBSTONE_MARKER;
            deleteRecord(fileHandle, recordDescriptor, rid_t);
            offset = 0;
            length = 0;
            memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), &offset, sizeof(unsigned));
            memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), &length, sizeof(unsigned));
            fileHandle.writePage(rid.pageNum, page);
            return 0;
        }
        if (length == 0) {
            return -1;
        }

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

        memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), &offset, sizeof(unsigned));
        memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), &length, sizeof(unsigned));
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
        memcpy(&length, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
        // updating a non-existent record returns error
        if (length == 0) {
            return -1;
        }
        memcpy(&offset, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));

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
            // offset stays the same
        }
        // if old record is larger than new record, delete it
        else {
            deleteRecord(fileHandle, recordDescriptor, rid);
            memcpy(&freeSpace, page + PAGE_SIZE - 1 * sizeof(unsigned), sizeof(unsigned));
            // insert at the end of current page
            if (freeSpace >= recordSize) {
                unsigned endOfRecords = directoryEnd - freeSpace;
                memcpy(page + endOfRecords, record, recordSize);
                freeSpace -= recordSize;
                offset = endOfRecords;
            }
            else {
                // insert in new page and leave tombstone: [pageNum_t][slotNum_t]
                RID rid_t;
                insertRecord(fileHandle, recordDescriptor, data, rid_t);
                unsigned pageNum_t = rid_t.pageNum + TOMBSTONE_MARKER; // length stores pageNum
                unsigned slotNum_t = rid_t.slotNum + TOMBSTONE_MARKER; // offset stores slotNum
                memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), &pageNum_t, sizeof(unsigned));
                memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), &slotNum_t, sizeof(unsigned));
                fileHandle.writePage(rid.pageNum, page);
                return 0;
            }
        }

        length = recordSize;
        memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), &offset, sizeof(unsigned));
        memcpy(page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), &length, sizeof(unsigned));

        fileHandle.writePage(rid.pageNum, page);

        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        char* page = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, page);
        unsigned offset, length;
        memcpy(&offset, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)), sizeof(unsigned));
        memcpy(&length, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)), sizeof(unsigned));
        // reading a tombstone attribute
        if (length >= TOMBSTONE_MARKER) {
            RID rid_t;
            rid_t.pageNum = offset - TOMBSTONE_MARKER;
            rid_t.slotNum = length - TOMBSTONE_MARKER;
            readAttribute(fileHandle, recordDescriptor, rid_t, attributeName, data);
            return 0;
        }
        if (length == 0) {
            return -1;
        }

        char* recordPtr = page + offset;
        std::vector<bool> isNull = extractNullInformation(recordPtr, recordDescriptor);
        unsigned nullIndicatorSize = ceil(static_cast<double>(recordDescriptor.size()) / 8.0);

        // Copy the null-indicator bytes
        char *dataPtr = recordPtr + nullIndicatorSize;
        unsigned fieldSize = 0;
        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (!isNull[i]) {
                if (recordDescriptor[i].name == attributeName) {
                    if (recordDescriptor[i].type == TypeVarChar) {
                        int varcharLength = 0;
                        memcpy(&varcharLength, dataPtr, sizeof(int));
                        fieldSize = sizeof(int) + varcharLength;
                    } else if (recordDescriptor[i].type == TypeInt) {
                        fieldSize = sizeof(int);
                    } else if (recordDescriptor[i].type == TypeReal) {
                        fieldSize = sizeof(float);
                    }
                    memcpy(data, dataPtr, fieldSize);
                    return 0;
                }
                else {
                    if (recordDescriptor[i].type == TypeVarChar) {
                        int varcharLength = 0;
                        memcpy(&varcharLength, dataPtr, sizeof(int));
                        fieldSize = sizeof(int) + varcharLength;
                    } else if (recordDescriptor[i].type == TypeInt) {
                        fieldSize = sizeof(int);
                    } else if (recordDescriptor[i].type == TypeReal) {
                        fieldSize = sizeof(float);
                    }
                }
                dataPtr += fieldSize;
            }
        }
        return -1;
    }

    RC RBFM_ScanIterator::initializeScan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                   const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                   const std::vector<std::string> &attributeNames) {

        this->rbfm = &RecordBasedFileManager::instance();
        this->fileHandle = fileHandle;
        this->recordDescriptor = recordDescriptor;
        this->conditionAttribute = conditionAttribute;
        this->compOp = compOp;
        this->value = value;
        this->page = new char[PAGE_SIZE];
        this->pageNum = 0;
        this->slotNum = 0; // slotNum start from 1
        this->attributeNames = attributeNames;
        this->numberOfPages = 0;
        this->numberOfSlots = 0;

        // read the first page
        this->numberOfPages = fileHandle.getNumberOfPages();
        if (numberOfPages > 0) {
            if (fileHandle.readPage(0, page)) {
                return -1;
            }
        }
        // Get number of slots on first page
        this->numberOfSlots = rbfm->getTotalSlots(page);

        return 0;
    }
    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator){
        return rbfm_ScanIterator.initializeScan(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames);
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        if (getNextSlot() == -1) {return RBFM_EOF;}

        // Not returning any result
        if (attributeNames.empty()) {
            rid.pageNum = pageNum;
            rid.slotNum = slotNum;
            return 0;
        }

        char* record = new char[PAGE_SIZE];
        rbfm->readNextRecord(fileHandle, rid, page, record);


        // Two passes over record
        // 1. Check for conditionAttribute
        if (!conditionAttribute.empty()) {
            bool condition = checkCondition(record, recordDescriptor);
            // no matching record
            if (!condition) {
                rid.pageNum = pageNum;
                rid.slotNum = slotNum;
                return 0;
            }
        }
        // 2. Extract Attribute in attributeNames
        extractAttributesAndNullBits(recordDescriptor, attributeNames, record, data);
        return 0;
    };
    RC RBFM_ScanIterator::close() {
        free(page);
        return 0;
    }

    RC RBFM_ScanIterator::getNextSlot() {
        if (slotNum > numberOfSlots) {
            if (pageNum == numberOfPages) {
                return RBFM_EOF;
            }
            pageNum++;
            slotNum = 1;
            fileHandle.readPage(pageNum, page);
            numberOfSlots = rbfm->getTotalSlots(page);
            return 0;
        }
        slotNum++;
        return 0;
    };

    bool RBFM_ScanIterator::compareInt(int &num, const void *newValue, CompOp compareOp) {
        if (compOp == NO_OP) {
            return true;
        }

        int val;
        memcpy(&val, newValue, sizeof(int));
        switch (compareOp) {
            case EQ_OP: return num == val;
            case LT_OP: return num < val;
            case LE_OP: return num <= val;
            case GT_OP: return num > val;
            case GE_OP: return num >= val;
            case NE_OP: return num != val;
            case NO_OP: return true;
        }
        return false;
    }
    bool RBFM_ScanIterator::compareReal(float &real, const void *newValue, CompOp compareOp) {
        if (compOp == NO_OP) {
            return true;
        }

        float val;
        memcpy(&val, newValue, sizeof(float));
        switch (compareOp) {
            case EQ_OP: return real == val;
            case LT_OP: return real < val;
            case LE_OP: return real <= val;
            case GT_OP: return real > val;
            case GE_OP: return real >= val;
            case NE_OP: return real != val;
            case NO_OP: return true;
        }
        return false;
    }
    bool RBFM_ScanIterator::compareVarchar(char* str, const void *newValue, CompOp compareOp) {
        if (compOp == NO_OP) {
            return true;
        }

        int length;
        memcpy(&length, (char*)newValue, sizeof(int));
        char* valStr = new char[length + 1];
        memcpy(valStr, newValue, length);
        valStr[length + 1] = '\0';

        int result = strcmp(str, valStr);

        switch (compareOp) {
            case EQ_OP: return result == 0;
            case LT_OP: return result < 0;
            case LE_OP: return result <= 0;
            case GT_OP: return result > 0;
            case GE_OP: return result >= 0;
            case NE_OP: return result != 0;
            case NO_OP: return true;
        }
        return false;
    }

    bool RBFM_ScanIterator::checkCondition(void* data, std::vector<Attribute> &recordDescriptor) {
        std::vector<bool> isNull = rbfm->extractNullInformation((char*)data, recordDescriptor);
        unsigned nullIndicatorSize = ceil(static_cast<double>(recordDescriptor.size()) / 8.0);
        char* dataPtr = (char*) data + nullIndicatorSize; // Skip Null for now
        unsigned fieldSize = 0;
        bool condition = false;

        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (!isNull[i]) {
                if (recordDescriptor[i].name == conditionAttribute) {
                    if (recordDescriptor[i].type == TypeInt) {
                        int num;
                        memcpy(&num, dataPtr, sizeof(int));
                        return compareInt(num, value, compOp);
                    }
                    else if (recordDescriptor[i].type == TypeReal) {
                        float real;
                        memcpy(&real, dataPtr, sizeof(float));
                        return compareReal(real, value, compOp);
                    }
                    else if (recordDescriptor[i].type == TypeVarChar) {
                        int length;
                        memcpy(&length, (char*)dataPtr, sizeof(int));
                        dataPtr += sizeof(int);
                        char* str = new char[length + 1];
                        str[length + 1] = '\0';
                        return compareVarchar(str, value, compOp);
                    }
                }
                else {
                    if (recordDescriptor[i].type == TypeVarChar) {
                        int varcharLength = 0;
                        memcpy(&varcharLength, dataPtr, sizeof(int));
                        fieldSize = sizeof(int) + varcharLength;
                    } else {
                        fieldSize = sizeof(int);
                    }
                }
                dataPtr += fieldSize;
            }
        }
        return condition;
    }
    void RBFM_ScanIterator::extractAttributesAndNullBits(const std::vector<Attribute> &recordDescriptor,
            const std::vector<std::string> &attributeNames, const char *record, void *data) {
        std::vector<bool> nullBits;
        std::vector<int> attributeIndexes;
        unsigned newNullIndicatorSize = ceil(static_cast<double>(attributeNames.size()) / 8.0);
        std::vector<unsigned char> newNullIndicator(newNullIndicatorSize, 0);

        // Temporary buffer to store extracted attributes before knowing the exact output size
        std::vector<char> tempBuffer;

        // Calculate the size of the original null indicator
        size_t nullIndicatorSize = ceil(static_cast<double>(recordDescriptor.size()) / 8.0);
        const char* currentPtr = record + nullIndicatorSize; // Start reading attributes after the null indicator

        for (int i = 0; i < recordDescriptor.size(); ++i) {
            bool isTargetAttribute = std::find(attributeNames.begin(), attributeNames.end(), recordDescriptor[i].name) != attributeNames.end();
            int byteIndex = i / 8;
            int bitIndex = i % 8;
            bool isNull = record[byteIndex] & (1 << (7 - bitIndex));

            if (isTargetAttribute) {
                size_t indexInTarget = std::distance(attributeNames.begin(), std::find(attributeNames.begin(), attributeNames.end(), recordDescriptor[i].name));
                if (!isNull) {
                    // Extract attribute value
                    switch (recordDescriptor[i].type) {
                        case TypeInt:
                        case TypeReal: {
                            tempBuffer.insert(tempBuffer.end(), currentPtr, currentPtr + 4);
                            currentPtr += 4;
                            break;
                        }
                        case TypeVarChar: {
                            int length;
                            std::memcpy(&length, currentPtr, sizeof(int));
                            tempBuffer.insert(tempBuffer.end(), currentPtr, currentPtr + 4 + length);
                            currentPtr += 4 + length;
                            break;
                        }
                    }
                }
                // Set null bit in new null indicator
                if (isNull) {
                    newNullIndicator[indexInTarget / 8] |= (1 << (7 - (indexInTarget % 8)));
                }
            } else {
                // Skip this attribute
                if (!isNull) {
                    switch (recordDescriptor[i].type) {
                        case TypeInt:
                        case TypeReal:
                            currentPtr += 4;
                            break;
                        case TypeVarChar:
                            int length;
                            std::memcpy(&length, currentPtr, sizeof(int));
                            currentPtr += 4 + length;
                            break;
                    }
                }
            }
        }
        // Copy the temporary buffer and new null indicator to the output
        std::memcpy(data, newNullIndicator.data(), newNullIndicatorSize);
        std::memcpy((char*)data + newNullIndicatorSize, tempBuffer.data(), tempBuffer.size());
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

    unsigned RecordBasedFileManager::getTotalSlots(void *page) {
        unsigned numSlots;
        memcpy(&numSlots, (char*)page + PAGE_SIZE - 2 * sizeof(unsigned), sizeof(unsigned));
        return numSlots;
    }

    // given an rid, read the next record from *data into *record
    RC RecordBasedFileManager::readNextRecord(FileHandle fileHandle, RID rid, void *data, void *record) {
        char* page = (char*) data;
        unsigned offset, length;
        memcpy(&offset, page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned)),
               sizeof(unsigned));
        memcpy(&length,
               page + (PAGE_SIZE - 2 * sizeof(unsigned) - rid.slotNum * 2 * sizeof(unsigned) + sizeof(unsigned)),
               sizeof(unsigned));
        // read from a tombstone
        if (length >= TOMBSTONE_MARKER) {
            unsigned pageNum_t = offset - TOMBSTONE_MARKER;
            unsigned slotNum_t = length - TOMBSTONE_MARKER;
            RID rid_t;
            rid_t.pageNum = pageNum_t;
            rid_t.slotNum = slotNum_t;
            char *temp_page = new char[PAGE_SIZE];
            fileHandle.readPage(pageNum_t, temp_page);
            readNextRecord(fileHandle, rid_t, temp_page, record);
            delete[] temp_page;
            return 0;
        }
        // reading an empty slot
        if (length == 0) {
            return -1;
        }
        memcpy(record, page + offset, length);
        return 0;
    }
} // namespace PeterDB

