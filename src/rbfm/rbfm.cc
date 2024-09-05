#include "src/include/rbfm.h"

#include <cstdio>
#include <cstring>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <memory>

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbfm = RecordBasedFileManager();
        _rbfm.reset();
        return _rbfm;
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

    void RecordBasedFileManager::updateHeap(FileHandle &fileHandle, const int &pageNum, const unsigned short &newFreeSpace) {
        fileHeapMap[&fileHandle].pop();
        addPageToHeap(fileHandle, pageNum, newFreeSpace);
    }

    void RecordBasedFileManager::addPageToHeap(FileHandle &fileHandle, const int &pageNum, const unsigned short &freeSpace) {
        fileHeapMap[&fileHandle].push(PageInfo{pageNum, freeSpace});
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        // Calculate record size
        std::vector<bool> isNull = extractNullInformation(data, recordDescriptor);
        const auto recordSize = static_cast<unsigned short>(getRecordSize(data, recordDescriptor, isNull));
        const unsigned short requiredSpace = recordSize + SLOT_SIZE;

        unsigned short numberOfSlots, freeSpace, offset = 0;
        unsigned short pageFreeSpace = 0;
        const unsigned numPages = fileHandle.getNumberOfPages();
        int pageNum = -1;
        const std::unique_ptr<char[]> page(new char[PAGE_SIZE]);

        // Locate largest available space on heap
        if (!fileHeapMap[&fileHandle].empty() && fileHeapMap[&fileHandle].top().freeSpace >= requiredSpace) {
            const PageInfo pi = fileHeapMap[&fileHandle].top();
            pageNum = pi.pageNum;
            pageFreeSpace = pi.freeSpace;
        }

        // Create and insert in new page
        if (numPages == 0 || pageNum == -1) {
            const unsigned directory = PAGE_SIZE - 2 * SLOT_SIZE;
            numberOfSlots = 1;
            freeSpace = PAGE_SIZE - recordSize - 2 * SLOT_SIZE;
            memcpy(page.get(), data, recordSize);
            memcpy(page.get() + directory, &offset, SHORT_SIZE);
            memcpy(page.get() + directory + SHORT_SIZE, &recordSize,SHORT_SIZE);
            memcpy(page.get() + directory + 2 * SHORT_SIZE, &numberOfSlots, SHORT_SIZE);
            memcpy(page.get() + directory + 3 * SHORT_SIZE, &freeSpace, SHORT_SIZE);
            fileHandle.appendPage(page.get());
            rid.slotNum = 1;
            pageNum = (numPages == 0) ? 0 : static_cast<int>(numPages);
            addPageToHeap(fileHandle, pageNum, freeSpace);
        }
        // Insert in an existing page
        else {
            fileHandle.readPage(pageNum, page.get());

            memcpy(&numberOfSlots, page.get() + PAGE_SIZE - SLOT_SIZE, SHORT_SIZE);
            const unsigned directoryEnd = PAGE_SIZE - SLOT_SIZE - numberOfSlots * SLOT_SIZE;

            // Insert record right after the last byte array
            const unsigned endOfRecords = directoryEnd - pageFreeSpace;
            memcpy(page.get() + endOfRecords, data, recordSize);

            // Find an available slot
            unsigned slotToInsert = 0;
            const auto dir = page.get() + directoryEnd;
            for (int i = 0; i < numberOfSlots; i++) {
                unsigned short slotLength;
                memcpy(&slotLength, dir + i * SLOT_SIZE + SHORT_SIZE, SHORT_SIZE);
                if (slotLength == 0) {
                    slotToInsert = numberOfSlots - i;
                    break;
                }
            }
            // Update slot entry
            offset = endOfRecords;
            pageFreeSpace -= recordSize;

            // If not re-using a slot, allocate space for a new slot
            if (slotToInsert == 0) {
                memcpy(page.get() + directoryEnd - SLOT_SIZE, &offset, SHORT_SIZE);
                memcpy(page.get() + directoryEnd - SHORT_SIZE, &recordSize, SHORT_SIZE);
                numberOfSlots += 1;
                rid.slotNum = numberOfSlots;
                pageFreeSpace -= SLOT_SIZE; // subtract space allocated for new slot
            } else {
                memcpy(page.get() + PAGE_SIZE - SLOT_SIZE - slotToInsert * SLOT_SIZE, &offset, SHORT_SIZE);
                memcpy(page.get() + PAGE_SIZE - SLOT_SIZE - slotToInsert * SLOT_SIZE + SHORT_SIZE, &recordSize, SHORT_SIZE);
                rid.slotNum = slotToInsert;
            }

            // Update directory
            memcpy(page.get() + PAGE_SIZE - SLOT_SIZE, &numberOfSlots, SHORT_SIZE);
            memcpy(page.get() + PAGE_SIZE - SHORT_SIZE, &pageFreeSpace, SHORT_SIZE);
            fileHandle.writePage(pageNum, page.get());
            updateHeap(fileHandle, pageNum, pageFreeSpace);
        }

        rid.pageNum = pageNum;
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        // Read page
        const std::unique_ptr<char[]> page(new char[PAGE_SIZE]);
        fileHandle.readPage(rid.pageNum, page.get());

        // Locate record
        unsigned short offset, length;
        memcpy(&offset, page.get() + (PAGE_SIZE - SLOT_SIZE - rid.slotNum * SLOT_SIZE), SHORT_SIZE);
        memcpy(&length, page.get() + (PAGE_SIZE - SLOT_SIZE - rid.slotNum * SLOT_SIZE + SHORT_SIZE), SHORT_SIZE);

        // Read from a tombstone
        if (length >= TOMBSTONE_MARKER) {
            const unsigned pageNum = offset - TOMBSTONE_MARKER;
            const unsigned slotNum = length - TOMBSTONE_MARKER;
            fileHandle.readPage(pageNum, page.get());
            memcpy(&offset, page.get() + (PAGE_SIZE - 2 * SHORT_SIZE - slotNum * SLOT_SIZE), SHORT_SIZE);
            memcpy(&length, page.get() + (PAGE_SIZE - 2 * SHORT_SIZE - slotNum * SLOT_SIZE + SHORT_SIZE), SHORT_SIZE);
        }

        // If a record was deleted, slot will have 0
        if (length == 0) {
            return -1;
        }

        // Read record into data
        memcpy(data, page.get() + offset, length);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        std::vector<bool> isNull = extractNullInformation(data, recordDescriptor);
        const unsigned fieldSize = recordDescriptor.size();
        const unsigned nullIndicatorSize = (recordDescriptor.size() + 7) / 8;
        auto charData = static_cast<const char*>(data); // cast to char for reading
        charData += nullIndicatorSize; // Move dataPtr past null indicator

        unsigned linebreak = 1;

        int num;
        float real;

        for (int i = 0; i < recordDescriptor.size(); ++i) {
            std::string name = recordDescriptor[i].name;

            if (!isNull[i]) {
                switch (recordDescriptor[i].type) {
                    case TypeVarChar: {
                        int varcharLength = 0;
                        memcpy(&varcharLength, charData, NUM_SIZE); // Copy varchar length
                        std::string str(charData + NUM_SIZE, varcharLength);
                        out << name + ": " << str;
                        charData += varcharLength; // Move charData pointer past varChar
                        break;
                    }
                    case TypeInt: {
                        memcpy(&num, charData, NUM_SIZE); // Copy int value
                        out << name + ": " << num;
                        break;
                    }
                    case TypeReal: {
                        memcpy(&real, charData, NUM_SIZE); // Copy float value
                        out << name + ": " << real;
                        break;
                    }
                }
                charData += NUM_SIZE; // Move charData pointer
            } else {
                out << name << ": NULL"; // Handle null fields
            }

            // Format output with commas and new lines
            if (linebreak % fieldSize != 0) {
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
        unsigned short offset, length;
        memcpy(&offset, page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE), SHORT_SIZE);
        memcpy(&length, page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE), SHORT_SIZE);
        // deleting a non-existent record returns error
        if (length >= TOMBSTONE_MARKER) {
            RID rid_t;
            rid_t.pageNum = offset - TOMBSTONE_MARKER;
            rid_t.slotNum = length - TOMBSTONE_MARKER;
            deleteRecord(fileHandle, recordDescriptor, rid_t);
            offset = 0;
            length = 0;
            memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE), &offset, SHORT_SIZE);
            memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE), &length, SHORT_SIZE);
            fileHandle.writePage(rid.pageNum, page);
            return 0;
        }
        if (length == 0) {
            return -1;
        }

        // find directoryStart: the furthest left the directory goes
        unsigned short numberOfSlots, freeSpace;
        memcpy(&numberOfSlots, page + PAGE_SIZE - 2 * SHORT_SIZE, SHORT_SIZE);
        memcpy(&freeSpace, page + PAGE_SIZE - 1 * SHORT_SIZE, SHORT_SIZE);

        // delete record
        unsigned directoryEnd = PAGE_SIZE - 2 * SHORT_SIZE - numberOfSlots * 2 * SHORT_SIZE;
        unsigned shiftSize = directoryEnd - (offset + length);
        memmove(page + offset, page + offset + length, shiftSize);

        // update directory
        unsigned short recordToDeleteOffset = offset;
        unsigned short recordToDeleteLength = length;
        char* dirPtr = page + directoryEnd;
        for (int i = 0; i < numberOfSlots; i++) {
            unsigned slotOffset;
            memcpy(&slotOffset, dirPtr + i * (SHORT_SIZE * 2), SHORT_SIZE);

            if (slotOffset > recordToDeleteOffset) {
                slotOffset -= recordToDeleteLength;
                memcpy(dirPtr + i * (SHORT_SIZE * 2), &slotOffset, SHORT_SIZE);
            }
        }

        freeSpace += length; // numberOfSlots remain the same
        offset = 0;
        length = 0;

        memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE), &offset, SHORT_SIZE);
        memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE), &length, SHORT_SIZE);
        memcpy(page + PAGE_SIZE - 2 * SHORT_SIZE, &numberOfSlots, SHORT_SIZE);
        memcpy(page + PAGE_SIZE - 1 * SHORT_SIZE, &freeSpace, SHORT_SIZE);

        fileHandle.writePage(rid.pageNum, page);

        delete[] page;
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        char* page = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, page);
        unsigned short offset, length;
        memcpy(&length, page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE), SHORT_SIZE);
        // updating a non-existent record returns error
        if (length == 0) {
            return -1;
        }
        memcpy(&offset, page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE), SHORT_SIZE);

        // prepare new record to insert
        std::vector<bool> isNull = extractNullInformation(data, recordDescriptor);
        unsigned recordSize = getRecordSize(data, recordDescriptor, isNull);

        // find old record
        unsigned short numberOfSlots, freeSpace;
        memcpy(&numberOfSlots, page + PAGE_SIZE - 2 * SHORT_SIZE, SHORT_SIZE);
        memcpy(&freeSpace, page + PAGE_SIZE - 1 * SHORT_SIZE, SHORT_SIZE);

        // delete record
        unsigned directoryEnd = PAGE_SIZE - 2 * SHORT_SIZE - numberOfSlots * 2 * SHORT_SIZE;

        // if recordSize <= length, replace record then compact space,
        if (recordSize <= length) {
            memcpy(page + offset, data, recordSize);
            memmove(page + offset + recordSize, page + offset + length, directoryEnd - (offset + recordSize));
            // adjust directory
            unsigned updatedRecordOffset = offset;
            unsigned lengthDiff = length - recordSize; // how much the new record and old record differ by length
            char* dirPtr = page + directoryEnd;
            for (int i = 0; i < numberOfSlots; i++) {
                unsigned slotOffset;
                memcpy(&slotOffset, dirPtr + i * (SHORT_SIZE * 2), SHORT_SIZE);
                if (slotOffset > updatedRecordOffset) {
                    slotOffset -= lengthDiff;
                    memcpy(dirPtr + i * (SHORT_SIZE * 2), &slotOffset, SHORT_SIZE);
                }
            }
            // offset stays the same
        }
        // if old record is larger than new record, delete it
        else {
            deleteRecord(fileHandle, recordDescriptor, rid);
            memcpy(&freeSpace, page + PAGE_SIZE - 1 * SHORT_SIZE, SHORT_SIZE);
            // insert at the end of current page
            if (freeSpace >= recordSize) {
                unsigned endOfRecords = directoryEnd - freeSpace;
                memcpy(page + endOfRecords, data, recordSize);
                freeSpace -= recordSize;
                offset = endOfRecords;
            }
            else {
                // insert in new page and leave tombstone: [pageNum_t][slotNum_t]
                RID rid_t;
                insertRecord(fileHandle, recordDescriptor, data, rid_t);
                unsigned pageNum_t = rid_t.pageNum + TOMBSTONE_MARKER; // length stores pageNum
                unsigned slotNum_t = rid_t.slotNum + TOMBSTONE_MARKER; // offset stores slotNum
                memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE), &pageNum_t, SHORT_SIZE);
                memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE), &slotNum_t, SHORT_SIZE);
                fileHandle.writePage(rid.pageNum, page);
                return 0;
            }
        }

        length = recordSize;
        memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE), &offset, SHORT_SIZE);
        memcpy(page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE), &length, SHORT_SIZE);

        fileHandle.writePage(rid.pageNum, page);

        return 0;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        char* page = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, page);
        unsigned short offset, length;
        memcpy(&offset, page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE), SHORT_SIZE);
        memcpy(&length, page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE), SHORT_SIZE);
        // reading a tombstone attribute
        if (length >= TOMBSTONE_MARKER) {
            RID rid_t;
            rid_t.pageNum = offset - TOMBSTONE_MARKER;
            rid_t.slotNum = length - TOMBSTONE_MARKER;
            readAttribute(fileHandle, recordDescriptor, rid_t, attributeName, data);
            return 0;
        }
        // reading a deleted slot
        if (length == 0) {
            return -1;
        }

        char* recordPtr = page + offset;
        std::vector<bool> isNull = extractNullInformation(recordPtr, recordDescriptor);
        unsigned nullIndicatorSize = (recordDescriptor.size() + 7) / 8;

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
                    char null = 0;
                    memcpy(data, &null, 1);
                    memcpy((char*)data+1, dataPtr, fieldSize);
                    delete[] page;
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
        char null = 1;
        memcpy(data, &null, 1);
        delete[] page;
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
        if (getNextSlot() == RBFM_EOF) {
            return RBFM_EOF;
        }

        rid.pageNum = pageNum;
        rid.slotNum = slotNum;
        // Not returning any result
        if (attributeNames.empty()) {
            return -2;
        }

        char* record = new char[PAGE_SIZE];
        rbfm->readNextRecord(fileHandle, rid, page, record);

        // Two passes over record
        // 1. Check for conditionAttribute
        if (!conditionAttribute.empty()) {
            // no matching record
            if (!checkCondition(record, recordDescriptor)) {
                delete[] record;
//                return getNextRecord(rid, data);
                return -2;
            }
        }
        // 2. Extract Attribute in attributeNames
        extractAttributesAndNullBits(recordDescriptor, attributeNames, record, data);
        delete[] record;
        return 0;
    }

    RC RBFM_ScanIterator::close() {
        free(page);
        return 0;
    }

    RC RBFM_ScanIterator::getNextSlot() {
        slotNum++;
        if (slotNum > numberOfSlots) {
            pageNum++;
            if (pageNum >= numberOfPages) {
                return RBFM_EOF;
            }
            fileHandle.readPage(pageNum, page);
            slotNum = 1;
            numberOfSlots = rbfm->getTotalSlots(page);
        }
        return 0;
    }

    bool RBFM_ScanIterator::checkCondition(void* data, std::vector<Attribute> &recordDescriptor) {
        std::vector<bool> isNull = rbfm->extractNullInformation((char*)data, recordDescriptor);
        unsigned nullIndicatorSize = (recordDescriptor.size() + 7) / 8;
        char* dataPtr = (char*) data + nullIndicatorSize; // Skip Null for now

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
                        memcpy(&length, dataPtr, sizeof(int));
                        dataPtr += sizeof(int);
                        char* str = new char[length + 1];
                        memcpy(str, dataPtr, length);
                        str[length] = '\0';
                        bool result = compareVarchar(str, value, compOp);
                        delete[] str; // Avoid memory leak
                        return result;
                    }
                }
                else {
                    if (recordDescriptor[i].type == TypeVarChar) {
                        int varcharLength = 0;
                        memcpy(&varcharLength, dataPtr, sizeof(int));
                        dataPtr += sizeof(int) + varcharLength;
                    } else {
                        dataPtr += sizeof(int);
                    }
                }
            }
        }
        return false;
    }

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
        memcpy(valStr, (char*)newValue + sizeof(int), length);
        valStr[length] = '\0';

        int result = strcmp(str, valStr);
        delete[] valStr;

        switch (compareOp) {
            case EQ_OP: return result == 0;
            case LT_OP: return result < 0;
            case LE_OP: return result <= 0;
            case GT_OP: return result > 0;
            case GE_OP: return result >= 0;
            case NE_OP: return result != 0;
            case NO_OP:
                return true;
        }
        return false; // Default case
    }

    void RBFM_ScanIterator::extractAttributesAndNullBits(const std::vector<Attribute> &recordDescriptor,
            const std::vector<std::string> &attributeNames, const char *record, void *data) {
        std::vector<bool> nullBits;
        std::vector<int> attributeIndexes;
        unsigned newNullIndicatorSize = (recordDescriptor.size() + 7) / 8;
        std::vector<unsigned char> newNullIndicator(newNullIndicatorSize, 0);

        // Temporary buffer to store extracted attributes before knowing the exact output size
        std::vector<char> tempBuffer;

        // Calculate the size of the original null indicator
        size_t nullIndicatorSize = (recordDescriptor.size() + 7) / 8;
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
        const unsigned fieldSize = recordDescriptor.size();
        const auto nullsIndicator = static_cast<const char*>(data);

        std::vector<bool> isNull(fieldSize, false);
        for (int i = 0; i < fieldSize; ++i) {
            const int byteIndex = i / 8;
            const int bitIndex = i % 8;
            const unsigned char mask = 1 << (7 - bitIndex);
            if (nullsIndicator[byteIndex] & mask) {
                isNull[i] = true;
            }
        }
        return isNull;
    }

    unsigned RecordBasedFileManager::getRecordSize(const void *data, const std::vector<Attribute> &recordDescriptor, std::vector<bool> &isNull) {
        const unsigned fieldSize = recordDescriptor.size();
        const unsigned nullIndicatorSize = (fieldSize + 7) / 8;
        unsigned recordSize = nullIndicatorSize; // Start with the size of the null indicator (in number of bytes)

        const char* dataPtr = static_cast<const char*>(data) + nullIndicatorSize;

        for (unsigned i = 0; i < fieldSize; i++) {
            if (!isNull[i]) {
                switch (recordDescriptor[i].type) {
                    // For VarChar, read the length, then add it to the size of the length field itself
                    case TypeVarChar: {
                        const unsigned varcharLength = *reinterpret_cast<const unsigned*>(dataPtr);
                        recordSize += NUM_SIZE + varcharLength;
                        dataPtr += NUM_SIZE + varcharLength; // Move past this VarChar field
                        break;
                    }
                    case TypeInt:
                    case TypeReal:
                        recordSize += NUM_SIZE;
                        dataPtr += NUM_SIZE; // Move past this Int or Real field
                        break;
                }
            }
        }
        return recordSize;
    }

    unsigned RecordBasedFileManager::getTotalSlots(void *page) {
        unsigned numSlots;
        memcpy(&numSlots, (char*)page + PAGE_SIZE - 2 * SHORT_SIZE, SHORT_SIZE);
        return numSlots;
    }

    // given an rid, read the next record from *data into *record
    RC RecordBasedFileManager::readNextRecord(FileHandle fileHandle, RID rid, void *data, void *record) {
        char* page = (char*) data;
        unsigned short offset, length;

        memcpy(&offset, page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE),
               SHORT_SIZE);
        memcpy(&length,
               page + (PAGE_SIZE - 2 * SHORT_SIZE - rid.slotNum * 2 * SHORT_SIZE + SHORT_SIZE),
               SHORT_SIZE);
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

