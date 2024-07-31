#include "src/include/rm.h"

#include <cstring>
#include <cmath>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::insertTable(const std::string &tableName, unsigned tableID, bool isSys) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RID rid;

        if (rbfm.openFile("Tables", fileHandle)) {
            return -1;
        }

        char *data = new char[TABLES_RECORD_SIZE];
        prepareTablesRecord(tableName, tableID, isSys, (void*)data);
        rbfm.insertRecord(fileHandle, tableDescriptor, (void*)data, rid);
        rbfm.closeFile(fileHandle);
        delete[] data;
        return 0;

    }

    RC RelationManager::insertColumns(unsigned tableID, const std::vector<Attribute> &recordDescriptor) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RID rid;
        if (rbfm.openFile("Columns", fileHandle)) {
            return -1;
        }
        char *data = new char[COLUMNS_RECORD_SIZE];

        for (int i = 0; i < recordDescriptor.size(); i++) {
            unsigned position = i+1;
            prepareColumnsRecord(tableID, recordDescriptor[i], position, data, true);
            rbfm.insertRecord(fileHandle, columnDescriptor, data, rid);
        }

        rbfm.closeFile(fileHandle);
        delete[] data;
        return 0;
    }


    RC RelationManager::createCatalog() {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        tableDescriptor.clear();
        columnDescriptor.clear();
        createTablesRecordDescriptor(tableDescriptor);
        createColumnsRecordDescriptor(columnDescriptor);

        // Create Tables and Columns tables
        if (rbfm.createFile("Tables")) {
            return -1;
        }
        if (rbfm.createFile("Columns")) {
            return -1;
        }
        // Add table entries for Tables and Columns
        if (insertTable("Tables", 1, true)) {
            return -1;
        }

        if (insertTable("Columns", 2, true)) {
            return -1;
        }

        // Add columns entries from Tables and Columns to Columns table
        if (insertColumns(1, tableDescriptor)) {
            return -1;
        }
        if (insertColumns(2, columnDescriptor)) {
            return -1;
        }

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        if (rbfm.destroyFile("Tables")) {
            return -1;
        }

        if (rbfm.destroyFile("Columns")) {
            return -1;
        }

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        unsigned fieldSize = attrs.size();

        // Get tableID
        unsigned tableID;
        if (getNextTablesID(tableID)) {
            return -1;
        }

        if (rbfm.createFile(tableName)) {
            return -1;
        }

        // Insert table into Tables
        if (insertTable(tableName, tableID, false)){
            return -1;
        }

        // Insert the table's columns into Columns
        if (insertColumns(tableID, attrs)) {
            return -1;
        }
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        bool isSys = false;
        if (checkSys(isSys, tableName)) // table might not exist
            return -1;
        if (isSys)
            return -1;

        // get tableID
        unsigned tableId;
        if (getTableID(tableName, tableId)) {
            return -1;
        }
        // Delete the tableName file
        if (rbfm.destroyFile(tableName)) {
            return -1;
        }


        // Find entry with same table ID
        RBFM_ScanIterator rbfm_si;
        FileHandle fileHandle;
        std::vector<std::string> attrs; // Don't need to project anything
        void *value = &tableId;

        // Delete tableName from Tables
        if (rbfm.openFile("Tables", fileHandle)) {
            return -1;
        }
        rbfm.scan(fileHandle, tableDescriptor, "table-id", EQ_OP, value, attrs, rbfm_si);
        RID rid_t;
        RC result;
        while ((result = rbfm_si.getNextRecord(rid_t, nullptr)) != RBFM_EOF) {
            if (result == 0) {
                if (rbfm.deleteRecord(fileHandle, tableDescriptor, rid_t)) {
                    return -1;
                }
            }
        }
        rbfm.closeFile(fileHandle);
        rbfm_si.close();

        // Delete from Columns table
        if (rbfm.openFile("Columns", fileHandle)) {
            return -1;
        }
        rbfm.scan(fileHandle, columnDescriptor, "table-id", EQ_OP, value, attrs, rbfm_si);
        RID rid_c;
        while ((result = rbfm_si.getNextRecord(rid_c, nullptr)) != RBFM_EOF) {
            if (result == 0) {
                if (rbfm.deleteRecord(fileHandle, columnDescriptor, rid_c)) {
                    return -1;
                }
            }
        }
        rbfm.closeFile(fileHandle);
        rbfm_si.close();

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        unsigned table_id;
        if (getTableID(tableName, table_id)) {
            return -1;
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        if (rbfm.openFile("Columns", fileHandle)) {
            return -1;
        }

        void *value = &table_id;
        std::vector<std::string> attributeNames{"column-name", "column-type", "column-length"};
        RBFM_ScanIterator rbfm_si;
        // Scan through Columns for records with matching table_id.
        if (rbfm.scan(fileHandle, columnDescriptor, "table-id", EQ_OP, value, attributeNames, rbfm_si)) {
            rbfm.closeFile(fileHandle);
            return -1;
        }

        RID rid;
        RC result;
        attrs.clear();
        char *data = new char[PAGE_SIZE];
        while ((result = rbfm_si.getNextRecord(rid, data)) != RBFM_EOF) {
            if (result == 0) {
                unsigned nullIndicatorSize = ceil((double)attributeNames.size() / 8.0);
                char* dataPtr = (char*) data + nullIndicatorSize;

                Attribute attr;
                unsigned nameLength;
                memcpy(&nameLength, dataPtr, sizeof(unsigned));
                dataPtr += sizeof(unsigned);
                char* column_name = new char[nameLength + 1];
                memcpy(column_name, dataPtr, nameLength);
                column_name[nameLength] = '\0'; // Ensure null-termination
                attr.name = std::string(column_name); // Convert C-style string to C++ string
                delete[] column_name;
                dataPtr += nameLength; // Move past the column name

                // Column-type
                memcpy(&attr.type, dataPtr, sizeof(unsigned));
                dataPtr += sizeof(unsigned);

                // Column-name
                memcpy(&attr.length, dataPtr, sizeof(unsigned));

                attrs.push_back(attr);
            }
        }
        delete[] data;
        rbfm_si.close();
        if (rbfm.closeFile(fileHandle)) {
            return -1;
        }

        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        bool isSys = false;
        if (checkSys(isSys, tableName)) // table might not exist
            return -1;

        if (isSys)
            return -1;

        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) != 0) {
            return -1;
        };

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        if (rbfm.openFile(tableName, fileHandle)) {
            return -1;
        }

        rbfm.insertRecord(fileHandle, recordDescriptor, data, rid);
        rbfm.closeFile(fileHandle);


        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        bool isSys = false;
        if (checkSys(isSys, tableName)) // table might not exist
            return -1;

        if (isSys)
            return -1;


        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor)) {
            return -1;
        }
        FileHandle fileHandle;
        if (rbfm.openFile(tableName, fileHandle)) {
            return -1;
        }
        if (rbfm.deleteRecord(fileHandle, recordDescriptor, rid)) {
            return -1;
        }
        rbfm.closeFile(fileHandle);

        return 0;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        bool isSys = false;
        if (checkSys(isSys, tableName)) // table might not exist
            return -1;

        if (isSys)
            return -1;

        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor)) {
            return -1;
        }

        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        if (rbfm.openFile(tableName, fileHandle)) {
            return -1;
        }
        if (rbfm.updateRecord(fileHandle, recordDescriptor, data, rid)) {
            return -1;
        }
        rbfm.closeFile(fileHandle);

        return 0;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<Attribute> recordDescriptor;

        if (getAttributes(tableName, recordDescriptor)) {
            return -1;
        }
        FileHandle fileHandle;
        if (rbfm.openFile(tableName, fileHandle)) {
            return -1;
        }
        if (rbfm.readRecord(fileHandle, recordDescriptor, rid, data)) {
            return -1;
        }
        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        return rbfm.printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor) != 0) {
            return -1;
        }
        FileHandle fileHandle;
        if (rbfm.openFile(tableName, fileHandle) != 0) {
            return -1;
        }
        if (rbfm.readAttribute(fileHandle, recordDescriptor, rid, attributeName, data) != 0) {
            return -1;
        }

        rbfm.closeFile(fileHandle);
        return 0;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        if (rbfm.openFile(tableName, rm_ScanIterator.fileHandle)) {
            return -1;
        }
        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor)) {
            return -1;
        }
        rbfm.scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute,
                        compOp, value, attributeNames, rm_ScanIterator.rbfm_iter);

        return 0;

    }
    RC RelationManager::createTablesRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        PeterDB::Attribute attr;
        attr.name = "table-id";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "table-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        recordDescriptor.push_back(attr);

        attr.name = "file-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        recordDescriptor.push_back(attr);

        attr.name = "sys";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);

        return 0;
    }
    RC RelationManager::createColumnsRecordDescriptor(std::vector<PeterDB::Attribute> &recordDescriptor) {
        PeterDB::Attribute attr;

        attr.name = "table-id";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "column-name";
        attr.type = PeterDB::TypeVarChar;
        attr.length = (PeterDB::AttrLength) 50;
        recordDescriptor.push_back(attr);

        attr.name = "column-type";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "column-length";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);

        attr.name = "column-position";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);

        return 0;
    }

    void RelationManager::prepareTablesRecord(const std::string &tableName, unsigned &tableId, bool isSys, void *data) {
        /* need:
         * table-id
         * table-name
         * file-name (the same as table-name)
         * system table indicator
         */
        unsigned str_length = tableName.length();
        char* dataPtr = (char*)data;

        int nullIndicatorSize = ceil((double)tableDescriptor.size() / 8.0);
        char* nullIndicator = new char[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);

        memcpy(dataPtr, nullIndicator, nullIndicatorSize); // Copy null fields
        dataPtr += nullIndicatorSize;

        // table-id
        memcpy(dataPtr, &tableId, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        // table-name
        memcpy(dataPtr, &str_length, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        memcpy(dataPtr, tableName.c_str(), str_length);
        dataPtr += str_length;
        // file-name
        memcpy(dataPtr, &str_length, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        memcpy(dataPtr, tableName.c_str(), str_length);
        dataPtr += str_length;
        // system indicator
        unsigned sysByte = (isSys) ? 1 : 0;
        memcpy(dataPtr, &sysByte, sizeof(unsigned));

        delete[] nullIndicator;
    }

    void RelationManager::prepareColumnsRecord(unsigned &tableID, const Attribute &attr, unsigned &position, void *data, bool isSys) {

        unsigned str_length = attr.name.length();
        char* dataPtr = (char*)data;

        int nullIndicatorSize = ceil((double)1 / 8.0);
        char* nullIndicator = new char[nullIndicatorSize];
        memset(nullIndicator, 0, nullIndicatorSize);

        memcpy(dataPtr, nullIndicator, nullIndicatorSize); // Copy null fields
        dataPtr += nullIndicatorSize;
        // tableID
        memcpy(dataPtr, &tableID, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        // tableName
        memcpy(dataPtr, &str_length, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        memcpy(dataPtr, attr.name.c_str(), str_length);
        dataPtr += str_length;
        // column type
        AttrType type = attr.type;
        memcpy(dataPtr, &type, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        // column length
        AttrLength length = attr.length;
        memcpy(dataPtr, &length, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        // position
        memcpy(dataPtr, &position, sizeof(unsigned));
        dataPtr += sizeof(unsigned);
        // system indicator
        unsigned sysByte = (isSys) ? 1 : 0;
        memcpy(dataPtr, &sysByte, sizeof(unsigned));

        delete[] nullIndicator;
    }

    RC RelationManager::getNextTablesID(unsigned &table_id) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;

        if (rbfm.openFile("Tables", fileHandle)) {
            return -1;
        }
        // select table-id
        std::vector<std::string> attributes{"table-id"};
        RBFM_ScanIterator rbfm_si;

        rbfm.scan(fileHandle, tableDescriptor, "table-id", NO_OP, nullptr, attributes, rbfm_si);

        RID rid;
        char* data = new char[sizeof(int) + 1];
        unsigned max_table_id = 0;
        while (rbfm_si.getNextRecord(rid, (void*)data) != EOF) {
            // Parse out the table id, compare it with the current max
            unsigned tid;
            parseInt(tid, data);
            if (tid > max_table_id)
                max_table_id = tid;
        }
        delete[] data;
        // Next table ID is 1 more than the largest table id
        table_id = max_table_id + 1;
        rbfm.closeFile(fileHandle);
        rbfm_si.close();
        return 0;
    }

    RC RelationManager::getTableID(const std::string &table_name, unsigned &table_id) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;

        if (rbfm.openFile("Tables", fileHandle)) {
            return -1;
        }

        std::vector<std::string> attributes{"table-id"};
        RBFM_ScanIterator rbfm_si;

        unsigned str_length = table_name.length();
        char *value = new char[4 + str_length];
        memcpy(value, &str_length, sizeof(unsigned));
        memcpy(value + sizeof(unsigned), table_name.c_str(), str_length);

        rbfm.scan(fileHandle, tableDescriptor, "table-name", EQ_OP, (void*)value, attributes, rbfm_si);

        RID rid;
        char* data = new char[sizeof(int) + 1];
        RC result;
        while ((result = rbfm_si.getNextRecord(rid, data)) != RBFM_EOF) {
            if (result == 0) { // Found a matching record
                // Parse the system field from that table entry
                unsigned tid;
                parseInt(tid, (void*)data);
                table_id = tid;
                break; // Exit the loop if a match is found
            }
            // If result is -2, continue to the next record automatically
        }
        delete[] value;
        delete[] data;
        rbfm.closeFile(fileHandle);
        rbfm_si.close();

        return 0;
    }

    RC RelationManager::checkSys(bool &system, const std::string &tableName) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;

        if (rbfm.openFile("Tables", fileHandle)) {
            return -1;
        }

        // get sys column
        std::vector<std::string> attrs{"sys"};

        char *value = new char[5 + 50];
        unsigned name_len = tableName.length();
        memcpy(value, &name_len, sizeof(unsigned));
        memcpy(value + sizeof(unsigned), tableName.c_str(), name_len);

        // Find table whose table-name is equal to tableName
        RBFM_ScanIterator rbfm_si;
        rbfm.scan(fileHandle, tableDescriptor, "table-name", EQ_OP, value, attrs, rbfm_si);

        RID rid;
        char *data = new char[1 + sizeof(unsigned)];
        RC result;
        while ((result = rbfm_si.getNextRecord(rid, data)) != RBFM_EOF) {
            if (result == 0) { // Found a matching record
                // Parse the system field from that table entry
                unsigned sysByte;
                parseInt(sysByte, data);
                system = sysByte == 1;
                break; // Exit the loop if a match is found
            }
            // If result is -2, continue to the next record automatically
        }

        delete[] data;
        delete[] value;
        rbfm.closeFile(fileHandle);
        rbfm_si.close();
        return 0;
    }

    RC RelationManager::parseInt(unsigned &num, const void* data) {
        char null = 0;

        memcpy(&null, data, 1);
        if (null)
            return -1;

        unsigned tmp;
        memcpy(&tmp, (char*) data + 1, sizeof(unsigned));

        num = tmp;
        return 0;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        RC rc = rbfm_iter.getNextRecord(rid, data);
        if (rc == RBFM_EOF) {
            return RM_EOF;
        }
        return rc; // Return the actual error code or success status
    }

    RC RM_ScanIterator::close() {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        rbfm_iter.close();
        rbfm.closeFile(fileHandle);
        return 0;
    }

//     Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB