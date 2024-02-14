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

    RC RelationManager::insertTable(const std::string &tableName, unsigned tableID, bool isSystem) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RID rid;

        if (rbfm.openFile("Tables", fileHandle)) {
            return -1;
        }

        char *data = new char[TABLES_RECORD_SIZE];
        prepareTablesRecord(tableName, tableID, isSystem, (void*)data);
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

        if (insertTable("Columns", 1, true)) {
            return -1;
        }

        // Add columns entries from Tables and Columns to Columns table
        if (insertColumns(1, tableDescriptor)) {
            return -1;
        }
        if (insertColumns(1, columnDescriptor)) {
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
        RID rid;
        unsigned id;
        getNextTablesID(id);

        return -1;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        // Clear out any old values
        attrs.clear();
        RC rc;

        unsigned table_id;
        getTableID(tableName, table_id);

        void *value = &table_id;

        RBFM_ScanIterator rbfm_si;
        std::vector<std::string> projection{"column-name", "column-type", "column-length", "column-position"};

        FileHandle fileHandle;
        if (rbfm.openFile("Columns", fileHandle)) {
            return -1;
        }

        // Scan through the Column table for all entries whose table-id equals tableName's table id.
        rbfm.scan(fileHandle, columnDescriptor, "table_id", EQ_OP, value, projection, rbfm_si);


        RID rid;
        char *data = new char[PAGE_SIZE];
        return 0;

    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        std::vector<Attribute> recordDescriptor;

//        bool isSystem;
//        rc = isSystemTable(isSystem, tableName);
//        if (rc)
//            return rc;
//        if (isSystem)
//            return RM_CANNOT_MOD_SYS_TBL;

        getAttributes(tableName, recordDescriptor);
        if (rbfm.openFile(tableName, fileHandle)) {
            return -1;
        }

        rbfm.insertRecord(fileHandle, recordDescriptor, data, rid);

        return 0;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        RC rc;

//        bool isSystem;
//        rc = isSystemTable(isSystem, tableName);
//        if (rc)
//            return rc;
//        if (isSystem)
//            return -1;


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
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

//        bool isSystem;
//        rc = isSystemTable(isSystem, tableName);
//        if (rc)
//            return rc;
//        if (isSystem)
//            return -1;

        std::vector<Attribute> recordDescriptor;
        if (getAttributes(tableName, recordDescriptor)) {
            return -1;
        }
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
        };
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
        if (getAttributes(tableName, recordDescriptor)) {
            return -1;
        }
        FileHandle fileHandle;
        if (rbfm.openFile(tableName, fileHandle)) {
            return -1;
        }

        if (rbfm.readAttribute(fileHandle, recordDescriptor, rid, attributeName, data)) {
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

        attr.name = "sys";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);

        return 0;
    }

    void RelationManager::prepareTablesRecord(const std::string &tableName, unsigned &tableId, bool isSystem, void *data) {
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
        unsigned sysByte = (isSystem) ? 1 : 0;
        memcpy(dataPtr, &sysByte, sizeof(unsigned));

        delete[] nullIndicator;
    }

    void RelationManager::prepareColumnsRecord(unsigned &tableID, const Attribute &attr, unsigned &position, void *data, bool isSystem) {

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
        unsigned sysByte = (isSystem) ? 1 : 0;
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
        while (rbfm_si.getNextRecord(rid, (void*)data) != EOF)
        {
            // Parse out the table id, compare it with the current max
            unsigned tid;
            parseInt(tid, data);
            if (tid > max_table_id)
                max_table_id = tid;
        }
        free(data);
        // Next table ID is 1 more than the largest table id
        table_id = max_table_id + 1;
        rbfm.closeFile(fileHandle);
        rbfm_si.close();
        return 0;
    }

    RC RelationManager::getTableID(const std::string &table_name, unsigned &table_id) {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;

        rbfm.openFile(table_name, fileHandle);

        std::vector<std::string> attributes{"table-id"};
        RBFM_ScanIterator rbfm_si;

        char *value = new char[4 + 10];
        unsigned str_length = table_name.length();
        memcpy(value, &str_length, sizeof(unsigned));
        memcpy(value + sizeof(unsigned), table_name.c_str(), str_length);

        rbfm.scan(fileHandle, tableDescriptor, "table-name", EQ_OP, (void*)value, attributes, rbfm_si);

        RID rid;
        char* data = new char[sizeof(int) + 1];
        if (rbfm_si.getNextRecord(rid, data) != EOF) {
            unsigned tid;
            parseInt(tid, data);
            table_id = tid;
        }
        delete[] value;
        delete[] data;
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
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        rbfm_iter.getNextRecord(rid,data);
        return 0;
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