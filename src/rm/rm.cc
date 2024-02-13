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

    RC RelationManager::createCatalog() {
        RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();
        FileHandle fileHandle;
        RID rid;
        // Create tables and columns tables, return error if either fails
        if (rbfm.createFile("Tables"))
            return -1;
        if (rbfm.createFile("Columns"))
            return -1;

        std::vector<Attribute> tablesRecordDescriptor;
        std::vector<Attribute> columnsRecordDescriptor;
        createTablesRecordDescriptor(tablesRecordDescriptor);
        createColumnsRecordDescriptor(columnsRecordDescriptor);

        char* data = new char[PAGE_SIZE];
        rbfm.insertRecord(fileHandle, tablesRecordDescriptor, data, rid);

        rbfm.insertRecord(fileHandle, columnsRecordDescriptor, data, rid);


        return 0;
    }

    RC RelationManager::deleteCatalog() {
        return -1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        return -1;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        return -1;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return -1;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return -1;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return -1;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return -1;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return -1;
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

        attr.name = "permission";
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

        attr.name = "permission";
        attr.type = PeterDB::TypeInt;
        attr.length = (PeterDB::AttrLength) 4;
        recordDescriptor.push_back(attr);


        return 0;
    }

    void RelationManager::prepareTablesRecord(const std::string &tableName, const std::vector<Attribute> &attrs, void *data) {
        unsigned id = getNextID();

    }
    void RelationManager::prepareColumnssRecord() {}


    int RelationManager::getActualByteForNullsIndicator(int fieldCount) {
        return ceil((double) fieldCount / 8.0);
    }

    unsigned char* RelationManager::initializeNullFieldsIndicator(const std::vector<PeterDB::Attribute> &recordDescriptor) {
        int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator((int) recordDescriptor.size());
        auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
        memset(indicator, 0, nullFieldsIndicatorActualSize);
        return indicator;
    }

    RC RelationManager::getNextTableID(unsigned &table_id) {
        RecordBasedFileManager *rbfm = &RecordBasedFileManager::instance();
        FileHandle fileHandle;

        rbfm->openFile("Tables", fileHandle);

        // Grab only the table ID
        std::vector<std::string> table_ids;
        table_ids.push_back("table-id");

        // Scan through all tables to get largest ID value
        RBFM_ScanIterator rbfm_si;
        rbfm->scan(fileHandle, tableDescriptor, "table-id", NO_OP, nullptr, table_ids, rbfm_si);

        RID rid;
        void *data = malloc (1 + sizeof(int));
        int32_t max_table_id = 0;
        while (rbfm_si.getNextRecord(rid, data) != EOF)
        {
            // Parse out the table id, compare it with the current max
            int32_t tid;
            fromAPI(tid, data);
            if (tid > max_table_id)
                max_table_id = tid;
        }

        free(data);
        // Next table ID is 1 more than largest table id
        table_id = max_table_id + 1;
        rbfm->closeFile(fileHandle);
        rbfm_si.close();
        return 0;

    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return RM_EOF; }

    RC RM_ScanIterator::close() { return -1; }

    // Extra credit work
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