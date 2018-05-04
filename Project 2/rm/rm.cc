
#include "rm.h"

#define SUCCESS 0
#define SYS_TABLE 0
#define USER_TABLE 1


RelationManager* RelationManager::_rm = 0;
RecordBasedFileManager* RelationManager::_rbf_manager = NULL;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    _rbf_manager = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RM_ScanIterator::RM_ScanIterator() {
    rbsi = new RBFM_ScanIterator();
}

RC RM_ScanIterator::close() {   
   rbsi->close();
   return SUCCESS;
}

RC RelationManager::createCatalog()
{
    // create "Tables.tbl" and "Columns.tbl" to hold the tables
    if(_rbf_manager->createFile("Tables.tbl") != SUCCESS) return -1;
    if(_rbf_manager->createFile("Columns.tbl") != SUCCESS) return -1;

    // create the table and column with records provided in doc
    vector<Attribute> table = tableAttr();
    vector<Attribute> column = columnAttr();

    // Catalog Filehandle fh
    FileHandle fh;
    if(_rbf_manager->openFile("Tables.tbl", fh) != SUCCESS) return -1;

    // set up table-id counter and table page column page
    RID rid;
    int size = 1 + INT_SIZE + 50 + 50 + INT_SIZE;
    void* table_data = malloc(size);
    
    if(setTableInitial( 1, "Tables", "Tables.tbl", SYS_TABLE, table_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, table, table_data, rid) != SUCCESS) return -1;
    
    if(setTableInitial( 2, "Columns", "Columns.tbl", SYS_TABLE, table_data) != SUCCESS) return -1;  
    if(_rbf_manager->insertRecord(fh, column, table_data, rid) != SUCCESS) return -1;
    
    if(_rbf_manager->closeFile(fh) != SUCCESS) return -1;
    
    /* ------------------------------------------------ */
    // column table

    if(_rbf_manager->openFile("Columns.tbl", fh) != SUCCESS) return -1;

    RID rid_col;
    void* col_data = malloc(PAGE_SIZE);

    // table
    if(setColumnInitial( 1, "table-id", TypeInt, INT_SIZE, 1, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    if(setColumnInitial( 1, "table-name", TypeVarChar, 50, 2, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    if(setColumnInitial( 1, "file-name", TypeVarChar, 50, 3, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    if(setColumnInitial( 1, "table-flag", TypeInt, INT_SIZE, 4, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    // column 
    if(setColumnInitial( 2, "table-id", TypeInt, INT_SIZE, 1, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    if(setColumnInitial( 2, "column-name", TypeVarChar, 50, 2, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    if(setColumnInitial( 2, "column-type", TypeInt, INT_SIZE, 3, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    if(setColumnInitial( 2, "column-length", TypeInt, INT_SIZE, 4, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    if(setColumnInitial( 2, "column-position", TypeInt, INT_SIZE, 5, col_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord(fh, column, col_data, rid_col) != SUCCESS) return -1;

    free(table_data);
    free(col_data);
    if(_rbf_manager->closeFile(fh) != SUCCESS) return -1;  

    maxTableID = 2;   
    return SUCCESS;
}


RC RelationManager::deleteCatalog()
{
    if(_rbf_manager->destroyFile("Tables.tbl") != SUCCESS) return -1;
    if(_rbf_manager->destroyFile("Columns.tbl") != SUCCESS) return -1;
    
    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    string fileName = tableName + ".tbl";
    if(_rbf_manager->createFile(fileName) != SUCCESS) return -1;
    
    FileHandle fh;
    if(_rbf_manager->openFile("Tables.tbl", fh) != SUCCESS) return -1;

    maxTableID++;
    int tableID = maxTableID;
    RID table_rid, column_rid;
    void* table_data = malloc(PAGE_SIZE);
    void* column_data = malloc(PAGE_SIZE);
    vector<Attribute> table_attrs = tableAttr();
    vector<Attribute> column_attrs = columnAttr();

    if(setTableInitial( tableID, tableName, fileName, USER_TABLE, table_data) != SUCCESS) return -1;
    if(_rbf_manager->insertRecord( fh, table_attrs, table_data, table_rid) != SUCCESS) return -1;    

    if(_rbf_manager->closeFile(fh) != SUCCESS) return -1;

    if(_rbf_manager->openFile("Columns.tbl", fh) != SUCCESS) return -1;
    
    Attribute attr;
    for(unsigned int i = 0; i < attrs.size(); i++) {
        attr = attrs.at(i);
        if(setColumnInitial( tableID, attr.name, attr.type, attr.length, i + 1, column_data) != SUCCESS) return -1;
        if(_rbf_manager->insertRecord( fh, column_attrs, column_data, column_rid) != SUCCESS) return -1; 
    }

    if(_rbf_manager->closeFile(fh) != SUCCESS) return -1;
    
    free(table_data);
    free(column_data);
    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle fh;
	if(_rbf_manager->openFile(tableName + ".tbl", fh) != SUCCESS) return -1;
	
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	
	if(_rbf_manager->insertRecord(fh, recordDescriptor, data, rid) != SUCCESS) return -1;
	
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle fh;
	if(_rbf_manager->openFile(tableName + ".tbl", fh) != SUCCESS) return -1;
	
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	
	if(_rbf_manager->deleteRecord(fh, recordDescriptor, rid) != SUCCESS) return -1;
	
    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	FileHandle fh;
	if(_rbf_manager->openFile(tableName + ".tbl", fh) != SUCCESS) return -1;
	
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	
	if(_rbf_manager->updateRecord(fh, recordDescriptor, data, rid) != SUCCESS) return -1;
	
    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle fh;
	if(_rbf_manager->openFile(tableName + ".tbl", fh) != SUCCESS) return -1;
	
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	
	if(_rbf_manager->readRecord(fh, recordDescriptor, rid, data) != SUCCESS) return -1;
	
    return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	if(_rbf_manager->printRecord(attrs, data) != SUCCESS) return -1;
	return SUCCESS;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	FileHandle fh;
	if(_rbf_manager->openFile(tableName + ".tbl", fh) != SUCCESS) return -1;
	
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	
	if(_rbf_manager->readAttribute(fh, recordDescriptor, rid, attributeName, data) != SUCCESS) return -1;
	
    return SUCCESS;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	FileHandle fh;
	if(_rbf_manager->openFile(tableName + ".tbl", fh) != SUCCESS) return -1;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	
	return _rbf_manager->scan(fh, recordDescriptor, conditionAttribute, compOp, value, attributeNames, *rm_ScanIterator.rbsi);

}


// tableAttr() + columnAttr() will be used for createCatalog()
// still working on createCatalog() on my local copy. 
vector<Attribute> RelationManager::tableAttr() {

    vector<Attribute> table;
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    table.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    table.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    table.push_back(attr);

    // "table-flag" will be used to determine if
    // system table(SYS_TBL) or user table(USER_TBL)
    attr.name = "table-flag";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    table.push_back(attr);    

    return table;
}

vector<Attribute> RelationManager::columnAttr() {

    vector<Attribute> column;
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    column.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    column.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    column.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    column.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    column.push_back(attr);

    return column;
}

RC RelationManager::setTableInitial(const int tableID, const string &tableName, const string &fileName, const int tableFlag, void *data) {

	int offset = 0;
	int tableName_len = tableName.length();
	int fileName_len = fileName.length();

    int nullIndicatorSize = int (ceil((double) 4 / CHAR_BIT));
    char nullIndicator[nullIndicatorSize];

    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy((char*) data, nullIndicator, nullIndicatorSize);
    offset = nullIndicatorSize;

    memcpy((char *)data + offset, &tableID, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char *)data + offset, &tableName_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;

    memcpy((char *)data + offset, &tableName, tableName_len);
    offset += tableName_len;
    
    memcpy((char *)data + offset, &fileName_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;

    memcpy((char *)data + offset, &fileName, fileName_len);
    offset += fileName_len;

    memcpy((char *)data + offset, &tableFlag, INT_SIZE);
    offset += INT_SIZE;

    return SUCCESS;
}

RC RelationManager::setColumnInitial(const int tableID, const string &columnName, const int columnType, const int columnLength, const int columnPosition, void* data) {

	int offset = 0;
	int columnName_len = columnName.length();
	
	int nullIndicatorSize = int (ceil((double) 5 / CHAR_BIT));
	char nullIndicator[nullIndicatorSize];

    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy((char*) data, nullIndicator, nullIndicatorSize);
    offset = nullIndicatorSize;

	memcpy((char *)data + offset, &tableID, INT_SIZE);
	offset += INT_SIZE;

	
	memcpy((char *)data + offset, &columnName_len, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;

	memcpy((char *)data + offset, &columnName, columnName_len);
	offset += columnName_len;

	memcpy((char *)data + offset, &columnType, INT_SIZE);
	offset += INT_SIZE;

	memcpy((char *)data + offset, &columnLength, INT_SIZE);
	offset += INT_SIZE;

	memcpy((char *)data + offset, &columnPosition, INT_SIZE);
	offset += INT_SIZE;

	return SUCCESS;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    return rbsi->getNextRecord(rid, data);
}
