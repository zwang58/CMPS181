
#include "rm.h"

#define SUCCESS 0



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
            
    // call to setTableInitial();        
    // _rbf_manager->insertRecord    

    if(_rbf_manager->openFile("Columns.tbl", fh) != SUCCESS) return -1;

    // call to setColumnInitial();
    // _rbf_manager->insertRecord

    return -1;
}


RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
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
    return -1;
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

RC RelationManager::setTableInitial(const int table-id, const string &table-name, const string &file-name, const int table-flag, void *data) {

	int offset = 0;
	int tableName_len = table-name.length();
	int fileName_len = file-name.length();

    int nullIndicatorSize = int (ceil((double) 4 / CHAR_BIT));
    char nullIndicator[nullIndicatorSize];

    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);
    offset = nullIndicatorSize;

    memcpy((char *)data + offset, &table-id, INT_SIZE);
    offset += INT_SIZE;

    memcpy((char *)data + offset, &tableName_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;

    memcpy((char *)data + offset, &table-name, tableName_len);
    offset += tableName_len;
    
    memcpy((char *)data + offset, &fileName_len, VARCHAR_LENGTH_SIZE);
    offset += VARCHAR_LENGTH_SIZE;

    memcpy((char *)data + offset, &file-name, fileName_len);
    offset += fileName_len;

    memcpy((char *)data + offset, &table-flag, INT_SIZE);
    offset += INT_SIZE;

    return SUCCESS;
}

RC RelationManager::setColumnInitial(const int table-id, const string &column-name, const int &column-type, const int column-length, const int column-position, void* data) {

	int offset = 0;
	int columnName_len = column-name.length();
	
	int nullIndicatorSize = int (ceil((double) 5 / CHAR_BIT));
	char nullIndicator[nullIndicatorSize];

    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);
    offset = nullIndicatorSize;

	memcpy((char *)data + offset, &table-id, INT_SIZE);
	offset += INT_SIZE;

	
	memcpy((char *)data + offset, &columnName_len, VARCHAR_LENGTH_SIZE);
	offset += VARCHAR_LENGTH_SIZE;

	memcpy((char *)data + offset, &column-name, columnName_len);
	offset += columnName_len;

	memcpy((char *)data + offset, &column-type, INT_SIZE);
	offset += INT_SIZE;

	memcpy((char *)data + offset, &column-length, INT_SIZE);
	offset += INT_SIZE;

	memcpy((char *)data + offset, &column-position, INT_SIZE);
	offset += INT_SIZE;

	return SUCCESS;
}
