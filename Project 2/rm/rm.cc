
#include "rm.h"

#define SUCCESS 0



RelationManager* RelationManager::_rm = 0;

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
    
    if(_rbf_manager->openFile("Columns.tbl", fh) != SUCCESS) return -1;
    
    _rbf_manager->close(fh);
    
    return SUCCESS;
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
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
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

RC RelationManager::setTableInitial(const int tableID, const string &tableNname, const string &fileName, const int tableFlag, void *data) {

	int offset = 0;
	int tableName_len = tableName.length();
	int fileName_len = fileName.length();

    int nullIndicatorSize = int (ceil((double) 4 / CHAR_BIT));
    char nullIndicator[nullIndicatorSize];

    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);
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
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);
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
