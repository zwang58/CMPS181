
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <climits>
#include <cstring>
#include <iostream>
#include <cmath>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator
#define SUCCESS 0

// used for table-flag to distinguish between
// a system table and user table
#define SYS_TBL 0
#define USER_TBL 1


// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator();
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
  
friend class RelationManager;
private:
  RBFM_ScanIterator *rbsi;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);



  // helper functions for createCatalog
  RC setTableInitial(const int tableID, const string &tableName, const string &fileName, const int tableFlag, void *data);

  RC setColumnInitial(const int tableID, const string &columnName, const int columnType, const int columnLength, const int columnPosition, void* data);
protected:
  RelationManager();
  ~RelationManager();

friend class RM_ScanIterator;
private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbf_manager;
  
  // used to hold number of table-id entries
  int maxTableID;

  vector<Attribute> tableAttr();
  vector<Attribute> columnAttr();  
  
    
};

#endif
