#define _CRT_SECURE_NO_WARNINGS

#ifndef CATALOGMANAGER_H_
#define CATALOGMANAGER_H_

#include <string>
#include <vector>
#include <iostream>
#include <sstream>

#include "BufferManager.h"
#include "IndexManager.h"

using namespace std;

class CatalogManager {
public:
	BufferManager bm;
	CatalogManager();//the constructor
	virtual ~CatalogManager();//the destructor
	int findIndex(string indexName); //find a designed indexname
	int addIndex(string indexName, string tableName, string attributeName, int type); //add an index to a table
	int dropIndex(string index); //drop a designed index	
	int getIndexNameList(string tableName, vector<string>* indexNameVector); //get index name list of a table
	int getAllIndex(vector<IndexInfo> * indexs); //get all indices of a table
	int getIndexType(string indexName);//get the type of an index
	int setIndexOnAttribute(string tableName, string AttributeName, string indexName); //set index on an attribute of a table
	int revokeIndexOnAttribute(string tableName, string AttributeName, string indexName); //revoke index of table
	int findTable(string tableName); //find out if a table exists
	int addTable(string tableName, vector<Attribute>* attributeVector, string primaryKeyName, int primaryKeyLocation); //create a table with attributes
	int dropTable(string tableName); //drop a designed table
	int insertRecord(string tableName, int recordNum);  // increment the number of records
	int deleteValue(string tableName, int deleteNum);// delete the number of records
	int getRecordNum(string tableName); //get the records number of a table
	int getRecordString(string tableName, vector<string>* recordContent, char* recordResult); //get the record string of a table by the table name and recordContent
	int getAttribute(string tableName, vector<Attribute>* attributeVector);//get the attributes of a table
	int calcuteLenth(int type); //calculate the length of a type
	int calcuteLenth(string tableName); //calculate the length of a table
	bool isDigit(string str); //judge if a string is a digit
	string getTableFileName(string tableName);//get the table catalog file name
};

#endif