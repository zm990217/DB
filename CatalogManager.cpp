#include "CatalogManager.h"

using namespace std;

#define UNKNOWN_FILE -10
#define TABLE_FILE -20
#define INDEX_FILE -30

CatalogManager::CatalogManager() {

}

CatalogManager::~CatalogManager() {

}

int CatalogManager::findIndex(string fileName) {
	fileNode *ftmp = bm.getFile("INDEX");
	blockNode *btmp = bm.getBlockHead(ftmp);
	if (btmp) {
		char* addressBegin;
		addressBegin = bm.getContent(*btmp);
		IndexInfo * i = (IndexInfo *)addressBegin;
		int flag = UNKNOWN_FILE;
		for (int j = 0; j < (bm.getUsingSize(*btmp) / sizeof(IndexInfo)); j++) {
			if ((*i).getIndexName() == fileName) {
				flag = INDEX_FILE;
				break;
			}
			i++;
		}
		return flag;
	}
	return 0;
}

int CatalogManager::addIndex(string indexName, string tableName, string Attribute, int type) {
	fileNode *ftmp = bm.getFile("INDEX");
	blockNode *btmp = bm.getBlockHead(ftmp);
	IndexInfo i(indexName, tableName, Attribute, type);
	while (true) {
		if (btmp == NULL) {
			return 0;
		}
		if (bm.getUsingSize(*btmp) <= bm.getBlockSize() - sizeof(IndexInfo)) {
			char* addressBegin;
			addressBegin = bm.getContent(*btmp) + bm.getUsingSize(*btmp);
			memcpy(addressBegin, &i, sizeof(IndexInfo));
			bm.setUsingSize(*btmp, bm.getUsingSize(*btmp) + sizeof(IndexInfo));
			bm.setDirty(*btmp);

			return this->setIndexOnAttribute(tableName, Attribute, indexName);
		}
		else {
			btmp = bm.getNextBlock(ftmp, btmp);
		}
	}
	return 0;
}

int CatalogManager::dropIndex(string index) {
	fileNode *ftmp = bm.getFile("INDEX");
	blockNode *btmp = bm.getBlockHead(ftmp);
	if (btmp) {
		char* addressBegin;
		addressBegin = bm.getContent(*btmp);
		IndexInfo * i = (IndexInfo *)addressBegin;
		int j = 0;
		for (j = 0; j < (bm.getUsingSize(*btmp) / sizeof(IndexInfo)); j++) {
			if ((*i).getIndexName() == index)
				break;
			i++;
		}
		this->revokeIndexOnAttribute((*i).getTableName(), (*i).getAttribute(), (*i).getIndexName());
		for (; j < (bm.getUsingSize(*btmp) / sizeof(IndexInfo) - 1); j++) {
			(*i) = *(i + sizeof(IndexInfo));
			i++;
		}
		bm.setUsingSize(*btmp, bm.getUsingSize(*btmp) - sizeof(IndexInfo));
		bm.setDirty(*btmp);

		return 1;
	}
	return 0;
}

int CatalogManager::getIndexNameList(string tableName, vector<string>* indexNameVector) {
	fileNode *ftmp = bm.getFile("INDEX");
	blockNode *btmp = bm.getBlockHead(ftmp);
	if (btmp) {
		char* addressBegin;
		addressBegin = bm.getContent(*btmp);
		IndexInfo * i = (IndexInfo *)addressBegin;
		for (int j = 0; j < (bm.getUsingSize(*btmp) / sizeof(IndexInfo)); j++) {
			if ((*i).getTableName() == tableName) {
				(*indexNameVector).push_back((*i).getIndexName());
			}
			i++;
		}
		return 1;
	}
	return 0;
}

int CatalogManager::getAllIndex(vector<IndexInfo> * indexs) {
	fileNode *ftmp = bm.getFile("INDEX");
	blockNode *btmp = bm.getBlockHead(ftmp);
	if (btmp) {
		char* addressBegin;
		addressBegin = bm.getContent(*btmp);
		IndexInfo * i = (IndexInfo *)addressBegin;
		for (int j = 0; j < (bm.getUsingSize(*btmp) / sizeof(IndexInfo)); j++) {
			indexs->push_back((*i));
			i++;
		}
	}
	return 1;
}

int CatalogManager::getIndexType(string indexName) {
	fileNode *ftmp = bm.getFile("INDEX");
	blockNode *btmp = bm.getBlockHead(ftmp);
	if (btmp) {
		char* addressBegin;
		addressBegin = bm.getContent(*btmp);
		IndexInfo * i = (IndexInfo *)addressBegin;
		for (int j = 0; j < (bm.getUsingSize(*btmp) / sizeof(IndexInfo)); j++) {
			if ((*i).getIndexName() == indexName) {
				return i->getType();
			}
			i++;
		}
		return -2;
	}
	return -2;
}

int CatalogManager::setIndexOnAttribute(string tableName, string AttributeName, string indexName) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		char* addressBegin = bm.getContent(*btmp);
		addressBegin += 1 + sizeof(int);
		int size = *addressBegin;
		addressBegin++;
		Attribute *a = (Attribute *)addressBegin;
		int i;
		for (i = 0; i < size; i++) {
			if (a->getName() == AttributeName) {
				a->setIndex(indexName);
				bm.setDirty(*btmp);
				break;
			}
			a++;
		}
		if (i < size)
			return 1;
		else
			return 0;
	}
	return 0;
}

int CatalogManager::revokeIndexOnAttribute(string tableName, string AttributeName, string indexName) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		char* addressBegin = bm.getContent(*btmp);
		addressBegin += (1 + sizeof(int));
		int size = *addressBegin;
		addressBegin++;
		Attribute *a = (Attribute *)addressBegin;
		int i;
		for (i = 0; i < size; i++) {
			if (a->getName() == AttributeName) {
				if (a->getIndex() == indexName) {
					a->getIndex() = "";
					bm.setDirty(*btmp);
				}
				else {
					cout << "The index: " << indexName << " you want to revoke is unknown on " << tableName << "!" << endl;
					cout << "Attribute: " << AttributeName << " on table " << tableName << " has index: " << a->getIndex() << "!" << endl;
				}
				break;
			}
			a++;
		}
		if (i < size)
			return 1;
		else
			return 0;
	}
	return 0;
}

int CatalogManager::findTable(string tableName) {
	FILE *fp;
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fp = fopen(tableFileName.c_str(), "r");
	if (fp == NULL) {
		return 0;
	}
	else {
		fclose(fp);
		return TABLE_FILE;
	}
}

int CatalogManager::addTable(string tableName, vector<Attribute>* attributeVector, string primaryKeyName = "", int primaryKeyLocation = 0) {
	FILE *fp;
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fp = fopen(tableFileName.c_str(), "w+");
	if (fp == NULL)
		return 0;
	fclose(fp);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		char* addressBegin = bm.getContent(*btmp);
		int * size = (int*)addressBegin;
		*size = 0;// 0 record number
		addressBegin += sizeof(int);
		*addressBegin = primaryKeyLocation;
		addressBegin++;
		*addressBegin = (*attributeVector).size();//the number of attributes
		addressBegin++;
		for (int i = 0; i < (*attributeVector).size(); i++) {
			memcpy(addressBegin, &((*attributeVector)[i]), sizeof(Attribute));
			addressBegin += sizeof(Attribute);
		}
		bm.setUsingSize(*btmp, bm.getUsingSize(*btmp) + (*attributeVector).size() * sizeof(Attribute) + 2 + sizeof(int));
		bm.setDirty(*btmp);
		return 1;
	}
	return 0;
}

int CatalogManager::dropTable(string tableName) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	bm.deleteFileNode(tableFileName.c_str());
	if (remove(tableFileName.c_str()))
		return 0;
	return 1;
}

int CatalogManager::insertRecord(string tableName, int recordNum) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		char* addressBegin = bm.getContent(*btmp);
		int * originalRecordNum = (int*)addressBegin;
		*originalRecordNum += recordNum;
		bm.setDirty(*btmp);
		return *originalRecordNum;
	}
	return 0;
}

int CatalogManager::deleteValue(string tableName, int deleteNum) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		char* addressBegin = bm.getContent(*btmp);
		int * recordNum = (int*)addressBegin;
		if ((*recordNum) < deleteNum) {
			cout << "Error! The number of records is less than the number to delete!" << endl;
			return 0;
		}
		else
			(*recordNum) -= deleteNum;

		bm.setDirty(*btmp);
		return *recordNum;
	}
	return 0;
}

int CatalogManager::getRecordNum(string tableName) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		char* addressBegin = bm.getContent(*btmp);
		int * recordNum = (int*)addressBegin;
		return *recordNum;
	}
	return 0;
}

int CatalogManager::getRecordString(string tableName, vector<string>* recordContent, char* recordResult) {
	vector<Attribute> attributeVector;
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	getAttribute(tableName, &attributeVector);//get all the attributes
	char * contentBegin = recordResult;

	for (int i = 0; i < attributeVector.size(); i++) {
		Attribute attribute = attributeVector[i];
		string content = (*recordContent)[i];//content in string form
		int type = attribute.getType();
		int typeSize = calcuteLenth(type);
		stringstream ss;
		ss << content;
		if (type == Attribute::TYPE_INT) {
			//if the attribute type is an integer
			if (isDigit(content)) {
				int intTmp;
				ss >> intTmp;
				memcpy(contentBegin, ((char*)&intTmp), typeSize);
			}
			else {
				string name = attributeVector[i].getName();
				cout << "Insert failed! Incorrect integer value: '" << content << "' for column '" << name << "'." << endl;
				return 0;
			}
		}
		else if (type == Attribute::TYPE_FLOAT) {
			//if the attribute type is a float
			if (isDigit(content)) {
				float floatTmp;
				ss >> floatTmp;
				memcpy(contentBegin, ((char*)&floatTmp), typeSize);
			}
			else {
				string name = attributeVector[i].getName();
				cout << "Insert failed! Incorrect float value: '" << content << "' for column '" << name << "'." << endl;
				return 0;
			}
		}
		else {
			//if the attribute type is a string
			memcpy(contentBegin, content.c_str(), typeSize);
		}

		contentBegin += typeSize;
	}
	return 1;
}

int CatalogManager::getAttribute(string tableName, vector<Attribute>* attributeVector) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		char* addressBegin = bm.getContent(*btmp);
		addressBegin += 1 + sizeof(int);
		int size = *addressBegin;
		addressBegin++;
		Attribute *a = (Attribute *)addressBegin;
		for (int i = 0; i < size; i++) {
			attributeVector->push_back(*a);
			a++;
		}

		return 1;
	}
	return 0;
}

int CatalogManager::calcuteLenth(int type) {
	if (type == Attribute::TYPE_INT) {
		return sizeof(int);
	}
	else if (type == Attribute::TYPE_FLOAT)
		return sizeof(float);
	else
		return (int)type * sizeof(char);
}

int CatalogManager::calcuteLenth(string tableName) {
	string tableFileName;
	tableFileName = getTableFileName(tableName);
	fileNode *ftmp = bm.getFile(tableFileName.c_str());
	blockNode *btmp = bm.getBlockHead(ftmp);

	if (btmp) {
		int singleRecordSize = 0;
		char* addressBegin = bm.getContent(*btmp);
		addressBegin += 1 + sizeof(int);
		int size = *addressBegin;
		addressBegin++;
		Attribute *a = (Attribute *)addressBegin;
		for (int i = 0; i < size; i++) {
			if ((*a).getType() == -1) {
				singleRecordSize += sizeof(float);
			}
			else if ((*a).getType() == 0) {
				singleRecordSize += sizeof(int);
			}
			else if ((*a).getType() > 0) {
				singleRecordSize += (*a).getType() * sizeof(char);
			}
			else {
				cout << "Catalog data was damaged!" << endl;
				return 0;
			}
			a++;
		}
		return singleRecordSize;
	}
	return 0;
}

string CatalogManager::getTableFileName(string tableName) {
	string tmp = "";
	return tmp + "CATALOG_" + tableName;
}

bool CatalogManager::isDigit(string str)
{
	stringstream sin(str);
	double d;
	char c;
	if (!(sin >> d)){
		return false;
	}
	if (sin >> c)
	{
		return false;
	}
	return true;
}