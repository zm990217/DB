#include "RecordManager.h"
#include "API.h"
#include <cstring>

using namespace std;

RecordManager::RecordManager() {
	//default ctor
}

RecordManager::~RecordManager() {
	//default dtor
}

int RecordManager::createTable(string tableName) {
	string TableFileName;
	FILE *fp;

	TableFileName = getTableFileName(tableName); //get the table file name to open   
	fp = fopen(TableFileName.c_str(), "a+");
	if (fp == NULL)
		return 0;
	fclose(fp);
	return 1;
}

int RecordManager::dropTable(string tableName) {
	string TableFileName;

	TableFileName = getTableFileName(tableName);
	bm.deleteFileNode(TableFileName.c_str());

	if (remove(TableFileName.c_str()) == 1)
		return 0;
	return 1;
}

int RecordManager::createIndex(string indexName) {
	string IndexFileName;
	FILE *fp;

	IndexFileName = getIndexFileName(indexName);
	fp = fopen(IndexFileName.c_str(), "a+");
	if (fp == NULL)
		return 0;
	fclose(fp);
	return 1;
}

int RecordManager::dropIndex(string indexName) {
	string IndexFileName;

	IndexFileName = getIndexFileName(indexName);
	bm.deleteFileNode(IndexFileName.c_str());
	if (remove(IndexFileName.c_str()) == 1)
		return 0;
	return 1;
}

int RecordManager::insertRecord(string tableName, char* record, int recordSize) {
	fileNode *ftmp;
	blockNode *btmp;

	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	btmp = bm.getBlockHead(ftmp);

	while (true) {
		if (btmp == NULL)
			return -1;
		else {
			//the room of block is not enough
			if (bm.getUsingSize(*btmp) > bm.getBlockSize() - recordSize) {
				btmp = bm.getNextBlock(ftmp, btmp);
			}
			//the room of block is enough
			else {
				char* addressBegin;

				addressBegin = bm.getUsingSize(*btmp) + bm.getContent(*btmp);
				memcpy(addressBegin, record, recordSize);
				bm.setDirty(*btmp);
				bm.setUsingSize(*btmp, bm.getUsingSize(*btmp) + recordSize);
				return btmp->offsetNum;
			}
		}
	}

	return -1;
}

int RecordManager::recordAllShow(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector) {
	fileNode *ftmp;
	blockNode *btmp;
	int count;
	int RecordBlockNum;

	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	btmp = bm.getBlockHead(ftmp);
	count = 0;

	while (true) {
		if (btmp == NULL)
			return -1;
		else {
			//this block is done
			if (btmp->ifbottom == NULL) {
				RecordBlockNum = recordBlockShow(tableName, attributeNameVector, conditionVector, btmp);
				count = count + RecordBlockNum;
				btmp = bm.getNextBlock(ftmp, btmp);
			}
			//this block is not done
			else {
				RecordBlockNum = recordBlockShow(tableName, attributeNameVector, conditionVector, btmp);
				count = count + RecordBlockNum;
				return count;
			}
		}
	}

	return -1;
}

int RecordManager::recordBlockShow(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector, int blockOffset) {
	fileNode *ftmp;
	blockNode* block;

	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	block = bm.getBlockByOffset(ftmp, blockOffset);

	if (block)
		return  recordBlockShow(tableName, attributeNameVector, conditionVector, block);
	else
		return -1;
}

int RecordManager::recordBlockShow(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector, blockNode* block) {
	if (block == NULL)
		return -1;
	else {
		int COUNT, recordSize, ri, rj;
		char* recordBegin;
		char* blockBegin;
		size_t usingSize;
		vector<Attribute> attributeVector;

		COUNT = 0;

		recordBegin = bm.getContent(*block);
		recordSize = api->getRecordSize(tableName);
		api->getAttribute(tableName, &attributeVector);
		blockBegin = bm.getContent(*block);
		usingSize = bm.getUsingSize(*block);

		if (usingSize == 0)
			return COUNT;
		while (recordBegin - blockBegin < usingSize) {
			//if the recordBegin point to a record
			if (recordConditionFit(recordBegin, recordSize, &attributeVector, conditionVector)) {
				cout << "| " ;
				recordPrint(recordBegin, recordSize, &attributeVector, attributeNameVector);
				cout << "\n";
				COUNT = COUNT + 1;
			}
			recordBegin = recordBegin + recordSize;
		}

		return COUNT;
	}

}

int RecordManager::recordAllFind(string tableName, vector<Condition>* conditionVector) {
	fileNode *ftmp;
	blockNode *btmp;
	int COUNT, recordBlockNum;

	COUNT = 0;
	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	btmp = bm.getBlockHead(ftmp);

	while (true) {
		if (btmp) {
			if (btmp->ifbottom) {
				recordBlockNum = recordBlockFind(tableName, conditionVector, btmp);
				COUNT = COUNT + recordBlockNum;
				return COUNT;
			}
			else {
				recordBlockNum = recordBlockFind(tableName, conditionVector, btmp);
				COUNT = COUNT + recordBlockNum;
				btmp = bm.getNextBlock(ftmp, btmp);
			}
		}
		else {
			return -1;
		}
	}
	return -1;
}

int RecordManager::recordBlockFind(string tableName, vector<Condition>* conditionVector, blockNode* block) {
	//if block is null, return -1
	if (block) {
		int COUNT, recordSize;
		char* recordBegin;
		vector<Attribute> attributeVector;

		COUNT = 0;
		recordBegin = bm.getContent(*block);
		recordSize = api->getRecordSize(tableName);

		api->getAttribute(tableName, &attributeVector);

		while (recordBegin - bm.getContent(*block) < bm.getUsingSize(*block)) {
			if (recordConditionFit(recordBegin, recordSize, &attributeVector, conditionVector))
				COUNT += 1;

			recordBegin = recordBegin + recordSize;
		}

		return COUNT;
	}
	else {
		return -1;
	}
}

int RecordManager::recordAllDelete(string tableName, vector<Condition>* conditionVector) {
	fileNode *ftmp;
	blockNode *btmp;
	int COUNT, recordBlockNum;

	COUNT = 0;
	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	btmp = bm.getBlockHead(ftmp);

	while (true) {
		if (btmp) {
			if (btmp->ifbottom) {
				recordBlockNum = recordBlockDelete(tableName, conditionVector, btmp);
				COUNT = COUNT + recordBlockNum;
				return COUNT;
			}
			else {
				recordBlockNum = recordBlockDelete(tableName, conditionVector, btmp);
				COUNT = COUNT + recordBlockNum;
				btmp = bm.getNextBlock(ftmp, btmp);
			}
		}
		else {
			return -1;
		}

	}
	return -1;
}

int RecordManager::recordBlockDelete(string tableName, vector<Condition>* conditionVector, int blockOffset) {
	fileNode *ftmp;
	blockNode* block;

	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	block = bm.getBlockByOffset(ftmp, blockOffset);
	if (block)
		return  recordBlockDelete(tableName, conditionVector, block);
	else
		return -1;
}

int RecordManager::recordBlockDelete(string tableName, vector<Condition>* conditionVector, blockNode* block) {
	//if block is null, return -1
	if (block) {
		int COUNT, recordSize, ri;
		char* recordBegin;
		vector<Attribute> attributeVector;

		COUNT = 0;
		recordBegin = bm.getContent(*block);
		recordSize = api->getRecordSize(tableName);

		api->getAttribute(tableName, &attributeVector);

		while (recordBegin - bm.getContent(*block) < bm.getUsingSize(*block)) {
			//if the recordBegin point to a record
			if (recordConditionFit(recordBegin, recordSize, &attributeVector, conditionVector)) {
				COUNT += 1;

				api->deleteRecordIndex(recordBegin, recordSize, &attributeVector, block->offsetNum);
				for (ri = 0; ri + recordSize + recordBegin - bm.getContent(*block) < bm.getUsingSize(*block); ri++) {
					recordBegin[ri] = recordBegin[ri + recordSize];
				}
				memset(recordBegin + ri, 0, recordSize);
				bm.setUsingSize(*block, bm.getUsingSize(*block) - recordSize);
				bm.setDirty(*block);
			}
			else
				recordBegin = recordBegin + recordSize;
		}
		return COUNT;
	}
	else
		return -1;

}

int RecordManager::indexRecordAllAlreadyInsert(string tableName, string indexName) {
	fileNode *ftmp;
	blockNode *btmp;
	int COUNT, recordBlockNum;

	COUNT = 0;
	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	btmp = bm.getBlockHead(ftmp);

	while (true) {
		if (btmp) {
			if (btmp->ifbottom) {
				recordBlockNum = indexRecordBlockAlreadyInsert(tableName, indexName, btmp);
				COUNT = COUNT + recordBlockNum;
				return COUNT;
			}
			else {
				recordBlockNum = indexRecordBlockAlreadyInsert(tableName, indexName, btmp);
				COUNT = COUNT + recordBlockNum;
				btmp = bm.getNextBlock(ftmp, btmp);
			}
		}
		else
			return -1;


	}

	return -1;
}

int RecordManager::indexRecordBlockAlreadyInsert(string tableName, string indexName, blockNode* block) {
	//if block is null, return -1
	if (block) {
		int count, recordSize, type, typeSize, ri;
		char* recordBegin;
		char * contentBegin;
		vector<Attribute> attributeVector;

		count = 0;
		recordBegin = bm.getContent(*block);
		recordSize = api->getRecordSize(tableName);

		api->getAttribute(tableName, &attributeVector);

		while (recordBegin - bm.getContent(*block) < bm.getUsingSize(*block)) {
			contentBegin = recordBegin;
			for (ri = 0; ri < attributeVector.size(); ri++) {
				type = attributeVector[ri].getType();
				typeSize = api->getTypeSize(type);

				//find the index  of the record, and insert it to index tree
				if (attributeVector[ri].getIndex() == indexName) {
					api->insertIndex(indexName, contentBegin, type, block->offsetNum);
					count += 1;
				}

				contentBegin = contentBegin + typeSize;
			}
			recordBegin = recordBegin + recordSize;
		}

		return count;
	}
	else
		return -1;
}

bool RecordManager::recordConditionFit(char* recordBegin, int recordSize, vector<Attribute>* attributeVector, vector<Condition>* conditionVector) {
	if (conditionVector) {
		int type, typeSize, ri, rj;
		string attributeName;
		char content[255];
		char *contentBegin;


		contentBegin = recordBegin;
		for (ri = 0; ri < attributeVector->size(); ri++) {
			type = (*attributeVector)[ri].getType();
			typeSize = api->getTypeSize(type);
			attributeName = (*attributeVector)[ri].getName();
			//init content (when content is string , we can get a string easily)
			memset(content, 0, 255);
			memcpy(content, contentBegin, typeSize);
			for (rj = 0; rj < (*conditionVector).size(); rj++) {
				if ((*conditionVector)[rj].getAttributeName() == attributeName) {
					//if this attribute need to deal about the condition
					if (!contentConditionFit(content, type, &(*conditionVector)[rj])) {
						//if this record is not fit the conditon
						return false;
					}
				}
			}

			contentBegin = contentBegin + typeSize;
		}
		return true;
	}
	else
		return true;

}

void RecordManager::recordPrint(char* recordBegin, int recordSize, vector<Attribute>* attributeVector, vector<string> *attributeNameVector) {
	int type, typeSize, ri, rj;
	string attributeName;
	char content[255];
	char *contentBegin;

	contentBegin = recordBegin;
	for (ri = 0; ri < attributeVector->size(); ri++) {
		type = (*attributeVector)[ri].getType();
		typeSize = api->getTypeSize(type);

		//init content (when content is string , we can get a string easily)
		memset(content, 0, 255);
		memcpy(content, contentBegin, typeSize);

		for (rj = 0; rj < (*attributeNameVector).size(); rj++) {
			if ((*attributeNameVector)[rj] == (*attributeVector)[ri].getName()) {
				contentPrint(content, type);
				break;
			}
		}
		contentBegin = contentBegin + typeSize;
	}
}

void RecordManager::contentPrint(char * content, int type) {
	int rj;
	stringstream ss;
	string stmp;

	if (type == Attribute::TYPE_INT) {
		//if the content is a int
		int tmp = *((int *)content);   //get content value by point
		cout << tmp;
		ss << tmp;
		ss >> stmp;
		for (rj = 0; rj < api->getLength() + 8 - stmp.size(); rj++)
			cout << " ";
		cout << "| ";//the vertical line in front of each value
	}
	else if (type == Attribute::TYPE_FLOAT) {
		//if the content is a float
		float tmp = *((float *)content);   //get content value by point
		cout << tmp;
		ss << tmp;
		ss >> stmp;
		for (rj = 0; rj < api->getLength() + 8 - stmp.size(); rj++)
			cout << " ";
		cout << "| ";
	}
	else {
		//if the content is a string
		string tmp = content;
		cout << tmp;
		for (rj = 0; rj < api->getLength() + 8 - tmp.size(); rj++)
			cout << " ";
		cout << "| ";
	}
}

bool RecordManager::contentConditionFit(char* content, int type, Condition* condition) {
	if (type == Attribute::TYPE_INT) {
		//if the content is a int
		int tmp = *((int *)content);   //get content value by point
		return condition->FitAttribute(tmp);
	}
	else if (type == Attribute::TYPE_FLOAT) {
		//if the content is a float
		float tmp = *((float *)content);   //get content value by point
		return condition->FitAttribute(tmp);
	}
	else {
		//if the content is a string
		return condition->FitAttribute(content);
	}
	//return true;
}

string RecordManager::getIndexFileName(string indexName) {
	string tmp = "";
	return tmp + "INDEX_" + indexName;
}

string RecordManager::getTableFileName(string tableName) {
	string tmp = "";
	return tmp + "TABLE_" + tableName;
}

void RecordManager::setAPI(API* apiInput) {
	api = apiInput;
}

int RecordManager::recordLength(string tableName, vector<string>* attributeNameVector, vector<Condition>* conditionVector, int blockOffset) {
	fileNode *ftmp;
	blockNode* block;
	int length, recordSize, lentmp;
	char* recordBegin;
	char* blockBegin;
	vector<Attribute> attributeVector;
	size_t usingSize;

	ftmp = bm.getFile(getTableFileName(tableName).c_str());
	block = bm.getBlockByOffset(ftmp, blockOffset);

	//if block is null, return -1
	if (block) {
		length = 0;
		recordBegin = bm.getContent(*block);
		recordSize = api->getRecordSize(tableName);
		api->getAttribute(tableName, &attributeVector);
		blockBegin = bm.getContent(*block);
		usingSize = bm.getUsingSize(*block);

		while (recordBegin - blockBegin < usingSize) {
			//if the recordBegin point to a record
			if (recordConditionFit(recordBegin, recordSize, &attributeVector, conditionVector)) {
				lentmp = recordRowLength(recordBegin, recordSize, &attributeVector, attributeNameVector);
				if (lentmp > length)
					length = lentmp;
			}
			recordBegin = recordBegin + recordSize;
		}
		return length;
	}
	else
		return -1;

}

int RecordManager::recordRowLength(char* recordBegin, int recordSize, vector<Attribute>* attributeVector, vector<string> *attributeNameVector) {
	int type, typeSize, length, ri, rj;
	string attributeName;
	char content[255];
	char *contentBegin;
	string stmp;
	stringstream ss;

	length = 0;
	contentBegin = recordBegin;
	for (ri = 0; ri < attributeVector->size(); ri++) {
		type = (*attributeVector)[ri].getType();
		typeSize = api->getTypeSize(type);

		//init content
		memset(content, 0, 255);
		memcpy(content, contentBegin, typeSize);

		for (rj = 0; rj < (*attributeNameVector).size(); rj++) {
			if ((*attributeNameVector)[rj] == (*attributeVector)[ri].getName()) {
				if (type == Attribute::TYPE_INT) {
					//if the content is a int
					int tmp = *((int *)content);   //get content value by point
					ss << tmp;
					ss >> stmp;
					if (stmp.size() > length)
						length = stmp.size();
				}
				else if (type == Attribute::TYPE_FLOAT) {
					//if the content is a float
					float tmp = *((float *)content);   //get content value by point
					ss << tmp;
					ss >> stmp;
					if (stmp.size() > length)
						length = stmp.size();
				}
				else {
					//if the content is a string
					string tmp = content;
					if (tmp.size() > length)
						length = tmp.size();
				}
				break;
			}
		}
		contentBegin = contentBegin + typeSize;
	}
	return length;
}