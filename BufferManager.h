#define _CRT_SECURE_NO_WARNINGS

#ifndef BUFFERMANAGER_H_
#define BUFFERMANAGER_H_

#include <queue>
#include <string>
#include <iostream>
#include <ctime>

#define MAX_FILE_NUM 100
#define MAX_BLOCK_NUM 500
#define MAX_FILE_NAME 100

using namespace std;

static int replaced_block = -1;

struct blockNode {
	int offsetNum; // the offset number in the block list
	bool pin;  // if the block is locked
	bool ifbottom; // if it is the end of the file node
	char* fileName; // the file the block node belongs to

	char *address; // the content address
	blockNode * preBlock; //the previous block
	blockNode * nextBlock; //the next block
	bool reference; // the LRU replacement flag
	bool dirty; // if the block is dirty
	size_t usingSize; // the size that the block have used. The total size of the block is BLOCK_SIZE.
};

struct fileNode {
	char *fileName;
	bool pin; // if the file is locked
	blockNode *blockHead;
	fileNode * nextFile;
	fileNode * preFile;
};

class BufferManager {
public:
	BufferManager();//the constructor
	~BufferManager();//the destructor

	fileNode* getFile(const char* fileName, bool if_pin = false); //get file from the buffer
	void deleteFileNode(const char * fileName); //delete the file in the buffer
	blockNode* getBlockHead(fileNode* file); //get the block head of the file
	blockNode* getNextBlock(fileNode * file, blockNode* block); //get the next block
	blockNode* getBlockByOffset(fileNode* file, int offestNumber); //get the block by offset
	void setPin(fileNode & file, bool pin);  //set the locked file on file
	void setPin(blockNode & block, bool pin); //set the locked file on block
	void setDirty(blockNode & block); //set the block dirty
	void setUsingSize(blockNode & block, size_t usage); //set the usingSize
	size_t getUsingSize(blockNode & block); //get the usingSize
	char* getContent(blockNode& block); //get the content address
	static int getBlockSize(); //get the size of the block that others can use(others cannot use the block head)

private:
	fileNode *fileHead;
	fileNode file_pool[MAX_FILE_NUM];
	blockNode block_pool[MAX_BLOCK_NUM];
	int totalBlock; // the number of blocks in the list.
	int totalFile; // the number of files in the list.
	void initBlock(blockNode & block); //initialize the block
	void initFile(fileNode & file); //initialize the file
	blockNode* getBlock(fileNode * file, blockNode* position, bool if_pin = false); //get the block with position
	void cleanDirty(blockNode &block); //make dirty block clean by writting back to disk
	size_t getUsingSize(blockNode* block); //get the usingSize by a pointer
	void writtenBackToDisk(const char* fileName, blockNode* block); //write designed blocks to disk
	void writtenBackToDiskAll(); //write all blocks in the memory back to disk

	static const int BLOCK_SIZE = 4096; //the total size of the block
};

extern clock_t start;

#endif