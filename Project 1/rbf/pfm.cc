#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
    FILE* file = fopen(fileName.c_str(), "w");
    if(file == NULL) return -1;

    fclose(file);
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if(remove(fileName.c_str()) != 0) return -1;
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if(fileHandle.currentFile == NULL){
    
		struct stat fileInfo;
		if(stat(fileName.c_str(), &fileInfo) != 0) return -1;
		
		fileHandle.currentFile = fopen(fileName.c_str(), "rb+");
		if(fileHandle.currentFile == NULL) return -1;
		
		fileHandle.fileSize = fileInfo.st_size;
		return 0;
		
	}else return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(fileHandle.currentFile != NULL){
		fclose(fileHandle.currentFile);
		fileHandle.currentFile = NULL;
		return 0;
	}else return -1;
}


FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
	currentFile = NULL;
	fileSize = 0;
}


FileHandle::~FileHandle()
{
	if(currentFile == NULL) return;
    
    fflush(currentFile);
    fclose(currentFile);
    currentFile = NULL;
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    unsigned long totalPages = fileSize/PAGE_SIZE;
    if(pageNum >= totalPages) return -1;

    if(fseek(currentFile, pageNum * PAGE_SIZE, SEEK_SET) != 0) return -1;
    if(fread(data, sizeof(char), PAGE_SIZE, currentFile) != PAGE_SIZE) return -1;
    readPageCounter += 1;

    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    unsigned long totalPages = fileSize/PAGE_SIZE;
    if(pageNum >= totalPages) return -1;
    
    if(fseek(currentFile, pageNum * PAGE_SIZE, SEEK_SET) != 0) return -1;
    if(fwrite(data, sizeof(char), PAGE_SIZE, currentFile) != PAGE_SIZE) return -1;
    
    writePageCounter += 1;
    
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if(fseek(currentFile, 0, SEEK_END) != 0) return -1;
    if(fwrite(data, sizeof(char), PAGE_SIZE, currentFile) != PAGE_SIZE) return -1;

    appendPageCounter += 1;
    
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    
    return fileSize/PAGE_SIZE;;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    
    return 0;
}
