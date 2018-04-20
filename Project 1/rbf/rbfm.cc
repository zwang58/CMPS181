#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
    free(_rbf_manager);
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager* pfm  = PagedFileManager::instance();
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    const char* data_c = (char*)data;
    uint16_t count = recordDescriptor.size();
    uint16_t in_offset = ceil(recordDescriptor.size()/8.0);
    uint16_t dir_offset = sizeof(uint16_t) + ceil(recordDescriptor.size()/8.0);
    uint16_t data_offset = dir_offset + sizeof(uint16_t) * count;
    uint16_t max_size = data_offset;
    
    for (int i = 0; i < count; ++i) {
        max_size += recordDescriptor[i].length;
    }
    
    char *record = (char*)calloc(max_size, sizeof(char));
    memcpy(record, &count, sizeof(uint16_t));
    memcpy(record + sizeof(uint16_t), &data_c[0], in_offset);
    
    for (int i = 0; i < count; ++i) {
        char target = *(data_c + (char)(i/8));
        if (!(target & (1<<(7-i%8)))) {
            if (recordDescriptor[i].type == TypeVarChar) {
                int attlen;
                memcpy(&attlen, &data_c[in_offset], sizeof(int));     // Read length
                memcpy(record + data_offset, &data_c[in_offset + 4], attlen);     // Write data
                data_offset += attlen;
                in_offset += (4 + attlen);
                memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));     // Write "dir entry"          
            } 
            
            else {
                if (recordDescriptor[i].type == TypeInt) {
                    memcpy(record + data_offset, &data_c[in_offset], sizeof(int));
                    in_offset += sizeof(int);
                    data_offset += sizeof(int); 
                    memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));  
                } 
                
                if (recordDescriptor[i].type == TypeReal) {
                    memcpy(record + data_offset, &data_c[in_offset], sizeof(float));
                    in_offset += sizeof(float);
                    data_offset += sizeof(float); 
                    memcpy(record + dir_offset, &data_offset, sizeof(uint16_t)); 
                }
            }
            
        } 
        
        else {
            memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));
        }
        dir_offset += sizeof(uint16_t);
    } 
     
    char *page = (char*)calloc(PAGE_SIZE, sizeof(char)); 
    uint16_t pageCount = fileHandle.getNumberOfPages(); 
    uint16_t currPage = (pageCount>0) ? pageCount-1 : 0;
    uint16_t freeSpace;
    uint16_t recordCount;  
    
    char first = 1;
    while (currPage < pageCount) {        
        if (fileHandle.readPage(currPage, page) != 0)
            return -1;
        memcpy(&recordCount, &page[PAGE_SIZE - 4], sizeof(uint16_t));
        memcpy(&freeSpace, &page[PAGE_SIZE - 2], sizeof(uint16_t));  

        if (data_offset + 4 <= PAGE_SIZE - (freeSpace + 4 * recordCount + 4)) {
            break;
        }
        if (!first) {
            ++currPage;
        } 
        else {
            currPage = 0;
            first = 0;
        }
    }    
    
    // When we have no enough space on pages
    if (currPage == pageCount) {
        memset(page, 0, PAGE_SIZE); 
        freeSpace = 0;
        recordCount = 0;
    }
    recordCount++;

    memcpy(page + freeSpace, record, data_offset);     // Actual records
   
    int page_dir = PAGE_SIZE - (4 * recordCount + 4);
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &data_offset, sizeof(uint16_t));
    freeSpace += data_offset;  
    memcpy(page + PAGE_SIZE - 4, &recordCount, sizeof(uint16_t));
    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));    

    int append_rc;
    if (currPage == pageCount) {
        append_rc = fileHandle.appendPage(page);
    } 
    else {
        append_rc = fileHandle.writePage(currPage, page);
        append_rc = fileHandle.readPage(currPage, page);
    }
    if (append_rc != 0) return -1;
    
    rid.slotNum = recordCount;
    rid.pageNum = currPage;
    free(record);
    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	
	//set up number of fields and offset where data starts
    uint16_t fieldCount = recordDescriptor.size();
    uint16_t offset = ceil(fieldCount/8.0);
    const char* pointer = (char*)data;
    
	//for each field, check for its attribute type and intepret values accordingly
    for (int i = 0; i < fieldCount; i++) {        
        char null_flag = *(pointer + (char)(i/8));
		
		//check if the null bit is 1, proceed if not null
        if (!(null_flag & (1<<(7-i%8)))) {
			
            switch(recordDescriptor[i].type){
				
				case TypeVarChar:
					int attlen;
					memcpy(&attlen, &pointer[offset], sizeof(int));
					char content[attlen + 1];
					memcpy(content, &pointer[offset + sizeof(int)], attlen );
					content[attlen] = 0;
					cout << recordDescriptor[i].name << ": " << content << "\t";
					offset += (4 + attlen);
					break;
           
				case TypeInt:
					int num;
                    memcpy(&num, &pointer[offset], sizeof(int));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    offset += sizeof(int); 
					break;
				
				case TypeReal:
					float num;
                    memcpy(&num, &pointer[offset], sizeof(float));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    offset += sizeof(float); 
					break;
					
				default: 
					cout <<"Incorrect field type" << endl;
					break;
			}
				
        } else {	
			//if null bit is 1 print NULL
            cout << recordDescriptor[i].name << ": NULL\t";            
        }
    }    
    cout << endl; 
    return 0;
}
}
