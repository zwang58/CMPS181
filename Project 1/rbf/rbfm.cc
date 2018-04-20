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
    const char* pointer = (char*)data;
    uint16_t fieldCount = recordDescriptor.size();
    uint16_t in_offset = ceil(recordDescriptor.size()/8.0);
    uint16_t dir_offset = sizeof(uint16_t) + in_offset;
    uint16_t data_offset = dir_offset + sizeof(uint16_t) * fieldCount;
    uint16_t max_size = data_offset;
    
    for (int i = 0; i < fieldCount; ++i) {
        max_size += recordDescriptor[i].length;
    }
    
    char *record = (char*)calloc(max_size, sizeof(char));
    memcpy(record, &fieldCount, sizeof(uint16_t));
    memcpy(record + sizeof(uint16_t), &pointer[0], in_offset);
    
    for (int i = 0; i < fieldCount; ++i) {
        char null_flag = *(pointer + (char)(i/8));
		//if null bit is off
        if (!(null_flag & (1<<(7-i%8)))) {
			
			switch(recordDescriptor[i].type){
				case TypeVarChar:{
					int valueLength;
					memcpy(&valueLength, &pointer[in_offset], sizeof(int));     // Read length
					memcpy(record + data_offset, &pointer[in_offset + 4], valueLength);     // Write data
					data_offset += valueLength;
					in_offset += (4 + valueLength);
					memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));     // Write "dir entry"
					break;
				}
					
				case TypeInt:{
					memcpy(record + data_offset, &pointer[in_offset], sizeof(int));
                    in_offset += sizeof(int);
                    data_offset += sizeof(int); 
                    memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));
					break;
				}
					
				case TypeReal:{
					memcpy(record + data_offset, &pointer[in_offset], sizeof(float));
                    in_offset += sizeof(float);
                    data_offset += sizeof(float); 
                    memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));
					break;
				}
					
				default: break;
			}            
        } 
        
        else {
            memcpy(record + dir_offset, &data_offset, sizeof(uint16_t));
        }
        dir_offset += sizeof(uint16_t);
    } 
     
    char *page = (char*)calloc(PAGE_SIZE, sizeof(char)); 
    uint16_t pagefieldCount = fileHandle.getNumberOfPages(); 
    uint16_t currPage = (pagefieldCount>0) ? pagefieldCount-1 : 0;
    uint16_t freeSpace;
    uint16_t recordfieldCount;  
    
    char first = 1;
    while (currPage < pagefieldCount) {        
        if (fileHandle.readPage(currPage, page) != 0)
            return -1;
        memcpy(&recordfieldCount, &page[PAGE_SIZE - 4], sizeof(uint16_t));
        memcpy(&freeSpace, &page[PAGE_SIZE - 2], sizeof(uint16_t));  

        if (data_offset + 4 <= PAGE_SIZE - (freeSpace + 4 * recordfieldCount + 4)) {
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
    if (currPage == pagefieldCount) {
        memset(page, 0, PAGE_SIZE); 
        freeSpace = 0;
        recordfieldCount = 0;
    }
    recordfieldCount++;

    memcpy(page + freeSpace, record, data_offset);     // Actual records
   
    int page_dir = PAGE_SIZE - (4 * recordfieldCount + 4);
    memcpy(page + page_dir    , &freeSpace, sizeof(uint16_t));
    memcpy(page + page_dir + 2, &data_offset, sizeof(uint16_t));
    freeSpace += data_offset;  
    memcpy(page + PAGE_SIZE - 4, &recordfieldCount, sizeof(uint16_t));
    memcpy(page + PAGE_SIZE - 2, &freeSpace, sizeof(uint16_t));    

    int append_rc;
    if (currPage == pagefieldCount) {
        append_rc = fileHandle.appendPage(page);
    } 
    else {
        append_rc = fileHandle.writePage(currPage, page);
        append_rc = fileHandle.readPage(currPage, page);
    }
    if (append_rc != 0) return -1;
    
    rid.slotNum = recordfieldCount;
    rid.pageNum = currPage;
    free(record);
    free(page);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    char *page = (char*) calloc(PAGE_SIZE, sizeof(char));
    if(fileHandle.readPage(rid.pageNum, page) != 0) return -1;

    uint16_t recordfieldCount;
    uint16_t record;
    uint16_t fieldfieldCount;
    uint16_t null_flag;
    uint16_t dir;
    uint16_t curr_offset;
    uint16_t prev_offset;

    memcpy(&recordfieldCount, &page[PAGE_SIZE - 4], sizeof(uint16_t));
    if(recordfieldCount < rid.slotNum) return -1;

    memcpy(&record, &page[PAGE_SIZE - 4 - (4 * rid.slotNum)], sizeof(uint16_t));

    memcpy(&fieldfieldCount, &page[record], sizeof(uint16_t));
    if(fieldfieldCount != recordDescriptor.size()) return -1;

    null_flag = ceil(fieldfieldCount / 8.0);
    memcpy(data, &page[record + sizeof(uint16_t)], null_flag);

    dir = record + sizeof(uint16_t) + null_flag;

    uint16_t i;
    char target;
    int attribute_len;
    char *pointeropy = (char *)data + null_flag;
    curr_offset = sizeof(uint16_t) + null_flag + fieldfieldCount * sizeof(uint16_t);
    for (i = 0; i < fieldfieldCount; i++) {
        
        target = *((char *)data + (char)(i/8));
        prev_offset = curr_offset;
        
        if(!(target & (1<<(7 - i % 8)))) {
            memcpy(&curr_offset, &page[dir + i * sizeof(uint16_t)], sizeof(uint16_t));
            attribute_len = curr_offset - prev_offset;
            
            switch(recordDescriptor[i].type) {
                
                case TypeVarChar:{
                    memcpy(&pointeropy[0], &attribute_len, sizeof(int));
                    memcpy(&pointeropy[4], &page[record + prev_offset], attribute_len);
                    pointeropy += (4 + attribute_len);
                    break;
                }

                case TypeReal:
                case TypeInt:{
                    memcpy(&pointeropy[0], &page[record + prev_offset], sizeof(int));
                    pointeropy += sizeof(int);
                    break;
                }
                default:
                    break;
            }
        }
    }
    free(page);

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	
	//set up number of fields and offset where data starts
    uint16_t fieldfieldCount = recordDescriptor.size();
    uint16_t offset = ceil(fieldfieldCount/8.0);
    const char* pointer = (char*)data;
    
	//for each field, check for its attribute type and intepret values accordingly
    for (int i = 0; i < fieldfieldCount; i++) {        
        char null_flag = *(pointer + (char)(i/8));
		
		//check if the null bit is 1, proceed if not null
        if (!(null_flag & (1<<(7-i%8)))) {
            switch(recordDescriptor[i].type){
				
				case TypeVarChar:{
					int valueLength;
					memcpy(&valueLength, &pointer[offset], sizeof(int));
					char value[valueLength + 1];
					memcpy(value, &pointer[offset + sizeof(int)], valueLength );
					value[valueLength] = 0;
					cout << recordDescriptor[i].name << ": " << value << "\t";
					offset += (4 + valueLength);
					break;
				}
           
				case TypeInt:{
					int num;
                    memcpy(&num, &pointer[offset], sizeof(int));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    offset += sizeof(int); 
					break;
				}
				
				case TypeReal:{
					float num;
                    memcpy(&num, &pointer[offset], sizeof(float));
                    cout << recordDescriptor[i].name << ": " << num << "\t";
                    offset += sizeof(float); 
					break;
				}
					
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

