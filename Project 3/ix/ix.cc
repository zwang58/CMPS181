
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
	_pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    if(_pf_manager->createFile(fileName)) return -1;
	
	IXFileHandle ifh;
	if(openFile(fileName, ifh)) return -1;
	
	 nodeHeader header;
    char *page = (char*)calloc(PAGE_SIZE, sizeof(char));
	header.left = NO_PAGE;
    header.right = NO_PAGE;    
    header.leaf = true;
    header.pageNum = ROOT_PAGE;
    header.freeSpace = sizeof( nodeHeader);
            
    memcpy(page, &header, sizeof( nodeHeader));        
    ifh.fh.appendPage(page);
    free(page);
	return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName, ixfileHandle.fh);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return _pf_manager->closeFile(ixfileHandle.fh);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	
    vector<uint16_t> pageStack;
    pageStack.push_back(ROOT_PAGE);     
    
    return insert(ixfileHandle, attribute, key, rid, pageStack);     
}

RC IndexManager::insert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, vector<uint16_t> pageStack) {
    
    uint16_t pageNum = pageStack.back();
    
    char* page = (char*)calloc(PAGE_SIZE, sizeof(char));
    ixfileHandle.fh.readPage(pageNum, page);
    
     nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof( nodeHeader));
    
    if (pageHeader.leaf) {    
        free(page);    
        return insertToLeaf(ixfileHandle, attribute, key, rid, pageStack);        
    } else {              
        uint16_t offset = sizeof( nodeHeader);   
        uint16_t nextPageNum;  
        memcpy(&nextPageNum, page + offset, sizeof(uint16_t));     
         interiorEntry entry;
        // walk through the page until key is not smaller any more 
        // or we reached the last entry 
        // (+sizeof(uint16_t) because offset points at the beinning of the next entry's left pointer)
        while (pageHeader.freeSpace > offset + sizeof(uint16_t)) {
            entry = nextInteriorEntry(page, attribute, offset);
            if (not isKeySmaller(attribute, entry.key, key)) {
                nextPageNum = entry.left;                
                break;
            } else {
                nextPageNum = entry.right;
            }
        }  
        free(page);
        pageStack.push_back(nextPageNum);    
        return insert(ixfileHandle, attribute, key, rid, pageStack);
    }
}

RC IndexManager::insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* const key, RID rid, vector<uint16_t> pageStack) {
    
    uint16_t pageNum = pageStack.back();
    
    // Note 2 * PAGE_SIZE
    char* page = (char*)malloc(2 * PAGE_SIZE);
    ixfileHandle.fh.readPage(pageNum, page);

     nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof( nodeHeader));
    
    // always insert key into page
    uint16_t keySize = getSize(attribute, key);    
    uint16_t offset = sizeof( nodeHeader);   
    uint16_t last = offset;      
    leafEntry entry;

    // walk through the page until key is not smaller any more 
    // or we reached the last entry
    bool past = pageHeader.freeSpace > offset; 
    while (pageHeader.freeSpace > offset) {
        entry = nextLeafEntry(page, attribute, offset);
        if (not isKeySmaller(attribute, entry.key, key)) {
            break;
        }
        last = offset;
    }

    if(past && entry.rid.pageNum == rid.pageNum && entry.rid.slotNum == rid.slotNum) {
        cerr << "ERROR: Found entry with same rid" << endl;
        free(page);
        return -1;
    }

    while(pageHeader.freeSpace > offset) {
        entry = nextLeafEntry(page, attribute, offset);
        if(entry.rid.pageNum == rid.pageNum && entry.rid.slotNum == rid.slotNum) {
           cerr << "ERROR: Found entry with same rid" << endl;
           free(page);
           return -1;
        } 
    }
    
    // memmove the entries that are not smaller than key
    // memcpy in the key and rid
    
    memmove(page + last + keySize + sizeof(RID), page + last, pageHeader.freeSpace - last);
    memcpy(page + last, key, keySize);
    memcpy(page + last + keySize, &rid, sizeof(RID));

    // update pageHeader
    pageHeader.freeSpace += keySize + sizeof(RID);
        
    // normal insert (if it still fits into one page)
    if (pageHeader.freeSpace <= PAGE_SIZE) {  
        memcpy(page, &pageHeader, sizeof( nodeHeader));
        ixfileHandle.fh.writePage(pageNum, page);
        free(page);
        return 0;
    } 

    // try to push one into sibling?
    // lowest priority for now
    
    // split node (if it doesn't fit into one page)    
    char* page2 = (char*)calloc(PAGE_SIZE, sizeof(char));    
    offset = sizeof( nodeHeader);
    // walk through the page until we are about half way through the page
    while (offset - (sizeof( nodeHeader) / 2) < PAGE_SIZE / 2) {
        entry = nextLeafEntry(page, attribute, offset);
    }
    
    // This code makes sure that we only split on keys that are different
    bool found = false;
    uint16_t splitKeySize = getSize(attribute, entry.key);
    char splitKey[splitKeySize];
    memcpy(splitKey, entry.key, splitKeySize);
    while (offset < pageHeader.freeSpace) {
        entry = nextLeafEntry(page, attribute, offset);
        if (memcmp(entry.key, splitKey, splitKeySize) != 0) {
            found = true;
            break;
        }
    }
    if (not found) {
        char newSplitKey[PAGE_SIZE];
        offset = sizeof( nodeHeader);
        while (offset < pageHeader.freeSpace) {
            entry = nextLeafEntry(page, attribute, offset);
            if (memcmp(entry.key, splitKey, splitKeySize) == 0) {
                break;
            }
            memcpy(newSplitKey, entry.key, getSize(attribute, entry.key));
        }
        if (offset - entry.sizeOnPage == sizeof( nodeHeader)) {
            free(page);
            free(page2);  
            return -1;
        } 
        offset -= entry.sizeOnPage;
        memcpy(entry.key, newSplitKey, getSize(attribute, newSplitKey));
    } else {
        offset -= entry.sizeOnPage;
        memcpy(entry.key, splitKey, splitKeySize);
    }
    
    // copy part of the page into the new page
    memcpy(page2 + sizeof( nodeHeader), page + offset, pageHeader.freeSpace - offset); 
    // set up headers
    nodeHeader page2Header;
    page2Header.freeSpace = sizeof( nodeHeader) + pageHeader.freeSpace - offset;
    pageHeader.freeSpace = offset;
    page2Header.leaf = true;
    page2Header.pageNum = ixfileHandle.fh.getNumberOfPages();
    page2Header.left = pageHeader.pageNum;
    page2Header.right = pageHeader.right;
    pageHeader.right = page2Header.pageNum;
    // write new headers
    memcpy(page, &pageHeader, sizeof( nodeHeader));
    memcpy(page2, &page2Header, sizeof( nodeHeader));
    ixfileHandle.fh.writePage(pageNum, page);
    ixfileHandle.fh.appendPage(page2);
    free(page);
    free(page2);
    
    // if root page split it else insert new trafficCup into parent  
    if (pageNum == ROOT_PAGE) {
        return createNewRoot(ixfileHandle, attribute, entry.key, page2Header.pageNum);
    } else {
        pageStack.pop_back();
        return insertToInterior(ixfileHandle, attribute, entry.key, pageNum, page2Header.pageNum, pageStack);
    }
}


RC IndexManager::insertToInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t oldPage, uint16_t newPage, vector<uint16_t> pageStack) {
    
    uint16_t pageNum = pageStack.back();
    
    // Note 2 * PAGE_SIZE
    char* page = (char*)malloc(2 * PAGE_SIZE);
    ixfileHandle.fh.readPage(pageNum, page);
    
     nodeHeader pageHeader;
    memcpy(&pageHeader, page, sizeof( nodeHeader));
    
    uint16_t keySize = getSize(attribute, key);    
      
    // find correct spot by walking down the list
    // then insert (memmove) key, page2Num
    // update header        
    uint16_t offset = sizeof( nodeHeader);    
    interiorEntry entry;
    // walk through the page until key is not smaller any more 
    // or we reached the last entry 
    uint16_t dummy;
    memcpy(&dummy, page + offset, sizeof(uint16_t));
    if (dummy != oldPage) {
        while (pageHeader.freeSpace > offset + sizeof(uint16_t)) {
            entry = nextInteriorEntry(page, attribute, offset);
            if (entry.right == oldPage)
                break;
        }
    }
    // now offset points at the "oldPage" pointer
    // and we want to insert the key, and a pointer to "newPage" right behind this pointer
    offset += sizeof(uint16_t);
    memmove(page + offset + keySize + sizeof(uint16_t), page + offset, pageHeader.freeSpace - offset);
    memcpy(page + offset, key, keySize);
    memcpy(page + offset + keySize, &newPage, sizeof(uint16_t));
    pageHeader.freeSpace += keySize + sizeof(uint16_t);   
    memcpy(page, &pageHeader, sizeof( nodeHeader));
    
    // normal insert
    if (pageHeader.freeSpace + keySize + sizeof(uint16_t) <= PAGE_SIZE) {  
        ixfileHandle.fh.writePage(pageNum, page);
        free(page);
        return 0;
    }    
    
    // split node (if it doesn't fit into one page)    
    char* page2 = (char*)calloc(PAGE_SIZE, sizeof(char));    
    offset = sizeof( nodeHeader);
    // walk through the page until we are about half way through the page
    while (offset - (sizeof( nodeHeader) / 2) < PAGE_SIZE / 2) {
        entry = nextInteriorEntry(page, attribute, offset);
    }
    // copy part of the page into the new page
    // begin with pointer after entry.key
    memcpy(page2 + sizeof( nodeHeader), page + offset, pageHeader.freeSpace - offset); 
    // set up headers
     nodeHeader page2Header;
    page2Header.freeSpace = sizeof( nodeHeader) + pageHeader.freeSpace - offset;
    // page ends now with the pointer that was right before enty.key
    pageHeader.freeSpace = offset - getSize(attribute, entry.key);
    page2Header.leaf = false;
    page2Header.pageNum = ixfileHandle.fh.getNumberOfPages();
    page2Header.left = pageHeader.pageNum;
    page2Header.right = pageHeader.right;
    pageHeader.right = page2Header.pageNum;
    // write new headers
    memcpy(page, &pageHeader, sizeof( nodeHeader));
    memcpy(page2, &page2Header, sizeof( nodeHeader));
    ixfileHandle.fh.writePage(pageNum, page);
    ixfileHandle.fh.appendPage(page2);    
    free(page);
    free(page2);
        
    // if root page split it else insert new trafficCop into parent  
    if (pageNum == ROOT_PAGE) {
        return createNewRoot(ixfileHandle, attribute, entry.key, page2Header.pageNum);
    } else {
        pageStack.pop_back();
        return insertToInterior(ixfileHandle, attribute, entry.key, pageNum, page2Header.pageNum, pageStack);
    }        
}

RC IndexManager::createNewRoot(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t page2Num) {
    
    char* newRoot = (char*)calloc(PAGE_SIZE, sizeof(char));
    char* oldRoot = (char*)calloc(PAGE_SIZE, sizeof(char));
    char* page2 = (char*)calloc(PAGE_SIZE, sizeof(char));
    
    ixfileHandle.fh.readPage(ROOT_PAGE, oldRoot);
    ixfileHandle.fh.readPage(page2Num, page2);
    
     nodeHeader oldRootHeader;
    memcpy(&oldRootHeader, oldRoot, sizeof( nodeHeader));
     nodeHeader page2Header;
    memcpy(&page2Header, page2, sizeof( nodeHeader));
    
    oldRootHeader.pageNum = ixfileHandle.fh.getNumberOfPages();
    page2Header.left = oldRootHeader.pageNum;
    
     nodeHeader newRootHeader;
    
    newRootHeader.leaf = false;
    newRootHeader.pageNum = ROOT_PAGE;
    newRootHeader.left = NO_PAGE;
    newRootHeader.right = NO_PAGE;
    
    uint16_t keySize = getSize(attribute, key);
    uint16_t offset = sizeof( nodeHeader);
    memcpy(newRoot + offset, &oldRootHeader.pageNum, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    memcpy(newRoot + offset, key, keySize);
    offset += keySize;
    memcpy(newRoot + offset, &page2Num, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    
    newRootHeader.freeSpace = offset;
    
    memcpy(newRoot, &newRootHeader, sizeof( nodeHeader));
    memcpy(oldRoot, &oldRootHeader, sizeof( nodeHeader));
    memcpy(page2,   &page2Header,   sizeof( nodeHeader));
    
    ixfileHandle.fh.writePage(ROOT_PAGE, newRoot);
    ixfileHandle.fh.writePage(page2Num, page2);
    ixfileHandle.fh.appendPage(oldRoot);
    free(newRoot);
    free(oldRoot);
    free(page2);
    
    return SUCCESS;
}

interiorEntry IndexManager::nextInteriorEntry(char* page, Attribute attribute, uint16_t &offset) const {
    
    interiorEntry entry;
    entry.attribute = attribute;
    memcpy(&entry.left, page + offset, sizeof(uint16_t));    
    uint16_t size = getSize(attribute, page + offset + sizeof(uint16_t));    
    memcpy(&entry.key, page + offset + sizeof(uint16_t), size);
    memcpy(&entry.right, page + offset + sizeof(uint16_t) + size, sizeof(uint16_t));
    offset += sizeof(uint16_t) + size;
    return entry;
}

 leafEntry IndexManager::nextLeafEntry(char* page, Attribute attribute, uint16_t &offset) const {
    
    leafEntry entry;  
    entry.attribute = attribute;
    uint16_t size = getSize(attribute, page + offset);    
    memcpy(&entry.key, page + offset, size);
    memcpy(&entry.rid, page + offset + size, sizeof(RID));    
    entry.sizeOnPage = size + sizeof(RID);
    offset += entry.sizeOnPage;
    return entry;

}

uint16_t IndexManager::getSize(const Attribute &attribute, const void* key) const {
    
    switch (attribute.type) {
        case TypeVarChar:
            int size;
            memcpy(&size, key, sizeof(int));
            return 4 + size;
        case TypeInt:
            return 4;
        case TypeReal:
            return 4;
        default:
            return -1;
    }
    
}

RC IndexManager::isKeySmaller(const Attribute &attribute, const void* pageEntryKey, const void* key) {   

    if(key == NULL) {
        return 0;
    }

    switch (attribute.type) {
        case TypeVarChar:
        {
            int size;
            memcpy(&size, pageEntryKey, sizeof(int));
            char c_pageEntryKey[size + 1];
            memcpy(c_pageEntryKey, (char*)pageEntryKey + 4, size);
            c_pageEntryKey[size] = 0;
            memcpy(&size, key, sizeof(int));
            char c_key[size + 1];
            memcpy(c_key, (char*)key + 4, size);
            c_key[size] = 0;      
            return strcmp(c_key, c_pageEntryKey) > 0;
        }
        case TypeInt:
        {
            int pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(int));
            memcpy(&searchKey, key, sizeof(int));
            return (pageKey < searchKey);
        }
        case TypeReal:
        {
            float pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(float));
            memcpy(&searchKey, key, sizeof(float));
            return (pageKey < searchKey);
        }
        default:
            return -1;
    }
    
}


RC IndexManager::isKeyEqual(const Attribute &attribute, const void* pageEntryKey, const void* key) {   

    switch (attribute.type) {
        case TypeVarChar:
        {
            int size;
            memcpy(&size, pageEntryKey, sizeof(int));
            char c_pageEntryKey[size + 1];
            memcpy(c_pageEntryKey, (char*)pageEntryKey + 4, size);
            c_pageEntryKey[size] = 0;
            memcpy(&size, key, sizeof(int));
            char c_key[size + 1];
            memcpy(c_key, (char*)key + 4, size);
            c_key[size] = 0;           
            return strcmp(c_key, c_pageEntryKey) == 0;
        }
        case TypeInt:
        {
            int pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(int));
            memcpy(&searchKey, key, sizeof(int));
            return (pageKey == searchKey);
        }
        case TypeReal:
        {
            float pageKey, searchKey;
            memcpy(&pageKey, pageEntryKey, sizeof(float));
            memcpy(&searchKey, key, sizeof(float));
            return (pageKey == searchKey);
        }
        default:
            return -1;
    }
    
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    char* page = (char*)calloc(PAGE_SIZE, sizeof(char));
    uint16_t curPage = ROOT_PAGE;
    nodeHeader pageHeader;
    uint16_t offset;

    // find correct page
    // note a certain key can only be on one page, never on multiple
    while(true) {
            
        ixfileHandle.fh.readPage(curPage, page);
        memcpy(&pageHeader, page, sizeof( nodeHeader));
        offset = sizeof( nodeHeader);    
 
        if(pageHeader.leaf)
            break;

         interiorEntry entry;
        while (pageHeader.freeSpace > offset + sizeof(uint16_t)) {
            entry = nextInteriorEntry(page, attribute, offset);
            if (not isKeySmaller(attribute, entry.key, key)) {
                curPage = entry.left;
                break;
            } else {
                curPage = entry.right;
            }
        }
    }

    leafEntry entry;
    bool found = false;
    if (offset == pageHeader.freeSpace) {
        free(page);
        return -1;
    }

    //look through the entries until the matching key is found or reaching freespace
    while (pageHeader.freeSpace > offset) {
        entry = nextLeafEntry(page, attribute, offset);      
        if (isKeyEqual(attribute, entry.key, key)) {
            found = true;
            if (memcmp(&entry.rid, &rid, sizeof(RID)) == 0)
                break;
        } else if (found) {
            free(page);
            return -1;
        }
    }

	//move the data between offset and freespace forward by sizeOnPage bytes
    memmove(page + offset - entry.sizeOnPage, page + offset, pageHeader.freeSpace - offset);
    pageHeader.freeSpace -= entry.sizeOnPage;
    memcpy(page, &pageHeader, sizeof(nodeHeader));

    ixfileHandle.fh.writePage(curPage, page);
    free(page); 
    return SUCCESS;
}

RC IndexManager::keyCompare(const Attribute &attr, const void* pageKey, const void* lowKey, const void* highKey, bool lowInc, bool highInc) {   
    
    if(lowKey != NULL) {
    
        if(isKeySmaller(attr, pageKey, lowKey))
            return -1;

        if(!lowInc && isKeyEqual(attr, pageKey, lowKey)) 
            return -1;
    }
    
    if(highKey != NULL) {

        if(isKeySmaller(attr, highKey, pageKey))
            return 1;

        if(!highInc && isKeyEqual(attr, highKey, pageKey)) 
            return 1;
    }

    return 0;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{

	if (ixfileHandle.fh.getfd() == NULL || ixfileHandle.fh.getNumberOfPages() == 0)
        return -1;

	ix_ScanIterator.ixfileHandle = &ixfileHandle;
	ix_ScanIterator.attribute = attribute;
    ix_ScanIterator.lowKey = lowKey; 
    ix_ScanIterator.highKey = highKey; 
    
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;

    char* page = (char*)calloc(PAGE_SIZE, sizeof(char));
    uint16_t curPage = ROOT_PAGE;
    struct nodeHeader pageHeader;

    while(true) {
            
        ixfileHandle.fh.readPage(curPage, page);
        memcpy(&pageHeader, page, sizeof(struct nodeHeader));
        uint16_t offset = sizeof(struct nodeHeader);    

        struct interiorEntry entry;
 
        if(pageHeader.leaf) {
            break;
        }

        while (pageHeader.freeSpace > offset + sizeof(uint16_t)) {
            entry = nextInteriorEntry(page, attribute, offset);
            if (not isKeySmaller(attribute, entry.key, lowKey)) {
                curPage = entry.left;
                break;
            } else {
                curPage = entry.right;
            }
        }
    }

    ix_ScanIterator.pageOffset = sizeof(struct nodeHeader);
    ix_ScanIterator.page = page;
    
    return SUCCESS;
}


void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
    printBtree_rec(ixfileHandle, ROOT_PAGE, attribute);
    cout << "\n"; 
}

void IndexManager::printBtree_rec(IXFileHandle &ixfileHandle, const Attribute &attribute, uint16_t pageNum, uint16_t depth) const {
    void* pageData = malloc(PAGE_SIZE);
    ixfilefileHandle.fh.readPage(pageNum, pageData);

    struct nodeHeader node_header;
    memcpy(&type, pageData, sizeof(struct nodeHeader));
    
    if(node_header.leaf) {
        cout << string(depth*4, ' ') << "{\"keys\" [";
        uint16_t offset = sizeof(struct nodeHeader);
        struct leafEntry leaf_entry;
        for(;;) {
            leaf_entry = nextLeafEntry(page, attribute, offset);
            printLeafNode(leaf_entry);
            if(node_header.freeSpace <= offset) break;
            cout << ", ";
        }
    }
    else { // internalNode
        cout << string(depth*4, ' ') << "{\"keys\": ]";
        uint16_t offset = sizeof(struct(nodeHeader);
        struct interiorEntry interior_entry;
        for(;;) {
            interior_entry = nextInteriorEntry(pageData, attribute, offset);
            cout << "\"";
            printKey(interior_entry.attribute, interior_entry.key);
            cout << "\"";
            if(offset + sizeof(uint16_t) >= node_header.freeSpace) break;
            cout << ", ";
        }
        
        cout << "],\n"
             << string(depth*4, ' ') << " \"children\" [\n";
        offset = sizeof(struct nodeHeader);
        
        memcpy(&interior_entry.right, pageData + offset, sizeof(uint16_t));
        
        while(sizeof(uint16_t) + offset < node_header.freeSpace) {
            interior_entry = nextInteriorEntry(pageData, attribute, offset);
            printBtree_rec(ixfileHandle, attribute, interior_entry.left, ++depth);
            cout << ",\n";
        }
        printBtree_rec(ixfileHandle, attribute, interior_entry.right, ++depth);
        cout << "\n" << string(depth*4, ' ') << "]}";
    }
    
    free(pageData);
} 

void IndexManager::printLeafNode(struct leafEntry leaf_entry) const {
    cout << '\"';
    printKey(leaf_entry.attribute, leaf_entry.key);
    cout << ":[(" << leaf_entry.rid.pageNum << "," << entry.rid.slotNum << ")]";
    cout << '\"';

}

void IndexManager::printKey(const Attribute &attribute, const void* key) const {
    if (attribute.type == TypeInt) {
        int num;
        memcpy(&num, key, sizeof(int));
        cout << num;
    }
    else if (attribute.type == TypeReal) {
        float num;
        memcpy(&num, key, sizeof(float));
        cout << num;
    }
    else {
        int len;
        memcpy(&len, key, sizeof(int));
        char string[len + 1];
        memcpy(string, (char*)key + 4, len);
        string[len] = 0;
        cout << string;
    }
}

IX_ScanIterator::IX_ScanIterator()
{
	_index_manager = IndexManager::instance();
}

IX_ScanIterator::~IX_ScanIterator()
{
	_index_manager = NULL;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    struct nodeHeader pageHeader;
    struct leafEntry entry;

    while(true) {
            
        memcpy(&pageHeader, page, sizeof(struct nodeHeader));
        while (pageHeader.freeSpace > pageOffset) {

            entry = _index_manager->nextLeafEntry((char*) page, attribute, pageOffset);
            int result = _index_manager->keyCompare(attribute, entry.key, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
            if (result == 0) {
                memcpy(&rid, &entry.rid, sizeof(RID));
                memcpy(key, entry.key, _index_manager->getSize(attribute, entry.key));
                return SUCCESS;
            } else if(result == 1) {
                break;
            }
        }

        if(pageHeader.right == NO_PAGE) {
            break;
        }

        ixfileHandle->fh.readPage(pageHeader.right, page);
        pageOffset = sizeof(struct nodeHeader);
    }

    return IX_EOF;
}

RC IX_ScanIterator::close()
{
    free(page);
    return SUCCESS;
}


IXFileHandle::IXFileHandle()
{
	ixReadPageCounter = 0;
	ixWritePageCounter = 0;
	ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	if(fh.collectCounterValues(readPageCount, writePageCount, appendPageCount)) return -1;
	ixReadPageCounter = readPageCount;
	ixWritePageCounter = writePageCount;
	ixAppendPageCounter = appendPageCount;
	return 0;
}

