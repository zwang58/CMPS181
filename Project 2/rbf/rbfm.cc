#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

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
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
	
	SlotDirectoryRecordEntry newRecordEntry;
	//try to use the first slot entry that was deleted before
	bool recycleSlot = false;
	for(int i = 0; i < slotHeader.recordEntriesNumber; i++){
		
		newRecordEntry = getSlotDirectoryRecordEntry(pageData, i);
		if(newRecordEntry.length == 0){
			rid.slotNum = i;
			recycleSlot = true;
		}
	}
	
	//if no deleted slot to be recycled, proceed to append new entry
	if(!recycleSlot){
		rid.slotNum = slotHeader.recordEntriesNumber;
		slotHeader.recordEntriesNumber += 1;
	}

	// Adding the new record reference in the slot directory.
	newRecordEntry.length = recordSize;
	newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
	setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

	//if it's a forwarding address
	if(recordEntry.offset < 0){
		RID realRid;
		realRid.pageNum = recordEntry.length;
		realRid.slotNum = recordEntry.offset * (-1);
		readRecord(fileHandle, recordDescriptor, realRid, data);
	}
	
    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);
 
    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    	
	//Retrieves target page
	void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
	
	//if an entry's offset is negative, this entry actually contains the forwarding
	//address in the form of (pageNum, slotNum) instead of (length, offset).
	//flip the sign of "offset" to get the real slotNum.
	if (recordEntry.offset < 0){
		
		RID realRid;
		realRid.pageNum = recordEntry.length;
		realRid.slotNum = recordEntry.offset * (-1);
		
		return deleteRecord(fileHandle, recordDescriptor, realRid);
	}
	
	//if this record is next to the freespace, just remove the record; otherwise push up the records behind it
	if(slotHeader.freeSpaceOffset < recordEntry.offset){
		
		//Push up the block of records and update the record entry offsets
		int moveSize = recordEntry.offset - slotHeader.freeSpaceOffset;
		int newOffset = slotHeader.freeSpaceOffset + recordEntry.length;
		memmove((char*)pageData + newOffset, (char*)pageData + slotHeader.freeSpaceOffset, moveSize);
		
		//increase the entry offset of every pushed-up record
		for(int i = 0; i< slotHeader.recordEntriesNumber; i++){
			
			SlotDirectoryRecordEntry newRecordEntry = getSlotDirectoryRecordEntry(pageData, i);
			if(newRecordEntry.offset < recordEntry.offset){
				newRecordEntry.offset += recordEntry.length;
				setSlotDirectoryRecordEntry(pageData, i, newRecordEntry);
			}
		}
	}
	
	//if this record entry is the last one, delete it; otherwise mark it as useable by setting its length to 0;
	if(rid.slotNum + 1 == slotHeader.recordEntriesNumber){
		
		memset((char*)pageData + sizeof(SlotDirectoryHeader) + rid.slotNum * sizeof(SlotDirectoryRecordEntry), 0, sizeof(SlotDirectoryRecordEntry));
		slotHeader.recordEntriesNumber --;
		
	}else{
		
		recordEntry.length = 0;
		setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry); 
		
	}
	
	//update and set the new header
	slotHeader.freeSpaceOffset += recordEntry.length;
	setSlotDirectoryHeader(pageData, slotHeader);
	
	//overwrites the old page
	if (fileHandle.writePage(rid.pageNum, pageData))
        return RBFM_WRITE_FAILED;
	
	free(pageData);
	return SUCCESS;    
}

int RecordBasedFileManager::reinsertRecord(void* page, const RID &rid, const vector<Attribute> &recordDescriptor, const void *data){
	
	//try to insert the record back to the given RID
	uint32_t recordSize = getRecordSize(recordDescriptor, data);
	SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
	SlotDirectoryRecordEntry recordEntry;
	
	//Ensure that slot entry is marked as usable. 
	//Report error if slot is occupied or number of entries is smaller than slotNum
	if(slotHeader.recordEntriesNumber > rid.slotNum){
		
		recordEntry = getSlotDirectoryRecordEntry(page, rid.slotNum);
		if(recordEntry.length > 0) return 1;
	
	}else if (slotHeader.recordEntriesNumber < rid.slotNum){
		return 1;
		
	//if recordEntriesNumber is equal to slotNum, this slot entry has just been deleted cuz it was the last one
	//so increment recordEntriesNumber
	}else
		slotHeader.recordEntriesNumber ++;
		
	
	recordEntry.length = recordSize;
	recordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
	setRecordAtOffset(page, recordEntry.offset, recordDescriptor, data);
	
	slotHeader.freeSpaceOffset -= recordSize;
	setSlotDirectoryRecordEntry(page, rid.slotNum, recordEntry);
	setSlotDirectoryHeader(page, slotHeader);
	
	return 0;
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

	//Retrieves target page
	void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
	
	//if an entry's offset is negative, this entry actually contains the forwarding
	//address in the form of (pageNum, slotNum) instead of (length, offset).
	//flip the sign of "offset" to get the real slotNum.
	if (recordEntry.offset < 0){
		
		RID realRid;
		realRid.pageNum = recordEntry.length;
		realRid.slotNum = recordEntry.offset * (-1);
		
		return updateRecord(fileHandle, recordDescriptor, data, realRid);
	}
	
	//Check if the record is expanding or shrinking in size
	int newRecordSize = getRecordSize(recordDescriptor, data);
	int oldRecordSize = recordEntry.length;
	
	//if the size stays the same just overwrite the record
	if(newRecordSize == oldRecordSize){
		
		setRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

	//if the size changes but still fits on that page, delete then reinsert with old rid;
	}else if((int)getPageFreeSpaceSize(pageData) >= newRecordSize - oldRecordSize){
		
		deleteRecord(fileHandle, recordDescriptor, rid);
		if(fileHandle.readPage(rid.pageNum, pageData))
			return RBFM_READ_FAILED;
		if(reinsertRecord(pageData, rid, recordDescriptor, data))
			return RBFM_WRITE_FAILED;

	//if the record wouldn't fit here, delete it and insert on another page with enough space
	}else{
		
		//delete() will remove the slot entry if it's the last entry
		//In that case restore the recordEntriesNumber since the slot entry will be added back later
		deleteRecord(fileHandle, recordDescriptor, rid);
		if(fileHandle.readPage(rid.pageNum, pageData))
			return RBFM_READ_FAILED;
		slotHeader = getSlotDirectoryHeader(pageData);
		if(slotHeader.recordEntriesNumber == rid.slotNum){
			slotHeader.recordEntriesNumber++;
			setSlotDirectoryHeader(pageData, slotHeader);
		}
		
		RID newRid;
		insertRecord(fileHandle, recordDescriptor, data, newRid);
		
		//store the returned RID inside slot entry as forwarding address
		recordEntry.length = newRid.pageNum;
		recordEntry.offset = newRid.slotNum * (-1);
		setSlotDirectoryRecordEntry(pageData, rid.slotNum, recordEntry);
		
	}
	
	//overwrites the old page
	if (fileHandle.writePage(rid.pageNum, pageData))
        return RBFM_WRITE_FAILED;
	return SUCCESS;   
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
	
	//find the field index that matches the attributeName
	bool found = false;
	bool isVarChar = false;
	unsigned attrIndex;
    for (attrIndex = 0; attrIndex < recordDescriptor.size(); attrIndex++)
    {
        if (attributeName == recordDescriptor[attrIndex].name){
			found = true;
			if(recordDescriptor[attrIndex].type == TypeVarChar) 
				isVarChar = true;
			break;
		}
	}
	
	if(!found) return 1;
	
	void* page = malloc(PAGE_SIZE);
	if (fileHandle.readPage(rid.pageNum, page))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(page, rid.slotNum);

	//if it's a forwarding address
	if(recordEntry.offset < 0){
		RID realRid;
		realRid.pageNum = recordEntry.length;
		realRid.slotNum = recordEntry.offset * (-1);
		return readAttribute(fileHandle, recordDescriptor, realRid, attributeName, data);
	}
	
	//set pointer to start of the record block
	char* recordStart = (char*)page + recordEntry.offset;
	RecordLength fieldCount;
	memcpy(&fieldCount, recordStart, sizeof(RecordLength));
	
	if(fieldCount <= attrIndex) return 1; 
	
	char* pointer = recordStart + sizeof(RecordLength);
	
	//get nullIndicator and return if requested field is NULL
	int nullIndicatorSize = getNullIndicatorSize(fieldCount);
	char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, pointer, nullIndicatorSize);
	
	if(fieldIsNull(nullIndicator, attrIndex)) return RBFM_FIELD_NULL;
	
	//pointer moves to start of columnOffsets
	pointer += nullIndicatorSize;
	
	//Initiate the Boundaries of the field, as offsets relative to recordStart
	ColumnOffset endPointer;
	ColumnOffset startPointer = sizeof(RecordLength) + nullIndicatorSize + fieldCount * sizeof(ColumnOffset);
	
    memcpy(&endPointer, pointer + attrIndex * sizeof(ColumnOffset), sizeof(ColumnOffset));
	
	//If target field is not the first one, set its start bound to the end bound of the last field
	if(attrIndex > 0)
		memcpy(&startPointer, pointer + (attrIndex - 1)* sizeof(ColumnOffset), sizeof(ColumnOffset));
	
	uint32_t fieldSize = endPointer - startPointer;
	
	//if varchar copy the field length first
	if(isVarChar){
		memcpy(data, &fieldSize, sizeof(uint32_t));
		memcpy((char*)data + sizeof(uint32_t), recordStart + startPointer, fieldSize);
	}else{
		
		//copy field data into data
		memcpy(data, recordStart + startPointer, fieldSize);
	}

    free(page);
	return SUCCESS;
};

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator){
	
	//Populate the private fields of rbfm_ScanIterator
    rbfm_ScanIterator.currFile = &fileHandle;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;
	rbfm_ScanIterator.conditionAttribute = conditionAttribute;
    rbfm_ScanIterator.compOp = compOp;
    rbfm_ScanIterator.value = (void*)value;
    rbfm_ScanIterator.page = (char*)malloc(PAGE_SIZE);
    rbfm_ScanIterator.outputAttributeCount = attributeNames.size();
	
	//record the index and type of the condition attribute
    for (uint16_t i = 0; i < recordDescriptor.size(); i++) {
        if (conditionAttribute == recordDescriptor[i].name) {
            rbfm_ScanIterator.conditionAttributeIndex = i;
			rbfm_ScanIterator.conditionAttrType = recordDescriptor[i].type;
			break;
        }
    }
	
	rbfm_ScanIterator.outputAttributeNames = attributeNames;
    
	// collect the indices of output columns/attributes 
    vector<uint16_t> attributeIndices;
    for (uint16_t i = 0; i < attributeNames.size(); i++) {
        for (uint16_t j = 0; j < recordDescriptor.size(); j++) {
            if (attributeNames[i] == recordDescriptor[j].name) {
                attributeIndices.push_back(j);
                break;
            }            
        }
    }
    
	//set up the initial pageNum to -1 indicating that getNextRecord() has yet to run
    rbfm_ScanIterator.outputAttributeIndices = attributeIndices;	
	rbfm_ScanIterator.currPageNum = -1;
    rbfm_ScanIterator.currSlotNum = -1;
    rbfm_ScanIterator.outputAttributeCount = attributeNames.size();

	return SUCCESS;
}

// Never keep the results in the memory. When getNextRecord() is called, 
// a satisfying record needs to be fetched from the file.
// "data" follows the same format as RecordBasedFileManager::insertRecord().
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 
	
	void* attrValue = malloc(PAGE_SIZE);
	RID currRid;
	bool conditionMet = false;
	
	do{
		
		currSlotNum++;
		//if it's the first time running getNextRecord, reset pageNum&slotNum to 0
		//or if slotNum goes overbound, go to the next page and reset slotNum to 0
		if(currPageNum < 0 || currSlotNum >= totalSlots){
		
			currPageNum++;
			currSlotNum = 0;
			if (currFile->readPage(currPageNum, page))
				return RBFM_EOF;
			SlotDirectoryHeader slotHeader = 
				RecordBasedFileManager::_rbf_manager->getSlotDirectoryHeader(page);
			totalSlots = slotHeader.recordEntriesNumber;
		}
		
		currRid.pageNum = currPageNum;
		currRid.slotNum = currSlotNum;
		if(RecordBasedFileManager::_rbf_manager->
			readAttribute(*currFile, recordDescriptor, currRid, conditionAttribute, attrValue))
			continue;
		
		switch(conditionAttrType){
			
			case TypeInt: 
			
				conditionMet = int_comp(*(int*)attrValue);
				break;
				
			case TypeReal: 
			
				conditionMet = float_comp(*(float*)attrValue);
				break;
				
			case TypeVarChar: 
			
				uint32_t fieldSize;
				memcpy(&fieldSize, attrValue, sizeof(uint32_t));
				conditionMet = varchar_comp(string((char*)attrValue + sizeof(uint32_t), fieldSize));
				break;
			
			default: break;
		}
		
	}while(!conditionMet);
	
	//tuple matching the condition is found, set the return rid
	rid.pageNum = currPageNum;
	rid.slotNum = currSlotNum;
	
	//start writing requested field data to data
	int nullIndicatorSize = RecordBasedFileManager::_rbf_manager->getNullIndicatorSize(outputAttributeCount);
	char nullIndicator[nullIndicatorSize];
	memset(&nullIndicator, 0, nullIndicatorSize);
	
	char* offset = (char*)data + nullIndicatorSize;
	
	for(unsigned i = 0; i < outputAttributeIndices.size(); i++){
		
		string attributeName = recordDescriptor[outputAttributeIndices[i]].name;
		AttrType type = recordDescriptor[outputAttributeIndices[i]].type;
		RC result = RecordBasedFileManager::_rbf_manager->
				readAttribute(*currFile, recordDescriptor, currRid, attributeName, attrValue);
				
		if(result == SUCCESS){
			
			if(type == TypeVarChar){
				
				uint32_t fieldSize;
				memcpy(&fieldSize, attrValue, sizeof(uint32_t));
				memcpy(offset, attrValue, sizeof(uint32_t) + fieldSize);
				offset += sizeof(uint32_t) + fieldSize;
	
			}else{
				
				memcpy(offset, attrValue, INT_SIZE);
				offset += INT_SIZE;
			}
			
		}else{
			
			//if readAttribute fails, turn on the corresponding null bit 
			int indicatorIndex = (i+1) / CHAR_BIT;
			int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
			nullIndicator[indicatorIndex] |= indicatorMask;
		}
	}
	
	//write the nullIndicator to beginning of data
	memcpy(data, &nullIndicator, nullIndicatorSize);
	free(attrValue);
	return SUCCESS; 
}

bool RBFM_ScanIterator::float_comp(float val) {

    float conditionValue = *(float*)value;
    switch (compOp) {
        case EQ_OP: return (val == conditionValue);
        case LT_OP: return (val < conditionValue); 
        case GT_OP: return (val > conditionValue); 
        case LE_OP: return (val <= conditionValue); 
        case GE_OP: return (val >= conditionValue); 
        case NE_OP: return (val != conditionValue);  
        default: return true;
    }
	
}

bool RBFM_ScanIterator::int_comp(int val) {
	
    int conditionValue = *(int*)value;
    switch (compOp) {
        case EQ_OP: return (val == conditionValue);
        case LT_OP: return (val < conditionValue); 
        case GT_OP: return (val > conditionValue); 
        case LE_OP: return (val <= conditionValue); 
        case GE_OP: return (val >= conditionValue); 
        case NE_OP: return (val != conditionValue);  
        default: return true;
    }
}

bool RBFM_ScanIterator::varchar_comp(string val) {
	
    string conditionValue = string((char*)(value));
    int result = strcmp(val.c_str(), conditionValue.c_str());
    switch (compOp) {
        case EQ_OP: return (result == 0);
        case LT_OP: return (result < 0); 
        case GT_OP: return (result > 0); 
        case LE_OP: return (result <=  0); 
        case GE_OP: return (result >=  0); 
        case NE_OP: return (result != 0);  
        default: return true;
    }
}

RC RBFM_ScanIterator::close() { 
    free(page);
    return SUCCESS; 
}