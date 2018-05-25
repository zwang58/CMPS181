#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cstdint>
#include <cstring>
#include <iostream>

#include "../rbf/rbfm.h"

#define ROOT_PAGE (0)  // (default root_page number) declare the root page 
#define NO_PAGE (0xffff)
#define IX_EOF (-1)  // end of the index scan
#define DELETED_ENTRY (0x8000)

class IX_ScanIterator;
class IXFileHandle;

struct nodeHeader {     // the page format
    uint16_t left;      // the left of this page
    uint16_t right;     // the right of this page
    bool leaf;      // bolean value whether the current page is a leaf
    uint16_t pageNum;     // # of the page
    uint16_t freeSpace;    // the position of the 1st free space in a packed or unpacked page
};

struct leafEntry {         // <key, rid>
    char key[PAGE_SIZE];  
    RID rid;
    uint16_t sizeOnPage;
	Attribute attribute;
};

struct interiorEntry {     // <key, pid>
    char key[PAGE_SIZE];
    uint16_t left;        // points to the page that is <= this key
    uint16_t right;       // points to the page that is > this key
	Attribute attribute;
};


class IndexManager {
    
    public:
        friend class IX_ScanIterator;
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
		RC isKeySmaller(const Attribute &attribute, const void* pageEntryKey, const void* key);
        RC isKeyEqual(const Attribute &attribute, const void* pageEntryKey, const void* key);
        RC keyCompare(const Attribute &attr, const void* pageKey, const void* lowKey, const void* highKey, bool lowInc, bool highInc);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
		static PagedFileManager *_pf_manager;
		RC insert(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, vector<uint16_t> pageStack);
        RC insertToLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* const key, RID rid, vector<uint16_t> pageStack);
        RC insertToInterior(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t oldPage, uint16_t newPage, vector<uint16_t> pageStack); 
        void printBtree_rec(IXFileHandle &ixfileHandle, const Attribute &attribute, uint16_t pageNum, uint16_t depth) const;
        void printLeafNode(struct leafEntry leaf_entry) const;
        void printKey(const Attribute &attribute, const void* key) const;
        struct interiorEntry nextInteriorEntry(char* page, Attribute attribute, uint16_t &offset) const;
        struct leafEntry nextLeafEntry(char* page, Attribute attribute, uint16_t &offset) const; 
        uint16_t getSize(const Attribute &attribute, const void* key) const;
		RC createNewRoot(IXFileHandle &ixfileHandle, const Attribute &attribute, const void* key, uint16_t page2Num);
};


class IX_ScanIterator {
    public:
        IXFileHandle *ixfileHandle;
        Attribute attribute;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        void* page;
        uint16_t pageOffset;
        IndexManager *_index_manager;

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:
	
	FileHandle fh;

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
