//#include "dberror.h"
#include <tables.h>
#include <storage_mgr.c>
#include <btree_mgr.h>

// ----- B+-tree Structures -----
// A B+-tree stores pointer to records index by keys of a given datatype (DT_INT)
// * Index backed up by page file
// * Pages of index accessed through buffer manager
// * Each node occupies 1 page
// * Pointers to intermediate nodes should be represented by the page number of the page the node is stored in

// typedef struct BTreeHandle
// {
//     DataType keyType;
//     char *idxId;
//     void *mgmtData;
// } BTreeHandle;

// typedef struct BT_ScanHandle
// {
//     BTreeHandle *tree;
//     void *mgmtData; // == TreeData
// } BT_ScanHandle;

typedef struct TreeData
{
    BM_BufferPool *bm;
    BM_PageHandle *ph;
    int nodes; // Total number of nodes in tree
    int n;     // Max capacity of one node
    Node root; // Root node of tree

} TreeData;

// Internal Node --> [ptr, val, ptr, val, ptr, val, ptr]
// Leaf Node --> [RID, RID, RID]

typedef struct Node
{
    Node parent;  // Parent node
    int capacity; // Current capacity of node
} Node;

// ----- Index Manager Functions -----

// ***FINISHED***
// Initialize Index Manager
extern RC initIndexManager(void *mgmtData)
{

    return RC_OK;
}

// ***FINISHED***
// Shutdown Index Manager && Free any acquired resources
extern RC shutdownIndexManager()
{

    return RC_OK;
}

// ----- B+-tree Functions -----

// ***FINISHED but with potential error***
// Create B+-tree
extern RC createBtree(char *idxId, DataType keyType, int n)
{

    // Error check for data type (only supports DT_INT)
    if (keyType != DT_INT)
    {
        return RC_RM_UNKOWN_DATATYPE;
    }

    // Define variables/data structures
    SM_FileHandle fh;
    SM_PageHandle ph;
    TreeData *data;
    BTreeHandle *treeHandle;

    // Allocate memory for page handle
    ph = malloc(PAGE_SIZE * sizeof(char));

    // Create page file with given name
    createPageFile(idxId);

    // Open the new page file
    openPageFile(idxId, &fh);

    // Check capacity
    ensureCapacity(1, &fh);

    // Write max number of keys to page
    writeCurrentBlock(&fh, ph); // Potential issue with ph?********************

    // Close page file
    closePageFile(&fh);

    // Initialize Tree Data structure
    data = (TreeData *)malloc(sizeof(TreeData *));
    // data->bm =
    // data->ph =
    data->nodes = 0;
    data->n = n;
    data->root = NULL;

    // Initialize BTreeHandle structure
    treeHandle = (BTreeHandle *)malloc(sizeof(BTreeHandle *));
    treeHandle->idxId = idxId;
    treeHandle->keyType = keyType;
    treeHandle->mgmtData = data;

    return RC_OK;
}

// ***FINISHED***
// Open B+-tree for access
extern RC openBtree(BTreeHandle **tree, char *idxId)
{
    // Define variables
    BM_BufferPool *bm;
    BM_PageHandle *ph;
    TreeData *data;

    // Allocate memory
    bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool *));
    ph = (BM_PageHandle *)malloc(sizeof(BM_PageHandle *));
    *tree = (BTreeHandle *)malloc(sizeof(BTreeHandle *));

    // Initialize Tree Data structure & assign missing values
    data = (TreeData *)malloc(sizeof(TreeData *));
    data = (*tree)->mgmtData;
    data->bm = bm;
    data->ph = ph;

    // Reassign mgmt data with updated values
    (*tree)->mgmtData = data;

    // Initialize the buffer pool
    int count = 10; //******UNSURE OF WHAT NUMBER SHOULD BE
    initBufferPool((*tree)->mgmtData->bm, idxId, count, RS_FIFO, NULL);

    // **** IS THIS NECESSARY?
    // Pin the page
    pinPage((*tree)->mgmtData->bm, (*tree)->mgmtData->ph, 1);

    // **** IS THIS NECESSARY?
    // Unpin the page
    unpinPage((*tree)->mgmtData->bm, (*tree)->mgmtData->ph);

    return RC_OK;
}

// ***FINISHED***
// Close B+-tree && flush all new or modified pages back to disk
extern RC closeBtree(BTreeHandle *tree)
{

    // Initialize TreeData structure to get necessary variables
    TreeData *data = (TreeData *)malloc(sizeof(TreeData *));
    data = tree->mgmtData;

    // Flush new/modified pages back to disk
    markDirty(data->bm, data->ph);
    shutdownBufferPool(data->bm);

    // Free the allocated memory for the tree
    free(data);
    free(tree);

    return RC_OK;
}

// ***FINISHED***
// Delete B+-tree && Delete corresponding page file
extern RC deleteBtree(char *idxId)
{

    // Delete corresponding page file
    destroyPageFile(idxId);

    return RC_OK;
}

// ----- Index Manager Functions -----

// ***FINISHED***
// Assigns number of nodes to given result variable
extern RC getNumNodes(BTreeHandle *tree, int *result)
{
    // Initialize TreeData structure to get data
    TreeData *data = (TreeData *)malloc(sizeof(TreeData *));
    data = tree->mgmtData;

    // Assign number of nodes to result
    (*result) = data->nodes;

    // Free used memory
    free(data);

    return RC_OK;
}

extern RC getNumEntries(BTreeHandle *tree, int *result)
{

    // Get num of nodes......

    return RC_OK;
}

// ***FINISHED***
// Assigns data type of keys to the given result variable
extern RC getKeyType(BTreeHandle *tree, DataType *result)
{

    // Assign keyType to result
    (*result) = tree->keyType;

    return RC_OK;
}

// ----- Key Functions -----

extern RC findKey(BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey(BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey(BTreeHandle *tree, Value *key);
extern RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    // Error code: RC_IM_NO_MORE_ENTRIES

    return RC_OK;
}

// ***FINISHED***
extern RC closeTreeScan(BT_ScanHandle *handle)
{

    // Free allocated memory
    free(handle);

    return RC_OK;
}

// debug and test functions
extern char *printTree(BTreeHandle *tree);