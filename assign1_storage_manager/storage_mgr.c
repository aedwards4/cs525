#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

// typedef struct SM_FileHandle {
// 	char *fileName;
// 	int totalNumPages;
// 	int curPagePos;
// 	void *mgmtInfo;
// } SM_FileHandle;

// typedef char* SM_PageHandle;'

FILE *fp;

void initStorageManager(void)
{
    printf("initializing the storage manager");
}

RC createPageFile(char *fileName)
{

    //if null, the file doesn't exist
    if (fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    //otherwise,
    //allocate one page size
    char *block = malloc(PAGE_SIZE * sizeof(char));

    //set bytes
    memset(block, '\0', PAGE_SIZE);

    //open file with write permissions
    fp = fopen(fileName, "w");

    //write to file
    fwrite(block, sizeof(char), PAGE_SIZE, fp);

    //free the memory
    free(block);

    //close the file
    fclose(fp);

    //return status
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    // helper variable
    int numPages;

    // open the file with read permissions
    fp = fopen(fileName, "r");

    //if null, the file doesn't exist
    if (fp == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // set position
    fseek(fp, 0, SEEK_END);

    // get total number of pages
    numPages = ftell(fp);

    // set attributes
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = (int)(numPages % PAGE_SIZE + 1);

    // return
    return RC_OK;
}

extern RC closePageFile(SM_FileHandle *fHandle);
extern RC destroyPageFile(char *fileName);

/* reading blocks from disc */
extern RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern int getBlockPos(SM_FileHandle *fHandle);
extern RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage);

/* writing blocks to a page file */
extern RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage);
extern RC appendEmptyBlock(SM_FileHandle *fHandle);
extern RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle);