#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"

// helper variable/pointer
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

    //if null, the file can't be found
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

RC closePageFile(SM_FileHandle *fHandle)
{

    //if null, the file can't be found
    if (fp == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // ***Unsure if necessary?
    fHandle->fileName = "";
    fHandle->curPagePos = 0;
    fHandle->totalNumPages = 0;

    // try to close the file
    int res = fclose(fp);

    // return appropriate code
    if (res == 0)
    {
        return RC_OK;
    }
    else
    {
        return RC_FILE_NOT_FOUND;
    }
}

extern RC destroyPageFile(char *fileName)
{

    //if null, the file doesn't exist
    if (fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // Remove the file
    int res = remove(fileName);

    // return appropriate code
    if (res == 0)
    {
        return RC_OK;
    }
    else
    {
        //****Need to double check this return code
        return RC_FILE_NOT_FOUND;
    }
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

    //if null, the file can't be found
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // if page does not exist
    if ((pageNum > fHandle->totalNumPages - 1) || pageNum < 0)
    {

        // return
        return RC_READ_NON_EXISTING_PAGE;
    }

    // get offset
    int offset = pageNum * PAGE_SIZE;

    // seek pointer to offset
    fseek(fp, offset, SEEK_SET);

    // Read the page into memory
    fread(memPage, sizeof(char), PAGE_SIZE, fp);

    // adjust the current page position in the file
    fHandle->curPagePos = pageNum;

    // return
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle)
{

    //if null, the file can't be found
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    return fHandle->curPagePos;
}

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