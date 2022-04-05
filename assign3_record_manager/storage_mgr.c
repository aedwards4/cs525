#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>
#include "storage_mgr.h"

FILE *fp;

extern void initStorageManager (void) {
	printf("initializing the storage manager");
	fp = NULL;
}

//Manipulating page files

extern RC createPageFile(char *fileName)
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

extern RC openPageFile(char *fileName, SM_FileHandle *fHandle)
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

    // get total number of pages
    numPages = ftell(fp);

    struct stat fileInfo;

	if(fstat(fileno(fp), &fileInfo) < 0)    
		return RC_ERROR;

    // set attributes
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
	fHandle->totalNumPages = fileInfo.st_size/ PAGE_SIZE;
 
	fclose(fp);

    return RC_OK;
}

extern RC closePageFile (SM_FileHandle *file_handle) {
	
	//if null, the file can't be found
    if (fp == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

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

extern RC destroyPageFile (char *fileName) {
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
        return RC_FILE_NOT_FOUND;
    }
}

//Reading blocks from a disc

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	//if null, the file can't be found
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // if page does not exist
    if ((pageNum > fHandle->totalNumPages) || pageNum < 0)
    {

        // return
        return RC_READ_NON_EXISTING_PAGE;
    }

    fp = fopen(fHandle->fileName, "r");

    int res = fseek(fp, (pageNum * PAGE_SIZE), SEEK_SET);

	if(res == 0) {
		if(fread(memPage, sizeof(char), PAGE_SIZE, fp) < PAGE_SIZE)
			return RC_ERROR;
	} 
	else {
		return RC_READ_NON_EXISTING_PAGE; 
	}

	fHandle->curPagePos = ftell(fp); 

	fclose(fp);

    // return
    return RC_OK;
}

extern int getBlockPos (SM_FileHandle *fHandle) {
	
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    return fHandle->curPagePos;
}

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    if (fp == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    fread(memPage, sizeof(char), PAGE_SIZE, fp);

    // adjust the current page position in the file
    fHandle->curPagePos = 0;
    
	fclose(fp);

    return RC_OK;
}

extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

	 //if null, the file can't be found
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // helper variable for current page
    int currentPage = fHandle->curPagePos;

    // set offset
    int offset = currentPage - 1;

    // seek pointer to offset
    fseek(fp, offset, SEEK_SET);

    // Read the page into memory
    fread(memPage, sizeof(char), PAGE_SIZE, fp);

    // adjust the current page position in the file
    fHandle->curPagePos = currentPage - 1;

    // return
    return RC_OK;
}

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	//if null, the file can't be found
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // helper variable for current page
    int currentPage = fHandle->curPagePos;
    int offset = currentPage * PAGE_SIZE;

    // seek pointer to offset
    fseek(fp, offset, SEEK_SET);

    // Read the page into memory
    fread(memPage, sizeof(char), PAGE_SIZE, fp);

    // return
    return RC_OK;
}

extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	
	//if null, the file can't be found
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // helper variable for current page
    int currentPage = fHandle->curPagePos;
    int nextBlock = currentPage + 1;
    int offset = nextBlock * PAGE_SIZE;

    // seek pointer to offset
    fseek(fp, offset, SEEK_SET);

    // Read the page into memory
    fread(memPage, sizeof(char), PAGE_SIZE, fp);

    // adjust the current page position in the file
    fHandle->curPagePos = currentPage + 1;

    // return
    return RC_OK;
}

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	
	//if null, the file can't be found
    if (fHandle->fileName == NULL)
    {
        // return
        return RC_FILE_NOT_FOUND;
    }

    // helper variable for current page
    int totalPages = fHandle->totalNumPages;
    int lastPage = totalPages - 1;

    // seek pointer to offset
    fseek(fp, lastPage, SEEK_SET);

    // Read the page into memory
    fread(memPage, sizeof(char), PAGE_SIZE, fp);

    // adjust the current page position in the file
    fHandle->curPagePos = lastPage;

    // return
    return RC_OK;
}

//Writing blocks to a page file
extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	
	// check page number is less than total number of pages and 0
	if (pageNum > fHandle->totalNumPages || pageNum < 0)
        	return RC_WRITE_FAILED;
	// Open file 
	fp = fopen(fHandle->fileName, "r+");
	// check file exist
	if(fp == NULL)
		return RC_FILE_NOT_FOUND;
	int initPos = pageNum * PAGE_SIZE;	

	if(pageNum == 0) { 
	// write data to non-first page
	fseek(fp, (pageNum * PAGE_SIZE), SEEK_SET);
	// write content from memPage to file_page stream
	fwrite(memPage, sizeof(char), strlen(memPage), fp);
	// set the current page position to cursor(pointer)
	fHandle->curPagePos = ftell(fp);
	// close file   	
	fclose(fp);
	} 
	else {
		// initialize current page position to start position
		fHandle->curPagePos = initPos;
		// close file
	 	fclose(fp);
		// write conteng to current block 
	 	writeCurrentBlock(fHandle, memPage);
	}	
	return RC_OK;
}

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage) {

    fp = fopen(fHandle->fileName, "r+");
	// check if file exist
	if(fp == NULL)
	 	return RC_FILE_NOT_FOUND;

    appendEmptyBlock(fHandle);

	// Initiliazing file pointer
	fseek(fp, fHandle->curPagePos, SEEK_SET);

    // call previous write block method on current pg
    int res = fwrite(memPage, sizeof(char), strlen(memPage), fp);

    fHandle->curPagePos = ftell(fp);

	//  close file stream    	
	fclose(fp);

    // return result
    return res;
}

extern RC appendEmptyBlock (SM_FileHandle *fHandle) {
    // allocate memory
    char *newPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    fseek(fp, 0, SEEK_END);
    int res = fwrite(newPage, 1, PAGE_SIZE, fp);
    if (res != 0)
    {
        // free the memory
        free(newPage);

        // return
        return RC_WRITE_FAILED;
    }

    // increment total number of pages
    fHandle->totalNumPages++;

    // free the memory
    free(newPage);

    // return
    return RC_OK;
}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle) {
	// if fHandle not initialized, return error
    if (fHandle == NULL)
    {
        // return
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (numberOfPages <= fHandle->totalNumPages && numberOfPages >= 0)
	    appendEmptyBlock(fHandle);

	// close file	
    fclose(fp);

	//return
	return RC_OK;
}
