#include<stdio.h>
#include<stdlib.h>
#include<sys/stat.h>
#include<sys/types.h>
#include<unistd.h>
#include<string.h>
#include<math.h>
#include "storage_mgr.h"

FILE *file_page;

extern void initStorageManager (void) {
	// Initialising file pointer (storage manager).
	file_page = NULL;
}

//Manipulating page files

extern RC createPageFile (char *fileName) {
	// Opening file stream in read & write mode.
	// 'w+' mode creates an empty file for both reading and writing.
	file_page = fopen(fileName, "w+");
	// Checking if file was successfully opened.
	if(file_page == NULL) {
		return RC_FILE_NOT_FOUND;
	} 
	else {
		// Creating an empty page in memory.
		SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
		// Writing empty page to file.
		if(fwrite(emptyPage, sizeof(char), PAGE_SIZE,file_page) < PAGE_SIZE)
			printf("write fail \n");
		else
			printf("write success \n");
		// Closing file stream so that all the buffers are flushed. 
		fclose(file_page);
		// De-allocating the memory previously allocated to 'emptyPage'.
		// This is better to do for proper memory management.
		free(emptyPage);
		return RC_OK;
	}
}

extern RC openPageFile (char *fileName, SM_FileHandle *file_handle) {
	// Opening file stream in read mode. 'r' mode creates an empty file for reading only.
	file_page = fopen(fileName, "r");
	// Checking if file was successfully opened.
	if(file_page == NULL) {
		return RC_FILE_NOT_FOUND;
	} 
	else { 
		// Updating file handle's file name and set the current position to the start of the page.
		file_handle->fileName = fileName;
		file_handle->curPagePos = 0;
		// Using fstat() to get the file total size.
		//fstat() is a system call that is used to determine information about a file based on its file descriptor.
		//'st_size' member variable of the 'stat' structure gives the total size of the file in bytes.
		struct stat fileInfo;
		if(fstat(fileno(file_page), &fileInfo) < 0)    
			return RC_ERROR;
		file_handle->totalNumPages = fileInfo.st_size/ PAGE_SIZE;
		// Closing file stream so that all the buffers are flushed. 
		fclose(file_page);
		return RC_OK;
	}
}

extern RC closePageFile (SM_FileHandle *file_handle) {
	// Checking if file pointer or the storage manager is intialised. If initialised, then close.
	if(file_page != NULL)
		file_page = NULL;	
	return RC_OK; 
}

extern RC destroyPageFile (char *fileName) {
	// Opening file stream in read mode. 'r' mode creates an empty file for reading only.	
	file_page = fopen(fileName, "r");
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND; 
	// Deleting the given file name so that it is no longer accessible.	
	remove(fileName);
	return RC_OK;
}

//Reading blocks from a disc

extern RC readBlock (int pageNum, SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
	if (pageNum > file_handle->totalNumPages || pageNum < 0)
        	return RC_READ_NON_EXISTING_PAGE;
	// Opening file stream in read mode. 'r' mode creates an empty file for reading only.	
	file_page = fopen(file_handle->fileName, "r");
	// Checking if file was successfully opened.
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND;
	// Setting the cursor(pointer) position of the file stream. Position is calculated by Page Number x Page Size
	// And the seek is success if fseek() return 0
	int isSeekSuccess = fseek(file_page, (pageNum * PAGE_SIZE), SEEK_SET);
	if(isSeekSuccess == 0) {
		// We're reading the content and storing it in the location pointed out by memPage.
		if(fread(memPage, sizeof(char), PAGE_SIZE, file_page) < PAGE_SIZE)
			return RC_ERROR;
	} 
	else {
		return RC_READ_NON_EXISTING_PAGE; 
	}
	// Setting the current page position to the cursor(pointer) position of the file stream
	file_handle->curPagePos = ftell(file_page); 
	// Closing file stream so that all the buffers are flushed.     	
	fclose(file_page);
    	return RC_OK;
}

extern int getBlockPos (SM_FileHandle *file_handle) {
	// Returning the current page position retrieved from the file handle	
	return file_handle->curPagePos;
}

extern RC readFirstBlock (SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Opening file stream in read ('r') mode.
	file_page = fopen(file_handle->fileName, "r");
	// Checking if file was successfully opened.
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND;
	int i;
	for(i = 0; i < PAGE_SIZE; i++) {
		// Reading a single character from the file
		char c = fgetc(file_page);
		// Checking if we have reached the end of file
		if(feof(file_page))
			break;
		else
			memPage[i] = c;
	}
	// Setting the current page position to the cursor position of the file stream
	file_handle->curPagePos = ftell(file_page); 
	// Closing file stream 
	// All the buffers are flushed.
	fclose(file_page);
	return RC_OK;
}

extern RC readPreviousBlock (SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Checking if we are on the first block.
	if(file_handle->curPagePos <= PAGE_SIZE) {
		printf("\n First block: Previous block not present.");
		return RC_READ_NON_EXISTING_PAGE;	
	} else {
		// Calculating current page number by dividing page size by current page position	.
		int currentPageNumber = file_handle->curPagePos / PAGE_SIZE;
		int startPosition = (PAGE_SIZE * (currentPageNumber - 2));
		// Opening file stream in read mode.
		file_page = fopen(file_handle->fileName, "r");
		// Checking if file was successfully opened.
		if(file_page == NULL)
			return RC_FILE_NOT_FOUND;
		// Initializing file pointer position.
		fseek(file_page, startPosition, SEEK_SET);
		int i;
		// Reading block character by character and storing it in memPage
		for(i = 0; i < PAGE_SIZE; i++) {
			memPage[i] = fgetc(file_page);
		}
		// Setting the current page position to the cursor position of the file stream
		file_handle->curPagePos = ftell(file_page); 
		// Closing file stream.
		fclose(file_page);
		return RC_OK;
	}
}

extern RC readCurrentBlock (SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Calculating current page number by dividing page size by current page position	
	int currentPageNumber = file_handle->curPagePos / PAGE_SIZE;
	int startPosition = (PAGE_SIZE * (currentPageNumber - 2));
	// Opening file stream in read mode.
	file_page = fopen(file_handle->fileName, "r");
	// Checking if file was successfully opened.
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND;
	// Initializing file pointer position.
	fseek(file_page, startPosition, SEEK_SET);
	int i;
	// Reading block character by character and storing it in memPage.
	// Also checking if we have reahed end of file.
	for(i = 0; i < PAGE_SIZE; i++) {
		char c = fgetc(file_page);		
		if(feof(file_page))
			break;
		memPage[i] = c;
	}
	// Setting the current page position to the cursor position of the file stream
	file_handle->curPagePos = ftell(file_page); 
	// Closing file stream. All the buffers are flushed.
	fclose(file_page);
	return RC_OK;	
}

extern RC readNextBlock (SM_FileHandle *file_handle, SM_PageHandle memPage){
	// Checking if we are on the last block because there's no next block to read
	if(file_handle->curPagePos == PAGE_SIZE) {
		printf("\n Last block: Next block not present.");
		return RC_READ_NON_EXISTING_PAGE;	
	} else {
		// Calculating current page number by dividing page size by current page position	
		int currentPageNumber = file_handle->curPagePos / PAGE_SIZE;
		int startPosition = (PAGE_SIZE * (currentPageNumber - 2));
		// Opening file stream in read mode.
		file_page = fopen(file_handle->fileName, "r");
		// Checking if file was successfully opened.
		if(file_page == NULL)
			return RC_FILE_NOT_FOUND;
		// Initializing file pointer position.
		fseek(file_page, startPosition, SEEK_SET);
		int i;
		// Reading block character by character and storing it in memPage.
		
		for(i = 0; i < PAGE_SIZE; i++) {
			char c = fgetc(file_page);	
			// Checking if we have reahed end of file.	
			if(feof(file_page))
				break;
			memPage[i] = c;
		}
		// Setting the current page position to the cursor(pointer) position of the file stream
		file_handle->curPagePos = ftell(file_page); 
		// Closing file stream so that all the buffers are flushed.
		fclose(file_page);
		return RC_OK;
	}
}

extern RC readLastBlock (SM_FileHandle *file_handle, SM_PageHandle memPage){
	// Opening file stream in read mode.
	file_page = fopen(file_handle->fileName, "r");
	// Checking if file was successfully opened.
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND;
	int startPosition = (file_handle->totalNumPages - 1) * PAGE_SIZE;
	// Initializing file pointer position.
	fseek(file_page, startPosition, SEEK_SET);
	int i;
	// Reading block character by character and storing it in memPage.
	for(i = 0; i < PAGE_SIZE; i++) {
		char c = fgetc(file_page);		
		// Checking if we have reahed end of file.
		if(feof(file_page))
			break;
		memPage[i] = c;
	}
	// Setting the current page position to the cursor position of the file stream
	file_handle->curPagePos = ftell(file_page); 
	// Closing file stream so that all the buffers are flushed.
	fclose(file_page);
	return RC_OK;	
}

//Writing blocks to a page file

extern RC writeBlock (int pageNum, SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
	if (pageNum > file_handle->totalNumPages || pageNum < 0)
        	return RC_WRITE_FAILED;
	// Opening file stream in read & write mode. 'r+' mode opens the file for both reading and writing.	
	file_page = fopen(file_handle->fileName, "r+");
	// Checking if file was successfully opened.
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND;
	int startPosition = pageNum * PAGE_SIZE;
	if(pageNum == 0) { 
		//Writing data to non-first page
		fseek(file_page, startPosition, SEEK_SET);	
		int i;
		for(i = 0; i < PAGE_SIZE; i++) 
		{
			// Checking if it is end of file. If yes then append an enpty block.
			if(feof(file_page)) // check file is ending in between writing
				 appendEmptyBlock(file_handle);
			// Writing a character from memPage to page file			
			fputc(memPage[i], file_page);
		}
		// Setting the current page position to the cursor position of the file stream
		file_handle->curPagePos = ftell(file_page); 
		// Closing file stream so that all the buffers are flushed.
		fclose(file_page);	
	} else {	
		// Writing data to the first page.
		file_handle->curPagePos = startPosition;
		fclose(file_page);
		writeCurrentBlock(file_handle, memPage);
	}
	return RC_OK;
}

extern RC writeCurrentBlock (SM_FileHandle *file_handle, SM_PageHandle memPage) {
	// Opening file stream in read & write mode. 'r+' mode opens the file for both reading and writing.	
	file_page = fopen(file_handle->fileName, "r+");
	// Checking if file was successfully opened.
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND;
	// Appending an empty block to make some space for the new content.
	appendEmptyBlock(file_handle);
	// Initiliazing file pointer
	fseek(file_page, file_handle->curPagePos, SEEK_SET);
	// Writing memPage contents to the file.
	fwrite(memPage, sizeof(char), strlen(memPage), file_page);
	// Setting the current page position to the cursor(pointer) position of the file stream
	file_handle->curPagePos = ftell(file_page);
	// Closing file stream so that all the buffers are flushed.     	
	fclose(file_page);
	return RC_OK;
}

extern RC appendEmptyBlock (SM_FileHandle *file_handle) {
	// Creating an empty page of size PAGE_SIZE bytes
	SM_PageHandle emptyBlock = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
	// Moving the cursor (pointer) position to the begining of the file stream.
	// And the seek is success if fseek() return 0
	int isSeekSuccess = fseek(file_page, 0, SEEK_END);
	if( isSeekSuccess == 0 ) {
		// Writing an empty page to the file
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, file_page);
	} 
	else {
		free(emptyBlock);
		return RC_WRITE_FAILED;
	}
	// De-allocating the memory previously allocated to 'emptyPage'.
	// This is optional but always better to do for proper memory management.
	free(emptyBlock);
	// Incrementing the total number of pages since we added an empty black.
	file_handle->totalNumPages++;
	return RC_OK;
}

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *file_handle) {
	// Opening file stream in append mode. 'a' mode opens the file to append the data at the end of file.
	file_page = fopen(file_handle->fileName, "a");
	if(file_page == NULL)
		return RC_FILE_NOT_FOUND;
	// Checking if numberOfPages is greater than totalNumPages.
	// If numberOfPages is greater than totalNumPages, then add empty pages till numberofPages = totalNumPages
	while(numberOfPages > file_handle->totalNumPages)
		appendEmptyBlock(file_handle);
	// Closing file stream so that all the buffers are flushed. 
	fclose(file_page);
	return RC_OK;
}
