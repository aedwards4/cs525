#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"
#include "helper.h"

FILE *file;
RC Rcode;

void initStorageManager (void)
{
    printf("Initializing the Storage Manager");
	printf("Submitted By: \n");
	printf("Devadas Challa\n");
	printf("Kapil Gund\n");
	printf("Alexis Edwards \n");
}

RC createPageFile (char *fileName)
{
IF_NULL(fileName,RC_FILE_NOT_FOUND,"The File does not exist");
char *Mblock = malloc(PAGE_SIZE * sizeof(char));
file = fopen(fileName, "w+");
memset(Mblock,'\0',PAGE_SIZE);
fwrite(Mblock, sizeof(char), PAGE_SIZE, file); 
free(Mblock);
fclose(file);
Rcode=RC_OK;
return Rcode;
}


RC openPageFile(char *fileName, SM_FileHandle *fHandle)         
{
    file = fopen(fileName, "r+");                                          
    IF_NULL(file, RC_FILE_NOT_FOUND, "File not found.");                              
        int sum_pages;                                           
        fseek(file, 0, SEEK_END);                                
        int total_pgs = (int)(ftell(file)+1)/PAGE_SIZE;                   
        fHandle->totalNumPages = total_pgs;                    
        fHandle->curPagePos = 0;                               
        fHandle->fileName = fileName;                          
        rewind(file);   
        Rcode=RC_OK;                                         
        return Rcode;                                 
}

RC closePageFile(SM_FileHandle *fHandle)
{
    IF_NULL(file, RC_FILE_NOT_FOUND, "File not found.");                               
    Rcode = fclose(file)!=0 ? RC_FILE_NOT_FOUND : RC_OK;           
    return Rcode;
}

RC destroyPageFile (char *Fname)
{
		IF_NULL(Fname,RC_FILE_NOT_FOUND,"The File does not exist");
		if(remove(Fname)==0){
			return RC_OK;
		}
		else{
			return RC_FAILED_DEL;
		}
		return RC_OK;
}


RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) 
{
    int seekpage=pageNum * PAGE_SIZE;
    IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
        if (pageNum < 0 && pageNum > fHandle->totalNumPages )           
            Rcode = RC_READ_NON_EXISTING_PAGE;                         
        else 
        {
            fseek(file, seekpage, SEEK_SET);                     
            fread(memPage, sizeof(char), PAGE_SIZE, file);  
            fHandle->curPagePos = pageNum;                         
            Rcode = RC_OK;                                         
        }
    return Rcode;
}

int getBlockPos (SM_FileHandle *fHandle)
{
	IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
	return(fHandle->curPagePos);	
}


RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
	return(readBlock (0, fHandle, memPage));
}


RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
	int PrevBlock=fHandle->curPagePos-1;
	return(readBlock (PrevBlock, fHandle, memPage));
}


RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
	return(readBlock (fHandle->curPagePos, fHandle, memPage));
}


RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
	int NextBlock=fHandle->curPagePos+1;
	return(readBlock (NextBlock, fHandle, memPage));
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int LastBlock=fHandle->totalNumPages-1;
	return(readBlock (LastBlock, fHandle, memPage));
}


RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	if (pageNum < 0 || pageNum > fHandle->totalNumPages){
        Rcode = RC_WRITE_FAILED;
    }
	else{
		IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
        int pageoffset=PAGE_SIZE * pageNum;
			if (fseek(file, pageoffset, SEEK_SET) == 0) {
				fwrite(memPage, sizeof(char), PAGE_SIZE, file);
				(fHandle->curPagePos = pageNum); 
				fseek(file, 0, SEEK_END);
                int totalP=ftell(file) / PAGE_SIZE;
				(fHandle->totalNumPages = totalP); 
				Rcode = RC_OK;
			} else {
				Rcode = RC_WRITE_FAILED;
			}
	return Rcode;
    }

}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
	int CurBlock=fHandle->curPagePos;
	return(writeBlock (CurBlock, fHandle, memPage));
}


RC appendEmptyBlock(SM_FileHandle *fHandle) 
{
    IF_NULL(fHandle->fileName,RC_FILE_NOT_FOUND,"The File does not exist");
    
            char *FreeBlock;
            FreeBlock = (char *) calloc(PAGE_SIZE, sizeof(char));          
            
            fseek(file, 0, SEEK_END);
            if(fwrite(FreeBlock, 1, PAGE_SIZE, file) == 0)                
                Rcode = RC_WRITE_FAILED;                               
            else
            {
                fHandle->totalNumPages = ftell(file) / PAGE_SIZE;         
                fHandle->curPagePos = fHandle->totalNumPages - 1;       
                free(FreeBlock);                                           
                Rcode = RC_OK;                                         
            }
    
	return Rcode;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) 
{
    if (fHandle == NULL){
				return RC_FILE_HANDLE_NOT_INIT;
				}
        else{
	int pgs = numberOfPages - fHandle->totalNumPages;                   
	if(pgs < 0)                                                          
	{
        Rcode = RC_WRITE_FAILED;    
		                                             
	}
	else
		for (int i=0; i < pgs; i++)
		appendEmptyBlock(fHandle);                                      
		Rcode = RC_OK;                                 
        }
        return Rcode;
}


