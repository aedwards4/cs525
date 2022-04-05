#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"

// initialize variables
int clockPtr, LFUPtr, rearIndx, frameAddCount, frameWriteCount, bufferCapacity = 0;
typedef struct Page
{
	// number of clients accessing that page
	int clientCount;
	// page's actual data
	SM_PageHandle pageInfo;
	// page used the fewest times
	int usedTimes;
	// a unique identifying number.
	PageNumber pageNum;
	// modified page's content
	int dirBits;
	// page with the fewest visits
	int refNum;

} pgs;

/**
 * LRU Function
 *
 * @param bufferPool
 * @param newPage
 */
extern void LRU(BM_BufferPool *const bufferPool, pgs *newPage)
{
	SM_FileHandle fh;						  // file handler pointer
	pgs *pages = (pgs *)bufferPool->bookInfo; // initialize page frame with buffer pool book info
	// initialize values
	int idx = 0;
	int lhIdx = 0;
	int lhNum;
	while (idx < bufferCapacity)
	{ // loop over pages
		if (pages[idx].clientCount == 0)
		{								  // check if page is already used by clients
			lhIdx = idx;				  // last index and current index are same
			lhNum = pages[idx].usedTimes; // how many times the page is used by clients
			break;
		}
		idx++; // increment index by one value
	}
	idx = lhIdx + 1;
	while (idx < bufferCapacity)
	{ // loop over pages of buffer pool
		if (pages[idx].usedTimes < lhNum)
		{
			lhIdx = idx;				  // last index and current index are same
			lhNum = pages[idx].usedTimes; // how many times the page is used by clients
		}
		idx++; // increment index by one value
	}
	if (pages[lhIdx].dirBits == 1)
	{ // check for dirty pages

		openPageFile(bufferPool->file, &fh);						  // open file for writing contents
		writeBlock(pages[lhIdx].pageNum, &fh, pages[lhIdx].pageInfo); // write block of data to pages
		frameWriteCount++;											  // Increase the write count by one value
	}
	pages[lhIdx] = newPage[0]; // store contents
}

/**
 * Label the page as modified by another client
 *
 * @param bufferPool
 * @param page
 * @return RC
 */
extern RC markDirty(BM_BufferPool *const bufferPool, *const pgHandler)
{
	pgs *pages = (pgs *)bufferPool->bookInfo; // initialize page frame with buffer pool book info
	int idx = 0;							  // assign initial index to zero
	if (bufferCapacity == 0)
	{					 // null and empty check for buffer size
		return RC_ERROR; // if buffer size is zero or null return error
	}
	while (idx < bufferCapacity)
	{ // iterate over pages in the buffer
		if (pages[idx].pageNum == pgHandler->pageNum)
		{							// // current page
			pages[idx].dirBits = 1; // it means already used
			return RC_OK;
		}
		idx++; // increment index by one value
	}

	return RC_ERROR;
}

/**
 * Queue for enqueue and dequeue data
 *
 * @param bufferPool
 * @param page
 */
extern void queue(BM_BufferPool *const bufferPool, pgs *newPage)
{
	int idx = 0;							  // initialize index to zero
	pgs *pages = (pgs *)bufferPool->bookInfo; // initialize page frame with buffer pool book info
	int fPointer = rearIndx % bufferCapacity; // initialize and assign front ponter value
	while (idx < bufferCapacity)
	{ // loop over pages in the buffer
		if (pages[fPointer].clientCount != 0)
		{				// check if page is access by any client
			fPointer++; // move to next frame
			if (fPointer % bufferCapacity == 0)
			{				  // check if front pointer reaches max capacity
				fPointer = 0; // reset pointer
			}
		}
		else
		{
			if (pages[fPointer].dirBits == 1)
			{ // check if frame already used
				SM_FileHandle fh;
				openPageFile(bufferPool->file, &fh);								// open file before writing data
				writeBlock(pages[fPointer].pageNum, &fh, pages[fPointer].pageInfo); // write block of data into file
				frameWriteCount++;													// incremt write count by one value
			}
			pages[fPointer] = newPage[0]; // store contents
			break;
		}
		idx++; // increment index by one value
	}
}

/**
 * Get the number of write operations performed
 *
 * @param bm - BM_BufferPool
 * @return int - number of write pages
 */
extern int getNumWriteIO()
{
	return frameWriteCount; // return write count
}

/**
 * initialize buffer pool
 *
 * @param bufferPool
 * @param pagesName
 * @param count
 * @param stg
 * @param intInfo
 * @return RC
 */
extern RC(BM_BufferPool *const bufferPool, const char *const pagesName, const int count, ReplacementStrategy stg, void *intInfo)
{
	// set aside Memory space
	pgs *pages = malloc(sizeof(pgs) * count); // initialize page frame
	bufferPool->file = (char *)pagesName;	  // assign filename to buffer managers pagefile
	int idx = 0;							  // assign initial index to zero
	bufferPool->replaceStgy = stg;			  // initialize strategy to bufferpool starategy
	bufferCapacity = count;					  // initialize number of pagesi.e count to buffer size
	bufferPool->pageCount = count;			  // // initialize count to number of pages
	// pages in the buffer pool are being initalized.
	while (idx < bufferCapacity)
	{																								// loop over pages
		pages[idx].usedTimes = pages[idx].refNum = pages[idx].dirBits = pages[idx].clientCount = 0; // initalize zero
		pages[idx].pageNum = -1;																	// initalize -1 to pafe frame's page number
		pages[idx].pageInfo = NULL;																	// initalize null to pafe frame's page info
		idx++;
	}
	bufferPool->bookInfo = pages;			 // assign page frame to buffer pool manage data
	frameWriteCount = clockPtr = LFUPtr = 0; // reset write count, clock pointer and lfu pointer
	return RC_OK;
}
/**
 *  Shutdown buffer pool
 *
 * @param bufferPool
 * @return RC
 */
extern RC shutdownBufferPool(BM_BufferPool *const bufferPool)
{
	pgs *pages = (pgs *)bufferPool->bookInfo; // initialize page frame
	forceFlushPool(bufferPool);				  // write used pages to disk
	int idx = 0;							  // initialize index to zero
	while (idx < bufferCapacity)
	{ // start with zero index untill reaches to buffer size
		if (pages[idx].clientCount != 0)
		{ // page used by client
			return RC_PINNED_PAGES_IN_BUFFER;
		}
		idx++; // increment index by one value
	}
	free(pages); // Taking up space that the page previously occupied
	bufferPool->bookInfo = NULL;
	return RC_OK;
}

/**
 * Remove page from the memory
 *
 * @param bufferPool - buffer pool pointer
 * @param page - page handler pointer
 * @return RC
 */
extern RC unPinPage(BM_BufferPool *const bufferPool, *const pgHandler)
{
	pgs *pages = (pgs *)bufferPool->bookInfo; // initialize page frame
	int idx = 0;							  // initialize index value by one
	while (idx < bufferCapacity)
	{												  // loop over pages in the buffer pool
		if (pages[idx].pageNum == pgHandler->pageNum) // check if page frame count and page handler count are same
		{
			pages[idx].clientCount--; // decrease client count by one value
			break;					  // exit the loop
		}
		idx++; // increment index value by one
	}
	return RC_OK;
}

/**
 * flush page from buffer pool
 *
 * @param bufferPool
 * @return RC
 */
extern RC forceFlushPool(BM_BufferPool *const bufferPool)
{
	pgs *pages = (pgs *)bufferPool->bookInfo;
	int idx = 0; // initialize index value with zßero
	while (idx < bufferCapacity)
	{ // loop over pages untill index equals capacity
		if (pages[idx].clientCount == 0 && pages[idx].dirBits == 1)
		{ // check client count and dirty bit flag
			SM_FileHandle fh;
			openPageFile(bufferPool->file, &fh);					  // open file
			writeBlock(pages[idx].pageNum, &fh, pages[idx].pageInfo); // write data to page
			pages[idx].dirBits = 0;									  // page not used by any client
			frameWriteCount++;										  // increment write count by one value
		}
		idx++; // increment index by one value
	}
	return RC_OK;
}

/**
 * Get the number of Read Operations performed
 *
 * @param bm - BM_BufferPool
 * @return int - number of pages read from disk
 */
extern int getNumReadIO()
{
	// increment index by one value and return it
	return (rearIndx + 1);
}

/**
 * Get the Frame Contents object
 *
 * @param bm BM_BufferPool
 * @return PageNumber* list of page numbers
 */
extern PageNumber *getFrameContents(BM_BufferPool *const bufferPool)
{
	pgs *pages = (pgs *)bufferPool->bookInfo;					  // frame pointer
	PageNumber *fc = malloc(sizeof(PageNumber) * bufferCapacity); // assign page number with buffer pool capacity
	if (NULL == fc)												  // if no pages then exit
		return 0;

	int idx = 0; // initialize index with zero value
	while (idx < bufferCapacity)
	{ // loop over pages
		if (pages[idx].pageNum != -1)
		{								  // page should not be empty
			fc[idx] = pages[idx].pageNum; // assign frame page number to fc
		}
		else
		{
			fc[idx] = NO_PAGE; // mark no page exist
		}
		idx++; // increment index by one value
	}

	return fc; // return page number object
}

/**
 * Get the Labeld Flags object
 *
 * @param bm
 * @return true
 * @return false
 */
extern bool *getDirtyFlags(BM_BufferPool *const bufferPool)
{
	pgs *pages = (pgs *)bufferPool->bookInfo; // frame pointer
	bool *labeled = malloc(sizeof(bool) * bufferCapacity);

	if (bufferCapacity == 0) // check if pages are null
		return false;		 // retun label as false

	int idx = 0;
	while (idx < bufferCapacity)
	{ // loop over pages
		if (pages[idx].dirBits == 1)
		{
			labeled[idx] = true; // mark page as used by other client
		}
		else
		{
			labeled[idx] = false; // page not used by any other client
		}
		idx++; // increment index value by one
	}

	return labeled; // return label
}

/**
 * Get the Fix Counts object
 *
 * @param bm
 * @return int* - fix count of the page
 */
extern int *getFixCounts(BM_BufferPool *const bufferPool)
{
	// Allocate the memory
	pgs *pages = (pgs *)bufferPool->bookInfo;
	int *fixNumber = malloc(sizeof(int) * bufferCapacity);
	if (NULL == fixNumber)
		return 0;

	// loop over the buffer pool
	int idx = 0;
	while (idx < bufferCapacity)
	{
		if (pages[idx].clientCount != -1)
		{
			fixNumber[idx] = pages[idx].clientCount;
		}
		else
		{
			fixNumber[idx] = 0;
		}
		idx++;
	}

	// return fix count
	return fixNumber;
}

/**
 * pin a page to the buffer pool
 *
 * @param bm
 * @param page
 * @param pageNum
 * @return RC
 */
extern RC pinPage(BM_BufferPool *const bufferPool, *const pgHandler, const PageNumber pageNum)
{
	SM_FileHandle fh;
	pgs *pages = (pgs *)bufferPool->bookInfo;
	bool isBufferFull = true;
	if (pages[0].pageNum != -1)
	{				 // buffer pool should not be empty
		int idx = 0; // initialize index with zero
		while (idx < bufferCapacity)
		{ // loop over bufferpool to pin pages
			if (pages[idx].pageNum == -1)
			{															// incase buffer pool is not empty
				openPageFile(bufferPool->file, &fh);					// open file
				pages[idx].pageInfo = (SM_PageHandle)malloc(PAGE_SIZE); // allocate memory
				readBlock(pageNum, &fh, pages[idx].pageInfo);			// read block from page
				pages[idx].pageNum = pageNum;							// assign page number to the page
				pages[idx].clientCount = 1;								// update client count
				pages[idx].refNum = 0;									// assign reference number with zero value
				rearIndx++;												// go to next index
				frameAddCount++;										// increase frame add count by one
				if (bufferPool->replaceStgy == RS_LRU)					// if replacement strategy is LRU
					pages[idx].usedTimes = frameAddCount;				// page used by clients is same as frame count
				else if (bufferPool->replaceStgy == RS_CLOCK)			// if replacement strategy is RS_CLOCK
					pages[idx].usedTimes = 1;							// page used by clients is one
				pgHandler->pageNum = pageNum;							// assign current page number to page handlers page number
				pgHandler->data = pages[idx].pageInfo;					// assign current page info to page handlers data
				isBufferFull = false;									// buffer pool as not full
				break;
			}
			if (pages[idx].pageNum == pageNum)
			{							  // current page number and Pages page number equal
				pages[idx].clientCount++; // increment page client count
				isBufferFull = false;
				frameAddCount++;							  // increment frame add count
				if (bufferPool->replaceStgy == RS_LRU)		  // if replacement strategy is LRU
					pages[idx].usedTimes = frameAddCount;	  // page used by clients is same as frame count
				else if (bufferPool->replaceStgy == RS_CLOCK) // if replacement strategy is RS_CLOCK
					pages[idx].usedTimes = 1;				  // page used by clients is one
				pgHandler->pageNum = pageNum;				  // assign current page number to page handlers page number
				pgHandler->data = pages[idx].pageInfo;		  // assign current page info to page handlers data
				clockPtr++;									  // increment clock pointer by one value
				break;
			}

			idx++; // increment index
		}
		if (isBufferFull)
		{														   // incase buffer pool is full
			pgs *new_page = (pgs *)malloc(sizeof(pgs));			   // allocate memory
			openPageFile(bufferPool->file, &fh);				   // open file
			new_page->pageInfo = (SM_PageHandle)malloc(PAGE_SIZE); // allocate memory to new page's page info
			readBlock(pageNum, &fh, new_page->pageInfo);		   // read block to new page
			new_page->pageNum = pageNum;						   // assign current page number to new pages page number
			new_page->dirBits = 0;								   // page is not dirty yet
			new_page->clientCount = 1;							   // client count is one
			new_page->refNum = 0;								   // no reference numbers yet
			rearIndx++;											   // increment rear index
			frameAddCount++;									   // increment frame add coun
			pgHandler->pageNum = pageNum;						   // assign page number to page handlers page number
			pgHandler->data = new_page->pageInfo;				   // assign page info to new pages page info
			if (bufferPool->replaceStgy == 0)
			{								 // if starttegy is queue
				queue(bufferPool, new_page); // call queue functionality
				new_page->usedTimes = 1;	 // assign dirty flag
			}
			else if (bufferPool->replaceStgy == 1)
			{										 // starategy is lru
				new_page->usedTimes = frameAddCount; // assign frame add count to dirty value
				LRU(bufferPool, new_page);			 // call LRU function
			}
			else
			{
				// nothing to execute
			}
		}
		return RC_OK;
	}

	openPageFile(bufferPool->file, &fh);				  // open file to read block
	pages[0].pageInfo = (SM_PageHandle)malloc(PAGE_SIZE); // allocate memory
	ensureCapacity(pageNum, &fh);						  // check for adequate memory size
	readBlock(pageNum, &fh, pages[0].pageInfo);			  // read block by block
	pages[0].pageNum = pageNum;							  //
	pages[0].clientCount++;								  // increment client count
	rearIndx = frameAddCount = 0;						  // frame count and rear index both are zero
	pages[0].usedTimes = frameAddCount;					  // assign frame count to pages number of clients
	pages[0].refNum = 0;								  // at this pont reference number is zero
	pgHandler->pageNum = pageNum;						  // assign page number to page handlers page number
	pgHandler->data = pages[0].pageInfo;				  // assign page info to page handlers data

	// retun
	return RC_OK;
}
