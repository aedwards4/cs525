#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TestFile "Test.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);

/* additional test cases */
static void readNotThere(void);
static void pageTest(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();
  readNotThere();
  pageTest();

  return 0;
}

void testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "Test Cases for Create, Open and Destroy";

  TEST_CHECK(createPageFile (TestFile));
  
  TEST_CHECK(openPageFile (TestFile, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TestFile) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "One Page in File");
  ASSERT_TRUE((fh.curPagePos == 0), "The page position of a newly opened file should be 0.");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TestFile));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TestFile, &fh) != RC_OK), "An error should be returned when trying to open a file that does not exist.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "Test Cases for Single Page Content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TestFile));
  TEST_CHECK(openPageFile (TestFile, &fh));
  printf("The File has been Created and Opened\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "In the first page of a newly initialized page, a zero byte is expected.");
  printf("first block was empty\n");
    
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "The value read from disk corresponds to the one we expected.");
  printf("reading first block\n");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TestFile));  
  
  TEST_DONE();
}

/* Try to create,open and close a page file */
void readNotThere(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int numPages = fh.totalNumPages;

  testName = "There are some test results that aren't over there.";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TestFile));
  TEST_CHECK(openPageFile (TestFile, &fh));
  printf("created and opened file\n");
  ASSERT_TRUE((readBlock (numPages, &fh, ph) == RC_OK), "Trying to read something that isn't there");

  // destroy new page file
  TEST_CHECK(destroyPageFile(TestFile));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TestFile, &fh) != RC_OK), "An error should be returned when trying to open a file that does not exist.");

  TEST_DONE();   
}

/* Try to create,open and close a page file */
void pageTest(void){
  SM_PageHandle ph;
  SM_FileHandle fh;
  
  int i = 0;

  testName = "putting the second page's content to the test";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TestFile));
  TEST_CHECK(openPageFile (TestFile, &fh));
  printf("created and opened file\n");

  // write the first block

  do{
    ph[i] = (i % 10) + '0';
    i++;
  } while (i < PAGE_SIZE);

  TEST_CHECK(writeBlock (0, &fh, ph));
	printf("first block is written\n");
	  
	// write the second block
  i=0;
  do{
    ph[i] = (i % 10) + '0';
    i++;
  }while(i < PAGE_SIZE);
    
  TEST_CHECK(writeBlock (1, &fh, ph));
	printf("second block is written\n");
	  
	// read the previous page 
	TEST_CHECK(readPreviousBlock (&fh, ph));
	i=0;
  do{
  ASSERT_TRUE((ph[i] == (i % 10) + '0'), "The character read from disk corresponds to the one we expected.");
  i++;
  }while(i < PAGE_SIZE);
    
	printf("reading previous block\n");

  // read the next page in the file
  TEST_CHECK(readNextBlock (&fh, ph));
	i=0;
  do{
      ASSERT_TRUE((ph[i] == (i % 10) + '0'), "The character read from disk corresponds to the one we expected.");
    i++;
  }while(i < PAGE_SIZE);
      
  printf("reading next block\n");

  // read the last page in the file
  TEST_CHECK(readLastBlock (&fh, ph));
	i=0;
  do{
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "The value read from disk corresponds to the one we anticipated.");
    i++;
  }while(i < PAGE_SIZE);

  printf("reading last block\n");

	// writing current block
	TEST_CHECK(writeCurrentBlock (&fh, ph));
	i=0;
  do{
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "The value read from disk corresponds to the one we anticipated.");
    i++;
  }while(i<PAGE_SIZE);
      
  printf("write the current block\n");

	// appending an empty bock for the file
  TEST_CHECK(appendEmptyBlock (&fh));
  printf("add an empty block to the end of the file\n");
   
	// Ensuring the capacity to be 5 so taht it won't exceed 5 pages
	TEST_CHECK(ensureCapacity (5, &fh));
	ASSERT_TRUE((fh.totalNumPages==5),"determining if the total number of pages is five");
	printf("ensuring the capacity\n");

	// destroy the page file
	TEST_CHECK(destroyPageFile (TestFile));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TestFile, &fh) != RC_OK), "An error should be returned when trying to open a file that does not exist.");

	TEST_DONE();
}
