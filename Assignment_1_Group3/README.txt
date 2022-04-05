Assignment 1 - Storage Manager

- - - - - - - - - - - - - - - - -
        Group Members:
- - - - - - - - - - - - - - - - -

Name - V.C Chandra Kishore
CWID - A20491272
Email- cvuppalachalapathy@hawk.iit.edu 


Name - Sai Sumeeth Reddy Manda
CWID - A20482283
Email- mreddy8@hawk.iit.edu 


Name - Preethi Vempati
CWID - A20466855
Email- pvempati1@hawk.iit.edu 


- - - - - - - - - - - - - - - - -
           To Run:
- - - - - - - - - - - - - - - - -

$ make
$ ./assign1 



- - - - - - - - - - - - - - - - -
     MANIPULATING FUNCTIONS:
- - - - - - - - - - - - - - - - -

createPageFile() It creates a page and then fills it with 0 bytes.

openPageFile() It opens a file if found, else it returns an error msg.

closePageFile - It is used to close an opened file.

destroyPageFile - It is used to delete a page that has been created.


- - - - - - - - - - - - - - - - -
        READ FUNCTIONS:
- - - - - - - - - - - - - - - - -

readBlock() It reads a block in a file that is at PageNum position. 

getBlockPos() It returns the current position of a block.

readFirstBlock() It reads the first block of a file.

readPreviousBlock() It reads the previous block of a file

readCurrentBlock() It Reads the current block of the file.

readNextBlock() It is used to read the next block in a file. 

readLastBlock() It reads the last block of the file.

- - - - - - - - - - - - - - - - -
        WRITE FUNCTIONS:
- - - - - - - - - - - - - - - - -

writeBlock() It writes a page to a disk.

writeCurrentBlock() It is used to write data on page's current postion.

appendEmptyBlock() It is used to append an empty block at the end.

ensureCapacity() It is used to make sure that the number of pages are more and no proble is caused dur to it.
 





