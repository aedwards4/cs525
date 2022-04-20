#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct RecordManager
{
	Expr *condition;
	BM_PageHandle page_handle;
	int num_of_tuples;
	BM_BufferPool buffer_pool;
	int free_page;
	RID record_id;
	int num_of_scan;
	
} RecordManager;

const int ONE = 1;
const int ZERO = 0;
const int MAX_PAGES = 77;
const int ATTR_CAPACITY = 20; 

RecordManager *record_mgr;


/***************************************************************
****************  Table and Manager Functions  *****************
***************************************************************/
 
extern RC initRecordManager (void *mgmtData) {
	// Return success code
	return RC_OK;
}

extern RC shutdownRecordManager (){
	// Return success code
	return RC_OK;
}

int findFreeSlot(char *data, int rSize)
{
	int count = PAGE_SIZE / rSize; 
	int i = 0;
	while(i < count ){
		if (data[i * rSize] != '+')
		   return i;
		i++;
	}

	return -1;
}

/**
 * @brief Create a Table object
 * 
 * @param name table name
 * @param schema 
 * @return RC 
 */
extern RC createTable (char *name, Schema *schema)
{
	size_t fourBytes = sizeof(int);
	// initialize data size with maximum page size
	char data[PAGE_SIZE];
	// assign data to page handler
	char *ph = data;
	// initially page handler is assigned zero
	*(int*)ph = 0; 
	// increase memory allocation
	ph += fourBytes;
	*(int*)ph = 1;
	// increase memory allocation by size of int
	ph += fourBytes;
	// assign schema attributes to page handler
	*(int*)ph = schema->numAttr;
	ph += fourBytes; 
	*(int*)ph = schema->keySize;
	// increase memory allocation by size of int
	ph += fourBytes;
	// allocate memory to record manager
	record_mgr = (RecordManager*) malloc(sizeof(RecordManager));
	// initialize buffer pool with max pages
	initBufferPool(&record_mgr->buffer_pool, name, MAX_PAGES, RS_LRU, NULL);
	int i = 0;
	while(i < schema->numAttr){
		strncpy(ph, schema->attrNames[i], ATTR_CAPACITY);
	    ph += ATTR_CAPACITY;
	    // assign schema datatype to page handler 	
	    *(int*)ph = (int)schema->dataTypes[i];
		// add four bytes of memory to page handler 
	    ph += fourBytes;
        // assign schema type length to page handler
	    *(int*)ph = (int) schema->typeLength[i];
		// add four bytes of memory to page handler 
	    ph += fourBytes;
		i++;
	}

	SM_FileHandle fileH;
	
    int res = createPageFile(name);
	// return RC_OK;
	if (res == RC_OK) {

		// Open the newly created page file
		res = openPageFile(name, &fileH);

		if (res == RC_OK){

			// Check capacity of first page
			res = ensureCapacity(1, &fileH);

			if (res == RC_OK){

				// Write serialized schema to first page
				writeBlock(0, &fileH, data);

				// Close the page file
				closePageFile(&fileH);

			} else {

				// Return error code
				return res;
			}

		} else {

			// Return error code
			return res;
		}

	} else {

		// Return error code
		return res;
	}

	// Return success code
	return RC_OK;

}

/**
 * @brief 
 * 
 * @param rel 
 * @param name 
 * @return RC 
 */
extern RC openTable (RM_TableData *rel, char *name)
{
	Schema *schema = (Schema*) malloc(sizeof(Schema));
	size_t fourBytes = sizeof(int);
	//Assign TableData attributes
	rel->mgmtData = record_mgr;
	rel->name = name;
	// pin page
	pinPage(&record_mgr->buffer_pool, &record_mgr->page_handle, 0);
	//assign sm_page_handler with record managers page_handler data
	SM_PageHandle sm_ph = (char*) record_mgr->page_handle.data;
	// assign value to record managers numer of tuples
	record_mgr->num_of_tuples= *(int*)sm_ph;
	// add four bytes of memory to page handler 
	sm_ph += fourBytes;
    // assign value to record managers freepage
	record_mgr->free_page= *(int*) sm_ph;
	// add four bytes of memory to page handler 
    sm_ph += fourBytes;
	// assign number of attributes
    int num_f = *(int*)sm_ph;
	// add four bytes of memory to page handler 
	sm_ph += fourBytes;
 	// Initialize the Schema and assign to attribute
	schema->numAttr = num_f;
	// assign one byte of memory to schema attributeNames
	schema->attrNames = (char**) malloc(sizeof(char*) *num_f);
	// assign sizeof DataType memory to schema attributeNames
	schema->dataTypes = (DataType*) malloc(sizeof(DataType) *num_f);
	// assign four bytes of memory to schema typelength 
	schema->typeLength = (int*) malloc(fourBytes *num_f);
	int itr = 0;
	// iterate over and allocatememory to schema attributes 
    while(itr < num_f){
        schema->attrNames[itr]= (char*) malloc(ATTR_CAPACITY);
		itr++;
	}

	int itm = 0;
	// loop over all attributes 
    while(itm < num_f){
        strncpy(schema->attrNames[itm], sm_ph, ATTR_CAPACITY);
		// add the size of attribute to page handler 
		sm_ph += ATTR_CAPACITY;
	    // assign page handler to schema datatype
		schema->dataTypes[itm]= *(int*) sm_ph;
		// add four bytes of memory to page handler 
		sm_ph += fourBytes;
        // assign page handler to schema datatype
		schema->typeLength[itm]= *(int*)sm_ph;
		// add four bytes of memory to page handler 
		sm_ph += fourBytes;
		itm++;
	}  
	// assign schema to table data schema
	rel->schema = schema;	
	// Unpin the page handle
	unPinPage(&record_mgr->buffer_pool, &record_mgr->page_handle);
	// Return success code
	return RC_OK;
}    

/**
 * @brief close table
 * 
 * @param rel RM_TableData
 * @return RC return code
 */
extern RC closeTable (RM_TableData *rel)
{
	// shut down the buffer BM_BufferPool
	RecordManager *rm_mgmt_data = rel->mgmtData;
	shutdownBufferPool(&rm_mgmt_data->buffer_pool);

	// Return success code
	return RC_OK;
}

/**
 * @brief delete table
 * 
 * @param name table name
 * @return RC destroy page file response
 */
extern RC deleteTable (char *name)
{
	// destroy page file with given name
	int res = destroyPageFile(name);

	// Return success or error code
	return res;

}

extern int getNumTuples (RM_TableData *rel)
{
	RecordManager *record_mgr = rel->mgmtData;
	// Return number of tuples
	return record_mgr->num_of_tuples;
}

/***************************************************************
*********************  Record Functions  ***********************
***************************************************************/


/**
 * @brief Create a Record object
 * 
 * @param record 
 * @param schema 
 * @return RC 
 */
extern RC createRecord (Record **record, Schema *schema)
{
	
	int rSize = getRecordSize(schema);
	// Allocate memory to record
	*record = (Record*) malloc(sizeof(Record));
	// Allocate memory to record->data
	(*record)->data = (char*) malloc(rSize);
	(*record)->id.page = (*record)->id.slot = -1;
	// initialize data pointer
	char *data_ptr = (*record)->data;
	*data_ptr = '-';
	*(++data_ptr) = '\0';
	
	// Return success code
	return RC_OK;
}

/**
 * @brief Insert a new record into the table and assign RID 
 * to the newly created record
 * 
 * @param one_rel relation handled by Record Manager
 * @param record new record
 * @return RC 
 */
extern RC insertRecord (RM_TableData *one_rel, Record *record)
{
	// create record id
	RID *record_id = &record->id; 
	// get size of the record
	int record_size = getRecordSize(one_rel->schema);
	RecordManager *record_mgr = one_rel->mgmtData;	
	// mark the page as free page
	record_id->page = record_mgr->free_page;
    // pin the record page, to mark that it is in use
	pinPage(&record_mgr->buffer_pool, &record_mgr->page_handle, record_id->page);
	// create record data
	char *record_data = record_mgr->page_handle.data;
	record_id->slot = findFreeSlot(record_data, record_size);
	// Loop with page and slot
	while(record_id->slot == -1){
		// unpin the page as now it has been used
		unPinPage(&record_mgr->buffer_pool, &record_mgr->page_handle);	
		record_id->page++;
		// pin the record page, to mark that it is in use
		pinPage(&record_mgr->buffer_pool, &record_mgr->page_handle, record_id->page);
		record_data = record_mgr->page_handle.data;
		record_id->slot = findFreeSlot(record_data, record_size);
	}
	// mark page as used 
	markDirty(&record_mgr->buffer_pool, &record_mgr->page_handle);
	char *pos = record_data + (record_id->slot * record_size);
	*pos = '+';
	// insert the new record data, into the Table i.e. Pages of the PageFile
	memcpy(++pos, record->data + 1, record_size - 1);
	// unpin the page as now it has been used
	unPinPage(&record_mgr->buffer_pool, &record_mgr->page_handle);
	record_mgr->num_of_tuples++;
	// pin the record page, to mark that it is in use
	pinPage(&record_mgr->buffer_pool, &record_mgr->page_handle, 0);
	return RC_OK;
}

/**
 * @brief update an existing record with new values
 * 
 * @param one_rel relation handled by Record Manager
 * @param record single record to modify
 * @return RC 
 */
extern RC updateRecord (RM_TableData *one_rel, Record *record)
{	
	// assign record manager with single relation in table data 
	RecordManager *record_mgr = one_rel->mgmtData;
	// pin page in buffer 
	pinPage(&record_mgr->buffer_pool, &record_mgr->page_handle, record->id.page);
	char *record_data; // declare new record variable
	// add data to new record
	record_data = record_mgr->page_handle.data + (record->id.slot * getRecordSize(one_rel->schema));
	// append record
	*record_data = '+'; 
	// copy data into new record
	memcpy(++record_data, record->data + 1, getRecordSize(one_rel->schema) - 1 ); 
	// mark record as dirty
	markDirty(&record_mgr->buffer_pool, &record_mgr->page_handle);
	// unpin record and leave
	unPinPage(&record_mgr->buffer_pool, &record_mgr->page_handle);

	return RC_OK;	
}

/**
 * @brief Delete an existing record from the Table
 * 
 * @param one_rel one relation handled by Record Manager
 * @param id id of the record to delete
 * @return RC 
 */
extern RC deleteRecord (RM_TableData *one_rel, RID id)
{
	// assign record manager with single relation in table data 
	RecordManager *record_mgr = one_rel->mgmtData;
	// pin page in buffer 
	pinPage(&record_mgr->buffer_pool, &record_mgr->page_handle, id.page);
	record_mgr->free_page = id.page;
	// add record to be deleted to record dta
	char *record_data = record_mgr->page_handle.data + 
	                    (id.slot * getRecordSize(one_rel->schema));
    // delete record					
	*record_data = '-';
	// mark page as dirty
	markDirty(&record_mgr->buffer_pool, &record_mgr->page_handle);
	// unpin page and leave
	unPinPage(&record_mgr->buffer_pool, &record_mgr->page_handle);
	return RC_OK;
}

/**
 * @brief Get the Record object
 * 
 * @param one_rel TableData relation 
 * @param id record id
 * @param record record object
 * @return RC 
 */
extern RC getRecord (RM_TableData *one_rel, RID id, Record *record)
{
	RecordManager *record_mgr = one_rel->mgmtData;
	// pin page 
	pinPage(&record_mgr->buffer_pool, &record_mgr->page_handle, id.page);
	// get record size
	int record_size = getRecordSize(one_rel->schema);
	// initialize data pointer
	char *data_ptr = record_mgr->page_handle.data;
	
	data_ptr += (id.slot * record_size);
	if(*data_ptr != '+'){
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	}
	else{
		record->id = id;
		char *data = record->data;
		// copy memory
		memcpy(++data, data_ptr + 1, record_size - 1);
	}
	// un pin page
	unPinPage(&record_mgr->buffer_pool, &record_mgr->page_handle);
	// return success
	return RC_OK;
}

/**
 * @brief Get the Record Size object
 * 
 * @param schema 
 * @return int 
 */
extern int getRecordSize (Schema *schema){

	// Helper variables
	int rSize = 0;
	int numAttr = schema->numAttr;
	DataType dt;
	int i;

	// Calculate record size
	for(i=0; i<numAttr; i++){

		dt = schema->dataTypes[i];

		if (dt == DT_INT) {

			rSize += sizeof(int);

		} else if (dt == DT_STRING) {

			int length = schema->typeLength[i];
			rSize += length;

		} else if (dt == DT_FLOAT) {

			rSize += sizeof(float);

		} else if (dt == DT_BOOL){

			rSize += sizeof(bool);

		}
	}

	// Return record size
	return ++rSize;

}

/***************************************************************
*******************   Scan Functions    ************************
***************************************************************/
/**
 * @brief Scan function
 * 
 * @param one_rel 
 * @param sHandle 
 * @param expr_condition 
 * @return RC 
 */
extern RC startScan (RM_TableData *one_rel, RM_ScanHandle *sHandle, Expr *expr_condition)
{
	// check for expr condition not null
	if (expr_condition != NULL){
		// open table for scan
	    openTable(one_rel, "ScanTable");  
		// allocate memory for scan manager 
        RecordManager *scan_mgr = (RecordManager*) malloc(sizeof(RecordManager));
		// initialize scan manager to mgmt scan handles mgmt data
        sHandle->mgmtData = scan_mgr;
		// initialize table data to mgmt scan handles relation
		sHandle->rel= one_rel;
		// assign scan mangers attributes
        scan_mgr->record_id.page = ONE;
		// clear slot and number of scans
	    scan_mgr->record_id.slot = scan_mgr->num_of_scan = ZERO;
        scan_mgr->condition = expr_condition;
		// initialize table manager
        RecordManager *table_mgr = one_rel->mgmtData;
		// assign numer of tuples with attribute capacity
        table_mgr->num_of_tuples = ATTR_CAPACITY;
       
	    return RC_OK;
	}

	return RC_SCAN_CONDITION_NOT_FOUND;
}

/**
 * @brief Next function
 * 
 * @param sHandle 
 * @param record 
 * @return RC 
 */
extern RC next (RM_ScanHandle *sHandle, Record *record)
{
	// initialize schema
	Schema *schema = sHandle->rel->schema;
	// get record size
	int rSize = getRecordSize(schema);
	// declare and initialize no of slots
	int slots = PAGE_SIZE / rSize;
	RecordManager *scan_mgr = sHandle->mgmtData;
	RecordManager *tb_mgr = sHandle->rel->mgmtData;
    // number of scans
	int num_scan = scan_mgr->num_of_scan;	
	// iterate over all scans 	
	while(num_scan <= tb_mgr->num_of_tuples){  
		if (num_scan <= 0){
			// assign record id page with value one
			scan_mgr->record_id.page = ONE;
			// assign record id slot with value zero
			scan_mgr->record_id.slot = ZERO;
		}
		else{
			scan_mgr->record_id.slot++;
			if(scan_mgr->record_id.slot >= slots){
				// assign record id slot with value zero
				scan_mgr->record_id.slot = ZERO;
				scan_mgr->record_id.page++;
			}
		}
        // pin page
		pinPage(&tb_mgr->buffer_pool, &scan_mgr->page_handle, scan_mgr->record_id.page);
		// initialize data pointer
		char *data = scan_mgr->page_handle.data + (scan_mgr->record_id.slot * rSize);
		char *dp = record->data;
		*dp = '-';
		// copy memory
		memcpy(++dp, data + 1, rSize - 1);
		// increment scan manager num_scan
		scan_mgr->num_of_scan++;
		// allocate memory to res pointer
		Value *res = (Value *) malloc(sizeof(Value));
		// evaluate expression 
		evalExpr(record, schema, scan_mgr->condition, &res); 
		if(res->v.boolV == TRUE){	
			// unpin page
			unPinPage(&tb_mgr->buffer_pool, &scan_mgr->page_handle);
			return RC_OK;
		}
		// increment scan counter
		num_scan++;
	}

    // clear slot and scans
	scan_mgr->record_id.slot = scan_mgr->num_of_scan = 0;
	scan_mgr->record_id.page = 1;
	
	return RC_RM_NO_MORE_TUPLES;
}

/**
 * @brief Close scan
 * 
 * @param scan 
 * @return RC 
 */
extern RC closeScan (RM_ScanHandle *sHandle)
{
	// initialize helper variable 
	RecordManager *scan_mgr = sHandle->mgmtData;
	RecordManager *record_mgr = sHandle->rel->mgmtData;
	// check whether number of scan are more than zero
	if(scan_mgr->num_of_scan > 0){
		// unpin page
		unPinPage(&record_mgr->buffer_pool, &scan_mgr->page_handle);
		// clear slot and number of scans
		scan_mgr->num_of_scan = scan_mgr->record_id.slot = ZERO;
		scan_mgr->record_id.page = ONE;
	}
    sHandle->mgmtData = NULL;
	// free mgmt data in scan handle
    free(sHandle->mgmtData);  
	// return success
	return RC_OK;
}

/***************************************************************
******************   Schemas Functions    **********************
***************************************************************/


/**
 * @brief Create a Schema object
 * 
 * @param numAttr 
 * @param attrNames 
 * @param dataTypes 
 * @param typeLength 
 * @param keySize 
 * @param keys 
 * @return Schema* 
 */
extern Schema *createSchema (int attr_num, char **attr_names, DataType *data_types, int *type_length, int key_size, int *keys){

	// Allocate memory
	Schema *schema = (Schema*) malloc(sizeof(Schema));

	// Assign attributes	
	schema->attrNames = attr_names;
	schema->keySize = key_size;
	schema->dataTypes = data_types;
	schema->numAttr = attr_num;
	schema->keyAttrs = keys;
	schema->typeLength = type_length;
	
	// Return Schema
	return schema;

}


extern RC freeSchema (Schema *schema){

	// Free the memory
	free(schema);

	// Return success code
	return RC_OK;

}

/**
 * @brief Helper function to return attribute offset
 * 
 * @param schema 
 * @param numAttr 
 * @param result 
 * @return RC 
 */
RC attrOffset (Schema *schema, int numAttr, int *result)
{
	DataType dt;
	if (numAttr == 1){
        dt = 1;
	}
	*result = 1;
	for(int i=0; i<numAttr; i++){

		dt = schema->dataTypes[i];

		if (dt == DT_INT) {

			*result = *result + sizeof(int);;

		} else if (dt == DT_STRING) {

			*result = *result + schema->typeLength[i];

		} else if (dt == DT_FLOAT) {

			*result = *result + sizeof(float);

		} else if (dt == DT_BOOL){

			*result = *result + sizeof(bool);

		}
	}
	
	return RC_OK;
}

/**
 * @brief Free the record in memory
 * 
 * @param record 
 * @return RC 
 */
extern RC freeRecord (Record *record)
{
	// Free the memory
	free(record->data);
	free(record);

	// Return success code
	return RC_OK;
}

/***************************************************************
************   Attribute Functions  ****************************
***************************************************************/

/**
 * @brief Get the Attr object
 * 
 * @param record 
 * @param schema 
 * @param attrNum 
 * @param value 
 * @return RC 
 */
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;
	attrOffset(schema, attrNum, &offset);
	// initialize src pointer with reord data and offset
	char *src = record->data + offset;
    // initialize data type with schema datatypes
	DataType dt = schema->dataTypes[attrNum];
	Value *val = (Value*) malloc(sizeof(Value));
	// check if number of attributes equl to one 
	if (attrNum == 1){
        dt = 1;
	}
    // handle string data type
	if (dt == DT_STRING){
        char end = '\0';
		// schema length
		int len = schema->typeLength[attrNum];
		// allocate memory 
		void *dest = (char*) malloc(len+1);
		val->v.stringV = dest;
		// copy memory 
		strncpy(dest, src, len);
		// mark end 
		val->v.stringV[len] = end;
		val->dt = DT_STRING;
	}
	// handle int data type
	else if (dt == DT_INT) {
		int value = 0;
		// copy memory
		memcpy(&value, src, sizeof(int));
		val->v.intV = value;
		val->dt = DT_INT;
	}
	// handle float data type 
	else if (dt == DT_FLOAT){
	  	float value;
		// copy memory 
	  	memcpy(&value, src, sizeof(float));
		val->dt = DT_FLOAT;
	}
	// handle boolean data type
	else if (dt == DT_BOOL){
		bool value;
		// copy memory 
		memcpy(&value,src, sizeof(bool));
		val->v.boolV = value;
		val->dt = DT_BOOL;
	}

	*value = val;
	return RC_OK;
}

/**
 * @brief Set the Attr object
 * 
 * @param record 
 * @param schema 
 * @param attrNum 
 * @param value 
 * @return RC 
 */
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{

	// Helper variables
	int offset = 0;
	attrOffset(schema, attrNum, &offset);
	// initialize src pointer with reord data and offset
	char *dest = record->data + offset;
	// initialize data type with schema datatypes
	DataType dt = schema->dataTypes[attrNum];
    // handle int datatype
	if (dt == DT_INT) {
        *(int *) dest = value->v.intV;	  
		dest = dest + sizeof(int);

	} 
	// handle string datatype
	else if (dt == DT_STRING) {

		int len = schema->typeLength[attrNum];
		strncpy(dest, value->v.stringV, len);
		dest += len;

	} 
	// handle float datatype
	else if (dt == DT_FLOAT) {

		*(float *) dest = value->v.floatV;
		dest += sizeof(float);

	} 
	// handle boolean datatype
	else if (dt == DT_BOOL){
        *(bool *) dest = value->v.boolV;
		dest += sizeof(bool);

	}

	// return success		
	return RC_OK;
}

/**
 * @brief Helper function to calculate offset
 * 
 * @param schema 
 * @param attrNum 
 * @return int 
 */
int getOffset(Schema *schema, int attrNum) {

	// Helper variables
	int offset = 0;
	DataType dt;

	// Calculate offset
	for (int i=0; i < attrNum; i++) {

		dt = schema->dataTypes[i];

		if (dt == DT_INT) {

			offset += sizeof(int);

		} else if (dt == DT_STRING) {

			offset += schema->typeLength[i];

		} else if (dt == DT_FLOAT) {

			offset += sizeof(float);

		} else if (dt == DT_BOOL){

			offset += sizeof(bool);

		}
	}

	// Return calculated offset
	return offset;
}