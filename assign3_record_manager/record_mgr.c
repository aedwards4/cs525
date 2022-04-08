#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

typedef struct RecordManager
{
	int num_of_tuples;
	int free_page;
	int num_of_scan;
	BM_PageHandle pageHandle;
	BM_BufferPool bufferPool;
	RID record_id;
	Expr *condition;
} RecordManager;

const int MAX_PAGES = 100;
const int ATTRIBUTE_SIZE = 15;

RecordManager *record_mgr;

int findFreeSlot(char *data, int record_size)
{
	int total_slots = PAGE_SIZE / record_size;
	int i = 0;
	while(i < total_slots ){
		if (data[i * record_size] != '+')
		   return i;
		i++;
	}

	return -1;
}

/***************************************************************
/***************  Table and Manager Functions  *****************
***************************************************************/

extern RC initRecordManager (void *mgmtData) {
	// Return success code
	return RC_OK;
}

extern RC shutdownRecordManager (){
	// Return success code
	return RC_OK;
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
	int res;
	record_mgr = (RecordManager*) malloc(sizeof(RecordManager));
	initBufferPool(&record_mgr->bufferPool, name, MAX_PAGES, RS_LRU, NULL);
	char data[PAGE_SIZE];
	char *pageHandle = data;

	*(int*)pageHandle = 0;
	pageHandle = pageHandle + sizeof(int);

	*(int*)pageHandle = 1;
	pageHandle = pageHandle + sizeof(int);

	*(int*)pageHandle = schema->numAttr;
	pageHandle = pageHandle + sizeof(int);

	*(int*)pageHandle = schema->keySize;
	pageHandle = pageHandle + sizeof(int);

	int i = 0;
	while(i < schema->numAttr){
		strncpy(pageHandle, schema->attrNames[i], ATTRIBUTE_SIZE);
	    pageHandle = pageHandle + ATTRIBUTE_SIZE;

	    *(int*)pageHandle = (int)schema->dataTypes[i];
	    pageHandle = pageHandle + sizeof(int);

	    *(int*)pageHandle = (int) schema->typeLength[i];
	    pageHandle = pageHandle + sizeof(int);
		i++;
	}

	SM_FileHandle fileH;

    res = createPageFile(name);
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
	// Helper Variables
	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
	BM_BufferPool *bp = (BM_BufferPool*) malloc(sizeof(BM_BufferPool));

	// Init the buffer pool
	int bufferSize = 10;			//******POSSIBLY NEEDS TO BE CHANGED
	initBufferPool(bp, name, bufferSize, RS_LRU, NULL);

	// Pin the page handle to the buffer pool
	pinPage(bp, ph, 0);

	//Assign TableData attributes
	rel->name = name;
	rel->mgmtData = bp;

	// Initialize the Schema and assign to attribute
	char *pgData = ph->data;
	Schema *schema = schemaHelper(pgData);
	rel->schema = schema;

	// Free the memory
	free(ph);

	// Unpin the page handle
	unPinPage(bp, ph);			// ********NOT SURE IF NEEDED

	// Return success code
	return RC_OK;
}


// *******NOT FINISHED
// Helper function that deserializes the given string to create a new Schema
Schema* schemaHelper(char *pgData){

	// Allocate the memory & copy the pgData into array
	Schema *schema = (Schema*) malloc(sizeof(schema));
	int len = strlen(pgData);
	char data[len];
	strcopy(data,pgData);

	// Capture data from given string
	//APPEND(result, "Schema with <%i> attributes (", schema->numAttr);
	char *temp = strtok(data,"<");
	temp = strtok(data,">");
	int numAttr = atoi(temp);
	temp = strtok(data,"(");

	// Helper variables for Schema attributes
	char **attrNames = (char**) malloc(sizeof(char*) * numAttr);
	DataType *dataTypes = (DataType*) malloc(sizeof(DataType) * numAttr);
	int *typeLength = (int*) malloc(sizeof(int) * numAttr);
	int *keyAttrs;
	int keySize;

	int i;
	for (i=0; i<numAttr; i++){

		// do things

	}


	// **************INCOMPLETE***************


	// Create and return the schema
	Schema *newSchema = createSchema(numAttr,attrNames,dataTypes,typeLength,keySize,keys);
	return newSchema;

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
	RecordManager *mgmtData = rel->mgmtData;
	shutdownBufferPool(&mgmtData->bufferPool);

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

	// Helper Variables
	int tuples = 0;

	// Get number of records from RecordManager
	RecordManager *record_mgr = rel->mgmtData;
	tuples = record_mgr->num_of_tuples;

	// Return number of tuples
	return tuples;
}

/***************************************************************
/********************  Record Functions  ***********************
***************************************************************/

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
	pinPage(&record_mgr->bufferPool, &record_mgr->pageHandle, record_id->page);
	// create record data
	char *record_data = record_mgr->pageHandle.data;
	record_id->slot = findFreeSlot(record_data, record_size);
	// Loop with page and slot
	while(record_id->slot == -1){
		// unpin the page as now it has been used
		unpinPage(&record_mgr->bufferPool, &record_mgr->pageHandle);
		record_id->page++;
		// pin the record page, to mark that it is in use
		pinPage(&record_mgr->bufferPool, &record_mgr->pageHandle, record_id->page);
		record_data = record_mgr->pageHandle.data;
		record_id->slot = findFreeSlot(record_data, record_size);
	}
	// mark page as used
	markDirty(&record_mgr->bufferPool, &record_mgr->pageHandle);
	char *pos = record_data + (record_id->slot * record_size);
	*pos = '+';
	// insert the new record data, into the Table i.e. Pages of the PageFile
	memcpy(++pos, record->data + 1, record_size - 1);
	// unpin the page as now it has been used
	unpinPage(&record_mgr->bufferPool, &record_mgr->pageHandle);
	record_mgr->num_of_tuples++;
	// pin the record page, to mark that it is in use
	pinPage(&record_mgr->bufferPool, &record_mgr->pageHandle, 0);
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
	pinPage(&record_mgr->bufferPool, &record_mgr->pageHandle, record->id.page);
	char *record_data; // declare new record variable
	// add data to new record
	record_data = record_mgr->pageHandle.data + (record->id.slot * getRecordSize(one_rel->schema));
	// append record
	*record_data = '+';
	// copy data into new record
	memcpy(++record_data, record->data + 1, getRecordSize(one_rel->schema) - 1 );
	// mark record as dirty
	markDirty(&record_mgr->bufferPool, &record_mgr->pageHandle);
	// unpin record and leave
	unpinPage(&record_mgr->bufferPool, &record_mgr->pageHandle);

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
	pinPage(&record_mgr->bufferPool, &record_mgr->pageHandle, id.page);
	record_mgr->free_page = id.page;
	// add record to be deleted to record dta
	char *record_data = record_mgr->pageHandle.data +
	                    (id.slot * getRecordSize(one_rel->schema));
    // delete record
	*record_data = '-';
	// mark page as dirty
	markDirty(&record_mgr->bufferPool, &record_mgr->pageHandle);
	// unpin page and leave
	unpinPage(&record_mgr->bufferPool, &record_mgr->pageHandle);
	return RC_OK;
}

/**
 * @brief Get the Record object
 *
 * @param rel
 * @param id
 * @param record
 * @return RC
 */
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	// implementation goes here
	return RC_OK;
}

/***************************************************************
/******************   Scan Functions    ************************
***************************************************************/
/**
 * @brief
 *
 * @param rel
 * @param scan
 * @param cond
 * @return RC
 */
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    // implementation goes here
	return RC_OK;
}

/**
 * @brief
 *
 * @param scan
 * @param record
 * @return RC
 */
extern RC next (RM_ScanHandle *scan, Record *record)
{
	RecordManager *scanManager = scan->mgmtData;
	RecordManager *tableManager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;
	if (scanManager->condition == NULL){
		return RC_SCAN_CONDITION_NOT_FOUND;
	}
	Value *result = (Value *) malloc(sizeof(Value));

	char *data;
	int recordSize = getRecordSize(schema);
	int totalSlots = PAGE_SIZE / recordSize;
	int num_of_scan = scanManager->num_of_scan;
	int num_of_tuples = tableManager->num_of_tuples;
	if (num_of_tuples == 0)
		return RC_RM_NO_MORE_TUPLES;

	while(num_of_scan <= num_of_tuples){
		if (num_of_scan <= 0){
			scanManager->record_id.page = 1;
			scanManager->record_id.slot = 0;
		}
		else{
			scanManager->record_id.slot++;

			if(scanManager->record_id.slot >= totalSlots){
				scanManager->record_id.slot = 0;
				scanManager->record_id.page++;
			}
		}

		pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->record_id.page);
		data = scanManager->pageHandle.data;
		data = data + (scanManager->record_id.slot * recordSize);
		record->id.page = scanManager->record_id.page;
		record->id.slot = scanManager->record_id.slot;
		char *dataPointer = record->data;
		*dataPointer = '-';
		memcpy(++dataPointer, data + 1, recordSize - 1);
		scanManager->num_of_scan++;
		num_of_scan++;
		evalExpr(record, schema, scanManager->condition, &result);
		if(result->v.boolV == TRUE){
			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
			return RC_OK;
		}
	}
	unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
	scanManager->record_id.page = 1;
	scanManager->record_id.slot = 0;
	scanManager->num_of_scan = 0;
	return RC_RM_NO_MORE_TUPLES;
}

/**
 * @brief
 *
 * @param scan
 * @return RC
 */
extern RC closeScan (RM_ScanHandle *scan)
{
	RecordManager *scanManager = scan->mgmtData;
	RecordManager *record_mgr = scan->rel->mgmtData;
	if(scanManager->num_of_scan > 0){
		unpinPage(&record_mgr->bufferPool, &scanManager->pageHandle);
		scanManager->num_of_scan = 0;
		scanManager->record_id.page = 1;
		scanManager->record_id.slot = 0;
	}
    	scan->mgmtData = NULL;
    	free(scan->mgmtData);
	return RC_OK;
}

/***************************************************************
/***********   Schemas Functions    ****************************
***************************************************************/

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
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){

	// Allocate memory
	Schema *schema = (Schema*) malloc(sizeof(Schema));

	// Assign attributes
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keySize = keySize;
	schema->keyAttrs = keys;

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
 * @brief Create a Record object
 *
 * @param record
 * @param schema
 * @return RC
 */
extern RC createRecord (Record **record, Schema *schema)
{
	// Allocate memory
	int rSize = getRecordSize(schema);
	*record = (Record*) malloc(sizeof(Record));
	(*record)->data = (char*) malloc(rSize);
	(*record)->id.page = (*record)->id.slot = -1;
	char *data_ptr = (*record)->data;
	*data_ptr = '-';
	*(++data_ptr) = '\0';

	// Return success code
	return RC_OK;
}

/**
 * @brief
 *
 * @param schema
 * @param attrNum
 * @param result
 * @return RC
 */
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i;
	*result = 1;
	for(i = 0; i < attrNum; i++){
		switch (schema->dataTypes[i]){
			case DT_STRING:
				*result = *result + schema->typeLength[i];
				break;
			case DT_INT:
				*result = *result + sizeof(int);
				break;
			case DT_FLOAT:
				*result = *result + sizeof(float);
				break;
			case DT_BOOL:
				*result = *result + sizeof(bool);
				break;
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
/***********   Attribute Functions  ****************************
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
	Value *val = (Value*) malloc(sizeof(Value));
	char *ptr = record->data;
	ptr += offset;
	DataType dt = schema->dataTypes[attrNum];
	if (attrNum == 1){
        dt = 1;
	}

	if (dt == DT_STRING){
		int len = schema->typeLength[attrNum];
		val->v.stringV = (char *) malloc(len + 1);
		strncpy(val->v.stringV, ptr, len);
		val->v.stringV[len] = '\0';
		val->dt = DT_STRING;
	}
	else if (dt == DT_INT) {
		int value = 0;
		memcpy(&value, ptr, sizeof(int));
		val->v.intV = value;
		val->dt = DT_INT;
	}
	else if (dt == DT_FLOAT){
	  	float value;
	  	memcpy(&value, ptr, sizeof(float));
	  	val->v.floatV = value;
		val->dt = DT_FLOAT;
	}
	else if (dt == DT_BOOL){
		bool value;
		memcpy(&value,ptr, sizeof(bool));
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
	char *dest = record->data + offset;

	DataType dt = schema->dataTypes[attrNum];

	if (dt == DT_INT) {
        *(int *) dest = value->v.intV;
		dest = dest + sizeof(int);

	} else if (dt == DT_STRING) {

		int len = schema->typeLength[attrNum];
		strncpy(dest, value->v.stringV, len);
		dest += len;

	} else if (dt == DT_FLOAT) {

		*(float *) dest = value->v.floatV;
		dest += sizeof(float);

	} else if (dt == DT_BOOL){
        *(bool *) dest = value->v.boolV;
		dest += sizeof(bool);

	}

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
	int i = 0;
	int offset = 0;
	DataType dt;

	// Calculate offset
	for (i=0; i < attrNum; i++) {

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
