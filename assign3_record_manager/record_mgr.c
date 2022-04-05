#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dberror.h"
#include "expr.h"
#include "tables.h"

// Bookkeeping for scans
// typedef struct RM_ScanHandle
// {
// 	RM_TableData *rel;
// 	void *mgmtData;
// } RM_ScanHandle;

// table and manager
extern RC initRecordManager (void *mgmtData) {

	// Return success code
	return RC_OK;

}


extern RC shutdownRecordManager (){

	// Return success code
	return RC_OK;

}


extern RC createTable (char *name, Schema *schema){

	// Helper variables
	SM_FileHandle fileH;
	int res;

	// Create the page file
	res = createPageFile(name);

	if (res == RC_OK) {

		// Open the newly created page file
		res = openPageFile(name, &fileH);

		if (res == RC_OK){

			// Check capacity of first page
			res = ensureCapacity(1, &fileH);

			if (res == RC_OK){

				// Write serialized schema to first page
				writeBlock(0, &fileH, &(serializeSchema(schema)));

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

// *************----->NOT FINISHED<-------**************
extern RC openTable (RM_TableData *rel, char *name){

	// Helper Variables
	BM_PageHandle *ph = (BM_PageHandle*) malloc(sizeof(BM_PageHandle));
	BM_BufferPool *bp = (BM_BufferPool*) malloc(sizeof(BM_BufferPool));

	// Init the buffer pool *****************CHECK ARGS****************
	initBufferPool(bp, name, _, RS_LRU, NULL);


	//Assign TableData attributes
	rel->name = name;
	//rel->schema = ....
	rel->mgmtData = bp;

	// Free the memory
	free(ph);

	// Return success code
	return RC_OK;

}


extern RC closeTable (RM_TableData *rel){

	// shut down the buffer BM_BufferPool
	void* mgmtData = rel->mgmtData;
	shutdownBufferPool(mgmtData);

	// Free all allocated memory
	Schema* schema = rel->schema;
	free(schema->attrNames);
	free(schema->dataTypes);
	free(schema->typeLength);
	free(schema->keyAttrs);
	free(schema);
	free(rel->name);								// NOT SURE IF NEEDED
	free(rel->mgmtData);						// NOT SURE IF NEEDED

	// Return success code
	return RC_OK;

}


extern RC deleteTable (char *name){

	// destroy page file with given name
	int res = destroyPageFile(name);

	// Return success or error code
	return res;

}


// *************----->NOT FINISHED<-------**************
extern int getNumTuples (RM_TableData *rel){

	// Helper Variables
	int tuples = 0;



	// Return number of tuples
	return tuples;

}

// handling records in a table
// *************----->NOT FINISHED<-------**************
extern RC insertRecord (RM_TableData *rel, Record *record){

	// Return success code
	return RC_OK;

}


// *************----->NOT FINISHED<-------**************
extern RC deleteRecord (RM_TableData *rel, RID id){


	// Return success code
	return RC_OK;

}


// *************----->NOT FINISHED<-------**************
extern RC updateRecord (RM_TableData *rel, Record *record){


	// Return success code
	return RC_OK;

}


// *************----->NOT FINISHED<-------**************
extern RC getRecord (RM_TableData *rel, RID id, Record *record) {


	// Return success code
	return RC_OK;

}

// scans
// typedef struct RM_ScanHandle
// {
// 	RM_TableData *rel;
// 	void *mgmtData;
// } RM_ScanHandle;

// *************----->NOT FINISHED<-------**************
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){


	// Return success code
	return RC_OK;

}


// *************----->NOT FINISHED<-------**************
extern RC next (RM_ScanHandle *scan, Record *record){

	// Return success code
	return RC_OK;

}


extern RC closeScan (RM_ScanHandle *scan){

	// Return success code
	return RC_OK;

}

// dealing with schemas
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
	return rSize;

}


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

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema){

	// Allocate memory
	int rSize = getRecordSize(schema);
	*record = (Record*) malloc(Record);
	(*record)->data = (char*) malloc(rSize);

	// Return success code
	return RC_OK;

}


extern RC freeRecord (Record *record){

	// Free the memory
	free(record->data);
	free(record);

	// Return success code
	return RC_OK;

}


extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){

	// Helper variables
	int offset = getOffset(schema, attrNum);
	char *src = record->data + offset;
	DataType dt = schema->dataTypes[attrNum];
	*value = (Value*)malloc(sizeof(Value));
	(*value)->dt = src;

	if (dt == DT_INT) {

		void *dest = value->v.intV
		memcpy(dest, src, sizeof(int));

	} else if (dt == DT_STRING) {

		char end = '\0';
		int length = schema->typeLength[attrNum];
		void* dest = (char*) malloc(length+1);
		strncopy(dest, src, length);
		dest[length] = end;
		(*value)->v.stringV = dest;

	} else if (dt == DT_FLOAT) {

		void *dest = value->v.floatV
		memcpy(dest, src, sizeof(float));

	} else if (dt == DT_BOOL){

		void *dest = value->v.boolV
		memcpy(dest, src, sizeof(bool));

	}

	// Return success code
	return RC_OK;

}


extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){

	// Helper variables
	int offset = getOffset(schema, attrNum);
	char *dest = record->data + offset;
	DataType dt = schema->dataTypes[attrNum];

	if (dt == DT_INT) {

		void *src = value->v.intV
		memcpy(dest, src, sizeof(int));

	} else if (dt == DT_STRING) {

		int length = schema->typeLength[attrNum];
		char *src = (char*) malloc(length);
		src = value->v.stringV;
		memcpy(dest, src, length);

	} else if (dt == DT_FLOAT) {

		void *src = value->v.floatV
		memcpy(dest, src, sizeof(float));

	} else if (dt == DT_BOOL){

		void *src = value->v.boolV
		memcpy(dest, src, sizeof(bool));

	}

	// Return success code
	return RC_OK;
}

// Helper function to calculate offset
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
