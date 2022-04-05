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

	return RC_OK;

}


extern RC shutdownRecordManager (){

	return RC_OK;

}


extern RC createTable (char *name, Schema *schema){

	// do things


	return RC_OK;

}


extern RC openTable (RM_TableData *rel, char *name);
extern RC closeTable (RM_TableData *rel);
extern RC deleteTable (char *name);
extern int getNumTuples (RM_TableData *rel);

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record);
extern RC deleteRecord (RM_TableData *rel, RID id);
extern RC updateRecord (RM_TableData *rel, Record *record);
extern RC getRecord (RM_TableData *rel, RID id, Record *record);

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond);
extern RC next (RM_ScanHandle *scan, Record *record);
extern RC closeScan (RM_ScanHandle *scan);

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

	return RC_OK;

}

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema){

	// Allocate memory
	int rSize = getRecordSize(schema);
	*record = (Record*) malloc(Record);
	(*record)->data = (char*) malloc(rSize);

	return RC_OK;

}


extern RC freeRecord (Record *record){

	// Free the memory
	free(record->data);
	free(record);

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
