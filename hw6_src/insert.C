#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	int attributeCounter;
	AttrDesc *attributeDescPtr;
	// use attrCat->getRelInfo to get the attributes in order and their descriptions

	Status status = attrCat->getRelInfo(relation, attributeCounter, attributeDescPtr);
	if (status != OK){
		return status;
	}
	// if the parameter attrCnt is wrong
	if (attributeCounter != attrCnt){
		return UNIXERR;
	}
	
// part 6
return OK;

}

