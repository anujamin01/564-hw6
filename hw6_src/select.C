#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
		Status status;
		// array to hold descriptions of all projections
		AttrDesc descs[projCnt];

		// get the needed information for each projection
		int i = 0;
		while (i < projCnt){
			status = attrCat->getInfo(descs[i].relName, descs[i].attrName, descs[i]);
			if (status != OK){
				return status;
			}
			i++;
		}
		int len = 0;
		AttrDesc* attrPtr = NULL;

		if (attr != NULL){
			attrPtr = new AttrDesc;
			status = attrCat->getInfo(attr->relName, attr->attrName, *attrPtr);
			if (status != OK){
				return status;
			}
			len = attrPtr->attrLen;
		}

		// scan and find
		return ScanSelect(result, projCnt, descs, attrPtr,op,attrValue, len);
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;


}
