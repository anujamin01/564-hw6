#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"

// forward declaration
const Status ScanSelect(const string &result,
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

const Status QU_Select(const string &result,
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
	int len = 0;
	while (i < projCnt)
	{
		// fills up descriptions of all projections
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, descs[i]);
		if (status != OK)
		{
			return status;
		}
		i++;
		len += descs[i].attrLen;
	}
	AttrDesc *attrPtr = NULL;
	// SELECT ALL condition
	if (attr != NULL)
	{
		attrPtr = new AttrDesc;
		// fill up attrPtr to be passed into scanselect
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrPtr);
		if (status != OK)
		{
			return status;
		}
	}

	// scanselect given all the relevant data
	status = ScanSelect(result, projCnt, descs, attrPtr, op, attrValue, len);
	return status;
}

const Status ScanSelect(const string &result,
						const int projCnt,
						const AttrDesc projNames[],
						const AttrDesc *attrDesc,
						const Operator op,
						const char *filter,
						const int reclen)
{
	cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Record r;
	RID rid;
	Status status;
	HeapFileScan *hfs = new HeapFileScan(projNames[0].relName, status);

	// couldn't start scan
	if (status != OK)
	{
		delete hfs;
		return status;
	}
	// no metadata to work with start empty scan
	if (attrDesc == NULL)
	{
		status = hfs->startScan(0, 0, STRING, NULL, EQ);
		if (status != OK)
		{
			delete hfs;
			return status;
		}
	}
	else
	{
		// start scan
		int val;
		float fval;

		if (attrDesc->attrType == STRING)
		{
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
		}
		else if (attrDesc->attrType == INTEGER)
		{
			val = atoi(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char *)&val, op);
		}
		else if (attrDesc->attrType == FLOAT)
		{
			fval = atof(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char *)&fval, op);
		}
		if (status != OK)
		{
			delete hfs;
			return status;
		}
	}

	while ((status = hfs->scanNext(rid)) == OK)
	{
		status = hfs->getRecord(r);
		if (status != OK)
		{
			break;
		}
		attrInfo attrs[projCnt];

		for (int i = 0; i < projCnt; i++)
		{
			// grab metadata for current projection
			AttrDesc tempAttrDesc = projNames[i];

			// Use memcpy to copy each attribute in the record into a list of attrInfo objects. 
			strcpy(attrs[i].relName, tempAttrDesc.relName);
			strcpy(attrs[i].attrName, tempAttrDesc.attrName);
			attrs[i].attrType = tempAttrDesc.attrType;
			attrs[i].attrLen = tempAttrDesc.attrLen;

			attrs[i].attrValue = (void *)malloc(tempAttrDesc.attrLen);

			int val;
			float fval;
			
			// copy the actual attribute value into attrs.attrValue
			if (attrs[i].attrType == STRING)
			{
				memcpy((char *)attrs[i].attrValue, (char *)(r.data + tempAttrDesc.attrOffset), tempAttrDesc.attrLen);
			}
			else if (attrs[i].attrType == INTEGER)
			{
				memcpy(&val, (int *)(r.data + tempAttrDesc.attrOffset), tempAttrDesc.attrLen);
				// put that int value into the attrValue 
				sprintf((char *)attrs[i].attrValue, "%d", val);
			}
			else if (attrs[i].attrType == FLOAT)
			{
				memcpy(&fval, (float *)(r.data + tempAttrDesc.attrOffset), tempAttrDesc.attrLen);
				// put that float value into the attrValue 
				sprintf((char *)attrs[i].attrValue, "%f", fval);
			}
		}
		// call QU_Insert to insert the record into the temporary table or the table specified by the user 
		status = QU_Insert(result, projCnt, attrs);
		if (status != OK)
		{
			delete hfs;
			return status;
		}
	}

	return OK;
}
