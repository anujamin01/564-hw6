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
	int len = 0;
	int i = 0;

	while (i < projCnt)
	{
		// fills up descriptions of all projections
		if (attrCat->getInfo(projNames[i].relName, projNames[i].attrName, descs[i]) != OK)
		{
			return attrCat->getInfo(projNames[i].relName, projNames[i].attrName, descs[i]);
		}
		len += descs[i].attrLen;
		i++;
	}
	AttrDesc *attrPtr = NULL;
	const char *filterType;

	// SELECT ALL condition
	if (attr != NULL)
	{
		attrPtr = new AttrDesc;
		// fill up attrPtr to be passed into scanselect
		if (attrCat->getInfo(attr->relName, attr->attrName, *attrPtr) != OK)
		{
			return attrCat->getInfo(attr->relName, attr->attrName, *attrPtr);
		}

		// type conversion to pass in attrType parameter for scan select function
		int val;
		float fval;
		if (attr->attrType == STRING)
		{
			filterType = attrValue;
		}
		else if (attr->attrType == INTEGER)
		{
			val = atoi(attrValue);
			filterType = (char *)&val;
		}
		else if (attr->attrType == FLOAT)
		{
			fval = atof(attrValue);
			filterType = (char *)&fval;
		}
	}

	// scanselect given all the relevant data
	return ScanSelect(result, projCnt, descs, attrPtr, op, filterType, len);
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
	// start a new hfs
	HeapFileScan *hfs = new HeapFileScan(projNames[0].relName, status);

	// if we couldn't start scan
	if (status != OK)
	{
		delete hfs;
		return status;
	}
	// no metadata to work with start empty scan
	if (attrDesc == NULL)
	{
		// start scan with default null values
		if (hfs->startScan(0, 0, STRING, NULL, EQ) != OK)
		{
			delete hfs;
			return hfs->startScan(0, 0, STRING, NULL, EQ);
		}
	}
	else
	{
		// start scan with attrDesc
		if (hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op) != OK)
		{
			delete hfs;
			return hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
		}
	}

	while (hfs->scanNext(rid) == OK)
	{
		// get record and populate into r,
		if (hfs->getRecord(r) != OK)
		{
			break;
		}
		// found a match
		attrInfo attrs[projCnt];
		int i = 0;
		while (i < projCnt)
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

			// copy the ACTUAL attribute value into attrs.attrValue
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
			i++;
		}
		// call QU_Insert to insert the record into the temporary table or the table specified by the user
		if (QU_Insert(result, projCnt, attrs) != OK)
		{
			delete hfs;
			return  QU_Insert(result, projCnt, attrs);
		}
	}

	return OK;
}
