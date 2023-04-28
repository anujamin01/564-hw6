#include "catalog.h"
#include "query.h"

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
	while (i < projCnt)
	{
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, descs[i]);
		if (status != OK)
		{
			return status;
		}
		i++;
	}
	int len = 0;
	AttrDesc *attrPtr = NULL;

	if (attr != NULL)
	{
		attrPtr = new AttrDesc;
		status = attrCat->getInfo(attr->relName, attr->attrName, *attrPtr);
		if (status != OK)
		{
			return status;
		}
		len = attrPtr->attrLen;
	}

	// scan and find
	status = ScanSelect(result, projCnt, descs, attrPtr, op, attrValue, len);
	return status;
}

const Status ScanSelect(const string &result,
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
		if (attrDesc == NULL)
		{
			status = hfs->startScan(0, 0, STRING, NULL, EQ);
		}
		else if (attrDesc->attrType == STRING)
		{
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
		}
		else if (attrDesc->attrType == INTEGER)
		{
			int val = atoi(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char *)&val, op);
		}
		else if (attrDesc->attrType == FLOAT)
		{
			float fval = atof(filter);
			status = hfs->startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (char *)&fval, op);
		}
		if (status != OK)
		{
			delete hfs;
			return status;
		}
	}

	while (true)
	{
		status = hfs->scanNext(rid);
		if (status == OK)
		{
			status = hfs->getRecord(r);
			if (status != OK)
			{
				break;
			}
			attrInfo attrs[projCnt];

			for (int i = 0; i < projCnt; i++)
			{
				// grab metadata for current project
				AttrDesc attrDesc = projNames[i];

				// copy metadata info over
				strcpy(attrs[i].relName, attrDesc.relName);
				strcpy(attrs[i].attrName, attrDesc.attrName);
				attrs[i].attrType = attrDesc.attrType;
				attrs[i].attrLen = attrDesc.attrLen;

				attrs[i].attrValue = (void *)malloc(attrDesc.attrLen);

				if (attrs[i].attrType == STRING)
				{
					memcpy((char *)attrs[i].attrValue, (char *)(r.data + attrDesc.attrOffset), attrDesc.attrLen);
				}
				else if (attrs[i].attrType == INTEGER)
				{
					int val = 0;
					memcpy(&val, (int *)(r.data + attrDesc.attrOffset), attrDesc.attrLen);
					sprintf((char *)attrs[i].attrValue, "%d", val);
				}
				else if (attrs[i].attrType == FLOAT)
				{
					float fval;
					memcpy(&fval, (float *)(r.data + attrDesc.attrOffset), attrDesc.attrLen);
					sprintf((char *)attrs[i].attrValue, "%f", fval);
				}
			}

			status = QU_Insert(result, projCnt, attrs);
			if (status != OK)
			{
				delete hfs;
				return status;
			}
		}
		else
		{
			break;
		}
	}

	return OK;
}
