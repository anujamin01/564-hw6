#include "catalog.h"
#include "query.h"

/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string &relation,
					   const int attrCnt,
					   const attrInfo attrList[])
{
	int attributeCounter;
	AttrDesc *attributeDescPtr;
	// use attrCat->getRelInfo to get the attributes in order and their descriptions

	Status status = OK;
	if (attrCat->getRelInfo(relation, attributeCounter, attributeDescPtr) != OK)
	{
		return  attrCat->getRelInfo(relation, attributeCounter, attributeDescPtr);
	}
	// if the parameter attrCnt is wrong
	if (attributeCounter != attrCnt)
	{
		return UNIXERR;
	}

	// grab total length of what to insert
	int relLen = 0;
	int num = 0;
	while (num < attrCnt)
	{
		relLen += attributeDescPtr[num].attrLen;
		num ++; 
	}

	InsertFileScan ifs(relation, status);
	if (status != OK)
	{
		return status;
	}

	char *insertInfo;
	if (!(insertInfo = new char[relLen]))
	{
		return INSUFMEM;
	}

	int offset = 0;
	// add all the attribute info to our buffer in order
	int i = 0;
	while (i < attrCnt)
	{
		int j = 0;
		while (j < attributeCounter)
		{
			// found matching attribute
			if (strcmp(attributeDescPtr[i].attrName, attrList[j].attrName) == 0)
			{
				offset = attributeDescPtr[i].attrOffset;
				if (attrList[j].attrType == STRING)
				{
					memcpy((char *)insertInfo + offset, (char *)attrList[j].attrValue, attributeDescPtr[i].attrLen);
				}
				else if (attrList[j].attrType == INTEGER)
				{
					int val = atoi((char *)attrList[j].attrValue);
					memcpy((char *)insertInfo + offset, &val, attributeDescPtr[i].attrLen);
				}
				else if (attrList[j].attrType == FLOAT)
				{
					float fval = atof((char *)attrList[j].attrValue);
					memcpy((char *)insertInfo + offset, &fval, attributeDescPtr[i].attrLen);
				}
				else
				{
					// some random type
					return UNIXERR;
				}
			}
			j++;
		}
		i ++;
	}
	// do the insertion
	Record r;
	r.data = (void *)insertInfo;
	r.length = relLen;

	RID insertRID;
	if (ifs.insertRecord(r, insertRID) != OK)
	{
		return ifs.insertRecord(r, insertRID);
	}

	return OK;
}
