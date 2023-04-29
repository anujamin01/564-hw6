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

	Status status = attrCat->getRelInfo(relation, attributeCounter, attributeDescPtr);
	if (status != OK)
	{
		return status;
	}
	// if the parameter attrCnt is wrong
	if (attributeCounter != attrCnt)
	{
		return UNIXERR;
	}

	// grab total length of what to insert
	int relLen = 0;
	for (int i = 0; i < attrCnt; i++)
	{
		relLen += attributeDescPtr[i].attrLen;
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
	int val = 0;
	float fval = 0;
	// add all the attribute info to our buffer in order

	for (int i = 0; i < attrCnt; i++)
	{
		bool attrFound = false;
		for (int j = 0; j < attributeCounter; j++)
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
					val = atoi((char *)attrList[j].attrValue);
					memcpy((char *)insertInfo + offset, &val, attributeDescPtr[i].attrLen);
				}
				else if (attrList[j].attrType == FLOAT)
				{
					fval = atof((char *)attrList[j].attrValue);
					memcpy((char *)insertInfo + offset, &fval, attributeDescPtr[i].attrLen);
				}
				else
				{
					// some random type
					return UNIXERR;
				}
				attrFound = true;
				break;
				// insert different types of data
			}
		}
		// error couldn't find a matching attribute
		if (!attrFound)
		{
			delete[] insertInfo;
			free(attributeDescPtr);
			return UNIXERR;
		}
	}
	// do the insertion
	Record r;
	r.data = (void *)insertInfo;
	r.length = relLen;

	RID insertRID;
	status = ifs.insertRecord(r, insertRID);
	if (status != OK)
	{
		return status;
	}

	// free some stuff up
	delete[] insertInfo;
	free(attributeDescPtr);

	return OK;
}
