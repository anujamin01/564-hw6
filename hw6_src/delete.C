#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation,
					   const string &attrName,
					   const Operator op,
					   const Datatype type,
					   const char *attrValue)
{
	Status status;
	// declare hfs object
	HeapFileScan *hfs = new HeapFileScan(relation, status);

	if (status != OK)
	{
		return status;
	}
	AttrDesc desc;
	RID rid;
	// get description of attribute info
	attrCat->getInfo(relation, attrName, desc);

	int val;
	float fval;
	// start scan
	// handle attrname null
	if (attrName.empty())
	{
		status = hfs->startScan(0, 0, STRING, NULL, EQ);
	}
	else if (type == STRING)
	{
		status = hfs->startScan(desc.attrOffset, desc.attrLen, type, attrValue, op);
	}
	else if (type == INTEGER)
	{
		val = atoi(attrValue);
		status = hfs->startScan(desc.attrOffset, desc.attrLen, type, (char *)&val, op);
	}
	else if (type == FLOAT)
	{
		fval = atof(attrValue);
		status = hfs->startScan(desc.attrOffset, desc.attrLen, type, (char *)&fval, op);
	}


	// scanNext and try to delete the record
	while (hfs->scanNext(rid) == OK)
	{
		if (hfs->deleteRecord() != OK)
			//	delete hfs;
			return hfs->deleteRecord();
	}
	// finished scan
	hfs->endScan();
	delete hfs;

	return OK;
}
