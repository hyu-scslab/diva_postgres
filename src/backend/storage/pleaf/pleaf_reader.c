/*-------------------------------------------------------------------------
 *
 * pleaf_reader.c
 * 		Implement functions called by reader  
 *
 * 
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/pleaf/pleaf_reader.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM
#include "postgres.h"

#include "storage/lwlock.h"
#include "utils/snapmgr.h"

#include "storage/pleaf_bufpage.h"
#include "storage/pleaf_internals.h"

#include <stdbool.h>
#include <assert.h>

/*
 * PLeafLookupVersion
 *
 * Lookup the visible version locator(or offset in indirect array).
 */
bool
PLeafLookupVersion(PLeafPage page, 
							PLeafOffset* offset, 
							Snapshot snapshot) 
{

	PLeafVersion first_version, version;
	PLeafVersionId version_id;
	PLeafVersionIndex version_index;
	PLeafVersionIndexData version_index_data;
	TransactionId xmin;
	TransactionId xmax;
	int status;
	int start, mid, end;
	uint16_t version_head, version_tail;
	bool version_found;
	int capacity, array_index;

	/* Initialize return value */
	version_found = false;

	/* Get capacity */
	capacity = PLeafPageGetCapacity(page);
	/* Get array index */
	array_index = PLeafPageGetArrayIndex(capacity, *offset);

	/* Initialize offset value */
	*offset = PLEAF_INVALID_VERSION_OFFSET;

	/* Get version index and its data */
	version_index = PLeafPageGetVersionIndex(page, array_index);
	version_index_data = *version_index;

	if (PLeafGetVersionType(version_index_data) == PLEAF_DIRECT_ARRAY) 
		version_found = true;

	/* Get version head and tail */
	version_head = PLeafGetVersionIndexHead(version_index_data);
	version_tail = PLeafGetVersionIndexTail(version_index_data);

	/* If empty, return immediately */
	if (PLeafCheckEmptiness(version_head, version_tail)) 
		return true;

	/* Get the very first version in the version array */
	first_version = PLeafPageGetFirstVersion(page, array_index, capacity); 

	/* Get the head version */
	version = PLeafPageGetVersion(first_version, version_head % capacity);

	/* Get xmin and xmax value */
	PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);

	/*
	 * !!! 
	 * Check visibility with transaction's snapshot.
	 * If return status is PLEAF_LOOKUP_BACKWARD, the contention between readers 
	 * and writer occurs. It can be solved by reading and checking head value 
	 * one more time.
	 */
	if((status = PLeafIsVisible(snapshot, xmin, xmax)) == PLEAF_LOOKUP_BACKWARD) 
	{
		/* Version head should be changed, and it must guarantee */
		version_index_data = *version_index;
		assert(version_head != PLeafGetVersionIndexHead(version_index_data));

		/* Read a version head one more. No need to read a version tail */
		version_head = PLeafGetVersionIndexHead(version_index_data);

		/* If empty, return immediately */
		if (PLeafCheckEmptiness(version_head, version_tail)) 
			return true;

		/* Get the head version */
		version = PLeafPageGetVersion(first_version, version_head % capacity);
		assert(version_id != PLeafGetVersionId(version));
		
		/* Get xmin and xmax value */
		PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);
	
		/* Check visibility */
		status = PLeafIsVisible(snapshot, xmin, xmax);
	}

	/* If found, return immediately */
	if (status == PLEAF_LOOKUP_FOUND) 
	{
		*offset = PLeafGetVersionOffset(version);
		return version_found;
	}
	
	/* From here, we must guarantee to search forward */
	assert(status != PLEAF_LOOKUP_BACKWARD);
	
	/* We already check visibility of version_head's version, so skip it */
	version_head = (version_head + 1) % (2 * capacity);

	/* Circular array to linear array */
	if (version_tail < version_head) 
	{
		version_tail += (2 * capacity);
	} 
	else if (version_tail == version_head) 
	{
		return true;
	}

	/*
	 * Binary search in version array
	 */
	start = version_head;
	end = version_tail - 1;

	assert(end >= 0);

	while (end >= start) 
	{
		mid = (start + end) / 2;

		version = PLeafPageGetVersion(first_version, mid % capacity);
		PLeafGetVersionInfo(PLeafGetVersionId(version), &xmin, &xmax);

		switch (PLeafIsVisible(snapshot, xmin, xmax)) 
		{
			case PLEAF_LOOKUP_FOUND:
				*offset = PLeafGetVersionOffset(version);
				return version_found;

			case PLEAF_LOOKUP_FORWARD:
				start = mid + 1;
				break;

			case PLEAF_LOOKUP_BACKWARD:
				end = mid - 1;
				break;

			default:
				assert(false);
		}
	}
	return true;
}

#endif
