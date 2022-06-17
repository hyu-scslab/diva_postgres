/*-------------------------------------------------------------------------
 *
 * pleaf_reader.c
 * 		Implement functions called by reader  
 *
 *
 * Copyright (C) 2021 Scalable Computing Systems Lab.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/pleaf/pleaf_reader.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA
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
    	return true;
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
