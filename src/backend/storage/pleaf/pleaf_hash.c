/*-------------------------------------------------------------------------
 *
 * pleaf_hash.c
 * 		Hash table implementation for mapping PLeafPage to PLeafBuffer indexes. 
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
 *    src/backend/storage/pleaf/pleaf_hash.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA
#include "postgres.h"

#include "utils/snapmgr.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"
#include "utils/dynahash.h"

#include "storage/pleaf_stack_helper.h"
#include "storage/pleaf_bufpage.h"
#include "storage/pleaf_buf.h"
#include "storage/pleaf_hash.h"

typedef struct
{
	PLeafTag key;			/* Tag of a disk page */
	int		id;			/* Associated buffer id */
} PLeafLookupEnt;

StaticAssertDecl(sizeof(PLeafLookupEnt) == (sizeof(PLeafTag) + sizeof(int)),
		"PLeaf Lookup Entry");

static HTAB *SharedPLeafHash;

/*
 * PLeafHashShmemSize
 *
 * compute the size of shared memory for pleaf hash
 */
Size
PLeafHashShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(PLeafLookupEnt));
}

/*
 * PLeafHashInit
 *
 * Initialize pleaf hash in shared memory
 */
void
PLeafHashInit(int size)
{
	HASHCTL		info;
	long num_partitions;

	/* See next_pow2_long(long num) in dynahash.c */
	num_partitions = 1L << my_log2(NUM_PLEAF_PARTITIONS);

	/* PLeafTag maps to PLeafHash */
	info.keysize = sizeof(PLeafTag);
	info.entrysize = sizeof(PLeafLookupEnt);
	info.num_partitions = num_partitions;

	SharedPLeafHash = ShmemInitHash("Shared PLeaf Lookup Table",
										size, size,
										&info,
										HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 * PLeafHashCode
 *
 * Compute the hash code associated with a PLeafTag
 * This must be passed to the lookup/insert/delete routines along with the
 * tag. We do this way because the callers need to know the hash code in
 * order to determine which buffer partition to lock, and we don't want to
 * do the hash computation twice (hash_any is a bit slow).
 */
uint32
PLeafHashCode(const PLeafTag *tagPtr)
{
	return get_hash_value(SharedPLeafHash, (void*) tagPtr);
}

/*
 * PLeafHashLookup
 *
 * Lookup the given PLeafTag; return pleaf page id, or -1 if not found
 * Caller must hold at least shared lock on PLeafMappingLock for tag's
 * partition
 */
int
PLeafHashLookup(const PLeafTag *tagPtr, uint32 hashcode)
{
	PLeafLookupEnt *result;

	result = (PLeafLookupEnt *)
			hash_search_with_hash_value(SharedPLeafHash,
									(void *) tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * PLeafHashInsert
 *
 * Insert a hashtable entry for given tag and buffer id,
 * unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion. If a conflicting entry already exists,
 * returns its buffer ID.
 *
 * Caller must hold exclusive lock on PLeafMappingLock for tag's partition
 */
int
PLeafHashInsert(const PLeafTag *tagPtr, uint32 hashcode, int buffer_id)
{
	PLeafLookupEnt		*result;
	bool		found;

	Assert(buffer_id >= 0);		/* -1 is reserved for not-in-table */

	result = (PLeafLookupEnt *)
			hash_search_with_hash_value(SharedPLeafHash,
										(void *) tagPtr,
										hashcode,
										HASH_ENTER,
										&found);

	if (found)		/* found something already in the hash table */
		return result->id;

	result->id = buffer_id;
	return -1;
}

/*
 * PLeafHashDelete
 *
 * Delete the hashtable entry for given tag (must exist)
 *
 * Caller must hold exclusive lock on PLeafMappingLock for tag's partition
 */
void
PLeafHashDelete(const PLeafTag *tagPtr, uint32 hashcode)
{
	PLeafLookupEnt *result;

	result = (PLeafLookupEnt *)
			hash_search_with_hash_value(SharedPLeafHash,
										(void *) tagPtr,
										hashcode,
										HASH_REMOVE,
										NULL);

	if (!result)
		elog(ERROR, "shared pleaf hash table corrupted");
}

#endif /* DIVA */

