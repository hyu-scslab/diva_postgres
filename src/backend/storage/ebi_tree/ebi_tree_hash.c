/*-------------------------------------------------------------------------
 *
 * ebi_tree_hash.c
 *
 * Hash table implementation for mapping EbiTreePage to
 * EbiTreeBuffer indexes.
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
 *    src/backend/storage/ebi_tree/ebi_tree_hash.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA
#include "postgres.h"

#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/dynahash.h"
#include "utils/hsearch.h"
#include "utils/snapmgr.h"

#include "storage/ebi_tree_buf.h"
#include "storage/ebi_tree_hash.h"

typedef struct
{
	EbiTreeBufTag key; /* Tag of a disk page */
	int id;            /* Associated buffer id */
} EbiTreeLookupEnt;

StaticAssertDecl(sizeof(EbiTreeLookupEnt) ==
					 (sizeof(EbiTreeBufTag) + sizeof(int)),
				 "EbiTree Lookup Entry");

static HTAB *SharedEbiTreeHash;

/*
 * EbiTreeHashShmemSize
 *
 * compute the size of shared memory for ebi_tree hash
 */
Size
EbiTreeHashShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(EbiTreeLookupEnt));
}

/*
 * EbiTreeHashInit
 *
 * Initialize ebi_tree hash in shared memory
 */
void
EbiTreeHashInit(int size)
{
	HASHCTL info;
	long num_partitions;

	/* See next_pow2_long(long num) in dynahash.c */
	num_partitions = 1L << my_log2(NUM_EBI_TREE_PARTITIONS);

	/* EbiTreeBufTag maps to EbiTreeHash */
	info.keysize = sizeof(EbiTreeBufTag);
	info.entrysize = sizeof(EbiTreeLookupEnt);
	info.num_partitions = num_partitions;

	SharedEbiTreeHash = ShmemInitHash("Shared EbiTree Lookup Table",
									  size,
									  size,
									  &info,
									  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 * EbiTreeHashCode
 *
 * Compute the hash code associated with a EbiTreeBufTag
 * This must be passed to the lookup/insert/delete routines along with the
 * tag. We do this way because the callers need to know the hash code in
 * order to determine which buffer partition to lock, and we don't want to
 * do the hash computation twice (hash_any is a bit slow).
 */
uint32
EbiTreeHashCode(const EbiTreeBufTag *tagPtr)
{
	return get_hash_value(SharedEbiTreeHash, (void *)tagPtr);
}

/*
 * EbiTreeHashLookup
 *
 * Lookup the given EbiTreeBufTag; return ebi_tree page id, or -1 if not found
 * Caller must hold at least shared lock on EbiTreeMappingLock for tag's
 * partition
 */
int
EbiTreeHashLookup(const EbiTreeBufTag *tagPtr, uint32 hashcode)
{
	EbiTreeLookupEnt *result;

	result = (EbiTreeLookupEnt *)hash_search_with_hash_value(
		SharedEbiTreeHash, (void *)tagPtr, hashcode, HASH_FIND, NULL);

	if (!result) return -1;

	return result->id;
}

/*
 * EbiTreeHashInsert
 *
 * Insert a hashtable entry for given tag and buffer id,
 * unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion. If a conflicting entry already exists,
 * returns its buffer ID.
 *
 * Caller must hold exclusive lock on EbiTreeMappingLock for tag's partition
 */
int
EbiTreeHashInsert(const EbiTreeBufTag *tagPtr, uint32 hashcode, int buffer_id)
{
	EbiTreeLookupEnt *result;
	bool found;

	Assert(buffer_id >= 0); /* -1 is reserved for not-in-table */

	result = (EbiTreeLookupEnt *)hash_search_with_hash_value(
		SharedEbiTreeHash, (void *)tagPtr, hashcode, HASH_ENTER, &found);

	if (found) /* found something already in the hash table */
		return result->id;

	result->id = buffer_id;
	return -1;
}

/*
 * EbiTreeHashDelete
 *
 * Delete the hashtable entry for given tag (must exist)
 *
 * Caller must hold exclusive lock on EbiTreeMappingLock for tag's partition
 */
void
EbiTreeHashDelete(const EbiTreeBufTag *tagPtr, uint32 hashcode)
{
	EbiTreeLookupEnt *result;

	result = (EbiTreeLookupEnt *)hash_search_with_hash_value(
		SharedEbiTreeHash, (void *)tagPtr, hashcode, HASH_REMOVE, NULL);

	if (!result) elog(ERROR, "shared ebi_tree hash table corrupted");
}

#endif /* DIVA */
