/*-------------------------------------------------------------------------
 *
 * ebi_tree_hash.h
 *    Hash table definitions for mapping EbiTreePage to EbiTreeBuffer indexes.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/ebi_tree_hash.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_HASH_H
#define EBI_TREE_HASH_H

#define EbiTreeHashPartition(hashcode) ((hashcode) % NUM_EBI_TREE_PARTITIONS)

#define EbiTreeMappingPartitionLock(hashcode)          \
	(&MainLWLockArray[EBI_TREE_MAPPING_LWLOCK_OFFSET + \
					  EbiTreeHashPartition(hashcode)]  \
		  .lock)

extern Size EbiTreeHashShmemSize(int size);
extern void EbiTreeHashInit(int size);
extern uint32 EbiTreeHashCode(const EbiTreeBufTag* tagPtr);

extern int EbiTreeHashLookup(const EbiTreeBufTag* tagPtr, uint32 hashcode);
extern int EbiTreeHashInsert(const EbiTreeBufTag* tagPtr,
							 uint32 hashcode,
							 int page_id);
extern void EbiTreeHashDelete(const EbiTreeBufTag* tagPtr, uint32 hashcode);

#endif /* EBI_TREE_HASH_H */
