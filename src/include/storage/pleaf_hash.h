/*-------------------------------------------------------------------------
 *
 * pleaf_hash.h
 *    Hash table definitions for mapping PLeafPage to PLeafBuffer indexes.
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pleaf_hash.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLEAF_HASH_H
#define PLEAF_HASH_H

#define PLeafHashPartition(hashcode) \
	((hashcode) % NUM_PLEAF_PARTITIONS)

#define PLeafMappingPartitionLock(hashcode) \
	(&MainLWLockArray[PLEAF_MAPPING_LWLOCK_OFFSET + \
	 PLeafHashPartition(hashcode)].lock)

extern Size PLeafHashShmemSize(int size);
extern void PLeafHashInit(int size);
extern uint32 PLeafHashCode(const PLeafTag* tagPtr); // JAESEON

extern int PLeafHashLookup(const PLeafTag* tagPtr, 
							uint32 hashcode);
extern int PLeafHashInsert(const PLeafTag* tagPtr, 
							uint32 hashcode, 
							int page_id);
extern void PLeafHashDelete(const PLeafTag* tagPtr,
							uint32 hashcode);

#endif /* PLEAF_HASH_H */
