/*-------------------------------------------------------------------------
 *
 * pleaf.h
 * 		PLeaf API Declarations 
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pleaf.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PLEAF_H
#define PLEAF_H

/* PLeafIsLeftUpdate return status */
#define PLEAF_NORMAL	(0)
#define PLEAF_SWITCH	(1)
#define PLEAF_RESET		(2)

extern int
PLeafLookupTuple(
		uint64 offset,
		Snapshot snapshot, 
		Size tuple_size,
		void** ret_value);

extern bool
PLeafIsLeftLookup(
		uint64 left_offset,
		uint64 right_offset,
		TransactionId xid_bound,
		Snapshot snapshot);

extern void 
PLeafAppendTuple(
		uint64 offset,
		uint64* ret_offset,
		TransactionId xmin,
		TransactionId xmax,
		Size tuple_size,
		const void* tuple,
		LWLock* rwlock);

extern bool
PLeafIsLeftUpdate(
		uint64 left_offset,
		uint64 right_offset,
		int* ret_status);

extern void
PLeafInit(void);

extern Size
PLeafShmemSize(void);

#endif /* PLEAF_H */
