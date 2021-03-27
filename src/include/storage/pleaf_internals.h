/*-------------------------------------------------------------------------
 *
 * pleaf_internals.h
 * 		Internal interface definitions 
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pleaf_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLEAF_INTERNALS_H
#define PLEAF_INTERNALS_H

/* pleaf_reader.c */
#define PLEAF_LOOKUP_FORWARD  (0)
#define PLEAF_LOOKUP_BACKWARD (1)
#define PLEAF_LOOKUP_FOUND		(2)

/* pleaf_writer.c */
#define PLEAF_APPEND_NOTHING 	(0)
#define PLEAF_APPEND_ERROR		(1)
#define PLEAF_APPEND_SUCCESS	(2)

#define MAX_ARRAY_ACCESSED (5)
#define LAST_ARRAY_ACCESSED (1)

#define PLEAF_APPEND_DONE (0)
#define PLEAF_APPEND_CONTRACT (1)
#define PLEAF_APPEND_COMPACT (2)
#define PLEAF_APPEND_EXPAND (3)

#define PLeafMakeOffset(gen_no, offset) \
	((uint64_t)(((uint64_t)gen_no << 48) | offset))

#define GetVersionCount(head, tail, cap) \
	((head <= tail) ? tail - head : tail - head + (2 * cap))

typedef struct 
{
	PLeafPage page;
	int array_index;
	int frame_id;
} PLeafTempInfoData;

typedef PLeafTempInfoData* PLeafTempInfo;

extern int PLeafIsVisible(Snapshot snapshot, 
								uint32_t xmin, 
								uint32_t xmax);

extern bool PLeafCheckVisibility(TransactionId xmin, 
								TransactionId xmax);

extern void PLeafGetVersionInfo(PLeafVersionId version_id,
								TransactionId* xmin, 
								TransactionId* xmax);

extern bool PLeafCheckAppendness(int cap, uint16_t head, uint16_t tail);
extern bool PLeafCheckEmptiness(uint16_t head, uint16_t tail);


extern bool PLeafLookupVersion(PLeafPage page, 
									PLeafOffset* offset, 
									Snapshot snapshot);

extern int PLeafAppendVersion(PLeafOffset offset, 
									PLeafOffset* ret_offset,
									TransactionId xmin, TransactionId xmax, 
									PLeafVersionOffset version_offset,
									Size tuple_size, 
									const void* tuple, 
									LWLock* rwlock);

#endif /* PLEAF_INTERNALS_H */
