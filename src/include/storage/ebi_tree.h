/*-------------------------------------------------------------------------
 *
 * ebi_tree.h
 *    EBI Tree
 *
 *
 *
 * src/include/storage/ebi_tree.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_H
#define EBI_TREE_H

#include <time.h>

#include "c.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"

/*
 * VersionOffset(64) = SegmentId(32) + SegmentOffset (32)
 * +---------------------+---------------------+
 * |   segment id(32)    | segment offset (32) |
 * +---------------------+---------------------+
 */
typedef uint64 EbiTreeVersionOffset;
typedef uint32 EbiTreeSegmentId;
typedef uint32 EbiTreeSegmentOffset;

typedef uint32 EbiTreeSegmentPageId;

#define EbiGetCurrentTime(timespec) clock_gettime(CLOCK_MONOTONIC, timespec)
#define EbiGetTimeElapsedInSeconds(now, base) \
	(1000LL * (now.tv_sec - base.tv_sec) + (now.tv_nsec - base.tv_nsec) / 1000000LL)

typedef struct EbiNodeData
{
	dsa_pointer parent;
	dsa_pointer left;
	dsa_pointer right;
	dsa_pointer proxy_target;
	dsa_pointer proxy_list_tail; /* tail pointer used for appending */

#ifdef JS_WIDTH
	uint32 left_most;
#endif
	uint32 height;
	pg_atomic_uint32 refcnt;

	dsa_pointer left_boundary;
	dsa_pointer right_boundary;

	EbiTreeSegmentId seg_id;       /* file segment */
	pg_atomic_uint32 seg_offset;   /* aligned version offset */
	pg_atomic_uint64 num_versions; /* number of versions */
} EbiNodeData;

typedef struct EbiNodeData* EbiNode;

typedef struct EbiTreeData
{
	dsa_pointer root;        /* EbiNode */
	dsa_pointer recent_node; /* EbiNode */

} EbiTreeData;

typedef struct EbiTreeData* EbiTree;

typedef struct EbiMpscQueueNodeData
{
	dsa_pointer dsa_node;    /* EbiNode */
	dsa_pointer_atomic next; /* EbiMpscQueueNode */
} EbiMpscQueueNodeData;

typedef struct EbiMpscQueueNodeData* EbiMpscQueueNode;

typedef struct EbiMpscQueueStruct
{
	dsa_pointer front; /* EbiMpscQueueNode */
	dsa_pointer rear;  /* EbiMpscQueueNode */
} EbiMpscQueueStruct;

typedef struct EbiMpscQueueStruct* EbiMpscQueue;

typedef struct EbiSpscQueueNodeData
{
	EbiNode node;
	dsa_pointer dsa_ptr; /* dsa_pointer to the EbiNode (optimization) */
	struct EbiSpscQueueNodeData* next;
} EbiSpscQueueNodeData;

typedef struct EbiSpscQueueNodeData* EbiSpscQueueNode;

typedef struct EbiSpscQueueData
{
	EbiSpscQueueNode front;
	EbiSpscQueueNode rear;
} EbiSpscQueueData;

typedef struct EbiSpscQueueData* EbiSpscQueue;

/* Public functions */
EbiTree EbiConvertToTree(dsa_area* area, dsa_pointer ptr);
EbiNode EbiConvertToNode(dsa_area* area, dsa_pointer ptr);

extern dsa_pointer EbiIncreaseRefCount(Snapshot snapshot);
extern void EbiDecreaseRefCount(dsa_pointer node);

extern dsa_pointer EbiInitTree(dsa_area* area);
extern void EbiDeleteTree(dsa_pointer dsa_ebitree);

extern void EbiInsertNode(dsa_pointer dsa_ebitree);
extern void EbiUnlinkNodes(dsa_pointer dsa_ebitree,
						   dsa_pointer unlink_queue,
						   EbiSpscQueue delete_queue);

#ifdef JS_WIDTH
extern void PrintTreeToFile(int time);
#endif
extern void EbiDeleteNodes(EbiSpscQueue delete_queue);
extern void EbiDeleteNode(EbiNode node, dsa_pointer dsa_ptr);

extern EbiNode EbiSift(TransactionId xmin, TransactionId xmax);

extern EbiTreeVersionOffset EbiSiftAndBind(TransactionId xmin,
										   TransactionId xmax,
										   Size tuple_size,
										   const void* tuple,
										   LWLock* rwlock);

extern int EbiLookupVersion(EbiTreeVersionOffset version_offset,
							Size tuple_size,
							void** ret_value);

extern bool EbiSegIsAlive(dsa_pointer dsa_ebitree, EbiTreeSegmentId seg_id);

extern bool EbiRecentNodeIsAlive(dsa_pointer dsa_ebitree);

/* Statistics */
extern void EbiMarkTupleSize(Size tuple_size);

/* Debug */
void EbiPrintTree(dsa_pointer dsa_ebitree);
void EbiPrintTreeRecursive(EbiNode node);


#endif /* EBI_TREE_H */
