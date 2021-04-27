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

#include "c.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"

/*
 * Actual EBI tree structure.
 * */
typedef uint32 EbiTreeSegmentId;
typedef uint32 EbiTreeSegmentOffset;
typedef uint32 EbiTreeSegmentPageId;
typedef uint64 EbiTreeVersionOffset;

#define EBI_TREE_INVALID_SEG_ID ((EbiTreeSegmentId)(0))
#define EBI_TREE_INVALID_VERSION_OFFSET ((uint64)(-1))

#define EBI_TREE_SEG_ID_MASK (0xFFFFFFFF00000000ULL)
#define EBI_TREE_SEG_OFFSET_MASK (0x00000000FFFFFFFFULL)
#define EBI_TREE_VERSION_OFFSET_TO_SEG_ID(version_offset) \
  ((version_offset & EBI_TREE_SEG_ID_MASK) >> 32)
#define EBI_TREE_VERSION_OFFSET_TO_SEG_OFFSET(version_offset) \
  (version_offset & EBI_TREE_SEG_OFFSET_MASK)
#define EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset) \
  ((((uint64)(seg_id)) << 32) | seg_offset)

typedef struct EbiNodeData {
  dsa_pointer parent;
  dsa_pointer left;
  dsa_pointer right;
  dsa_pointer proxy_target;

  uint32 height;
  pg_atomic_uint32 refcnt;

  dsa_pointer left_boundary;
  dsa_pointer right_boundary;

  EbiTreeSegmentId seg_id;       /* file segment */
  pg_atomic_uint32 seg_offset;   /* aligned version offset */
  pg_atomic_uint64 num_versions; /* number of versions */
} EbiNodeData;

typedef struct EbiNodeData* EbiNode;

typedef struct EbiTreeData {
  dsa_pointer root;        /* EbiNode */
  dsa_pointer recent_node; /* EbiNode */
} EbiTreeData;

typedef struct EbiTreeData* EbiTree;

typedef struct EbiMpscQueueNodeData {
  dsa_pointer dsa_node;    /* EbiNode */
  dsa_pointer_atomic next; /* EbiMpscQueueNode */
} EbiMpscQueueNodeData;

typedef struct EbiMpscQueueNodeData* EbiMpscQueueNode;

typedef struct EbiMpscQueueStruct {
  dsa_pointer front; /* EbiMpscQueueNode */
  dsa_pointer rear;  /* EbiMpscQueueNode */
} EbiMpscQueueStruct;

typedef struct EbiMpscQueueStruct* EbiMpscQueue;

typedef struct EbiSpscQueueNodeData {
  EbiNode node;
  dsa_pointer dsa_ptr; /* dsa_pointer to the EbiNode (optimization) */
  struct EbiSpscQueueNodeData* next;
} EbiSpscQueueNodeData;

typedef struct EbiSpscQueueNodeData* EbiSpscQueueNode;

typedef struct EbiSpscQueueData {
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
extern void EbiUnlinkNodes(
    dsa_pointer dsa_ebitree,
    dsa_pointer unlink_queue,
    EbiSpscQueue delete_queue);

extern void EbiDeleteNodes(EbiSpscQueue delete_queue);
extern void EbiDeleteNode(EbiNode node, dsa_pointer dsa_ptr);

extern bool EbiNeedsNewNode(dsa_pointer dsa_ebitree);

extern EbiNode EbiSift(TransactionId vmin, TransactionId vmax);

extern EbiTreeVersionOffset EbiSiftAndBind(
    TransactionId xmin,
    TransactionId xmax,
    Size tuple_size,
    const void* tuple,
    LWLock* rwlock);

extern int EbiLookupVersion(
    EbiTreeVersionOffset version_offset,
    Size tuple_size,
    void** ret_value);

extern bool EbiSegIsAlive(dsa_pointer dsa_ebitree, EbiTreeSegmentId seg_id);

/* Debug */
void EbiPrintTree(dsa_pointer dsa_ebitree);
void EbiPrintTreeRecursive(EbiNode node);

#endif /* EBI_TREE_H */
