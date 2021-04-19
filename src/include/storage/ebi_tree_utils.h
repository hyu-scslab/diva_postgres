/*-------------------------------------------------------------------------
 *
 * ebi_tree_utils.h
 *    Includes multiple data structures such as,
 *      - Multiple Producer Single Consumer Queue (MPSC queue)
 *	    - Linked List
 *
 *
 *
 * src/include/storage/ebi_tree_utils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_UTILS_H
#define EBI_TREE_UTILS_H

#include "c.h"
#include "utils/dsa.h"

/*
 * MPSC Queue
 */

typedef struct TaskNodeStruct {
  dsa_pointer dsa_node; /* EbiNode */
  pg_atomic_uint32 next;
} TaskNodeStruct;

typedef struct TaskNodeStruct* TaskNode;

typedef struct TaskQueueStruct {
  dsa_pointer head; /* TaskNode */
  dsa_pointer tail; /* TaskNode */
} TaskQueueStruct;

typedef struct TaskQueueStruct* TaskQueue;

extern dsa_pointer InitQueue(dsa_area* area);
extern void DeleteQueue(dsa_area* area, dsa_pointer dsa_queue);
extern bool QueueIsEmpty(dsa_area* area, dsa_pointer dsa_queue);
extern void
Enqueue(dsa_area* area, dsa_pointer dsa_queue, dsa_pointer dsa_node);
extern dsa_pointer Dequeue(dsa_area* area, dsa_pointer dsa_queue);
extern void PrintQueue(dsa_area* area, dsa_pointer dsa_queue);

typedef struct EbiTreeLinkedListStruct* EbiTreeLinkedList;

/*
 * Linked List
 */

typedef struct EbiListElementData {
  dsa_pointer dsa_node; /* EbiNode */
  struct EbiListElementData* next;
} EbiListElementData;

typedef struct EbiListElementData* EbiListElement;

typedef struct EbiListData {
  EbiListElement head;
} EbiListData;

typedef struct EbiListData* EbiList;

extern void EbiListInsert(dsa_area* area, EbiList list, dsa_pointer dsa_node);
extern bool EbiListIsEmpty(EbiList list);
extern void EbiListDestroy(dsa_area* area, EbiList list);
extern void EbiListPrint(dsa_area* area, EbiList list);

#endif /* EBI_TREE_UTILS_H */
