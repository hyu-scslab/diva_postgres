/*-------------------------------------------------------------------------
 *
 * mpsc_queue.h
 *	  Multiple Producer Single Consumer Queue for EBI Tree
 *
 *
 *
 * src/include/storage/mpsc_queue.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MPSC_QUEUE_H
#define MPSC_QUEUE_H

#include "c.h"
#include "storage/ebi_tree.h"

typedef struct TaskNodeStruct {
  dsa_pointer dsa_node; /* EbiNode */
  // dsa_pointer next;     /* TaskNode */
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
extern bool IsEmpty(dsa_area* area, dsa_pointer dsa_queue);
extern void Enqueue(dsa_area* area, dsa_pointer dsa_queue,
                    dsa_pointer dsa_node);
extern dsa_pointer Dequeue(dsa_area* area, dsa_pointer dsa_queue);
extern void PrintQueue(dsa_area* area, dsa_pointer dsa_queue);

#endif /* MPSC_QUEUE_H */
