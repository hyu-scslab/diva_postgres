/*-------------------------------------------------------------------------
 *
 * ebi_tree_utils.c
 *
 * Data Structures for EBI Tree Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/ebi_tree/ebi_tree_utils.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM
#include "postgres.h"

#include "storage/ebi_tree.h"
#include "storage/ebi_tree_utils.h"

static dsa_pointer EbiAllocMpscQueueNode(dsa_area* area, dsa_pointer dsa_node);
static EbiSpscQueueNode EbiAllocSpscQueueNode(
    EbiNode node,
    dsa_pointer dsa_ptr);

/*
 * MPSC Queue Implementation
 */

dsa_pointer
EbiInitMpscQueue(dsa_area* area) {
  dsa_pointer dsa_queue, dsa_sentinel;
  EbiMpscQueue queue;

  dsa_queue =
      dsa_allocate_extended(area, sizeof(EbiMpscQueueStruct), DSA_ALLOC_ZERO);

  dsa_sentinel = EbiAllocMpscQueueNode(area, InvalidDsaPointer);

  queue = (EbiMpscQueue)dsa_get_address(area, dsa_queue);
  queue->front = queue->rear = dsa_sentinel;

  return dsa_queue;
}

static dsa_pointer
EbiAllocMpscQueueNode(dsa_area* area, dsa_pointer dsa_node) {
  dsa_pointer dsa_task;
  EbiMpscQueueNode task;

  dsa_task =
      dsa_allocate_extended(area, sizeof(EbiMpscQueueNodeData), DSA_ALLOC_ZERO);
  task = (EbiMpscQueueNode)dsa_get_address(area, dsa_task);
  task->dsa_node = dsa_node;
  dsa_pointer_atomic_init(&task->next, InvalidDsaPointer);

  return dsa_task;
}

void
EbiDeleteMpscQueue(dsa_area* area, dsa_pointer dsa_queue) {
  EbiMpscQueue queue;
  dsa_pointer dsa_node;
  EbiNode node;

  if (DsaPointerIsValid(dsa_queue)) {
    dsa_node = EbiMpscDequeue(area, dsa_queue);

    while (DsaPointerIsValid(dsa_node)) {
      /* Delete node */
      node = EbiConvertToNode(area, dsa_node);
      EbiDeleteNode(node, dsa_node);

      dsa_node = EbiMpscDequeue(area, dsa_queue);
    }

    queue = (EbiMpscQueue)dsa_get_address(area, dsa_queue);
    dsa_free(area, queue->front);
    dsa_free(area, dsa_queue);
  }
}

bool
EbiMpscQueueIsEmpty(dsa_area* area, dsa_pointer dsa_queue) {
  EbiMpscQueue queue;
  queue = (EbiMpscQueue)dsa_get_address(area, dsa_queue);
  return queue->rear == queue->front;
}

void
EbiMpscEnqueue(dsa_area* area, dsa_pointer dsa_queue, dsa_pointer dsa_node) {
  EbiMpscQueue queue;
  EbiMpscQueueNode rear;
  dsa_pointer dsa_new_rear;
  dsa_pointer dsa_rear;
  bool success;
  dsa_pointer expected;

  queue = (EbiMpscQueue)dsa_get_address(area, dsa_queue);
  dsa_new_rear = EbiAllocMpscQueueNode(area, dsa_node);

  success = false;
  expected = InvalidDsaPointer;

  while (!success) {
    dsa_rear = queue->rear;

    rear = (EbiMpscQueueNode)dsa_get_address(area, dsa_rear);

    // Try logical enqueue, not visible to the dequeuer.
    success = dsa_pointer_atomic_compare_exchange(
        &rear->next, &expected, dsa_new_rear);

    // Physical enqueue.
    if (success) {
      // The thread that succeeded in changing the rear is responsible for the
      // physical enqueue. Other threads that fail might retry the loop, but
      // the ones that read the rear before the rear is changed will fail on
      // calling CAS since the next pointer is not nullptr. Thus, only the
      // threads that read the rear after the new rear assignment will be
      // competing for logical enqueue.
      queue->rear = dsa_new_rear;
    } else {
      // Instead of retrying right away, calling yield() will save the CPU
      // from wasting cycles.
      // TODO: uncomment
      // pthread_yield();
    }
  }
}

dsa_pointer
EbiMpscDequeue(dsa_area* area, dsa_pointer dsa_queue) {
  EbiMpscQueue queue;
  dsa_pointer dsa_front, dsa_next;
  EbiMpscQueueNode front, next;
  dsa_pointer ret;

  queue = (EbiMpscQueue)dsa_get_address(area, dsa_queue);

  dsa_front = queue->front;
  front = (EbiMpscQueueNode)dsa_get_address(area, dsa_front);

  dsa_next = dsa_pointer_atomic_read(&front->next);
  if (!DsaPointerIsValid(dsa_next)) {
    return InvalidDsaPointer;
  }

  next = (EbiMpscQueueNode)dsa_get_address(area, dsa_next);

  ret = next->dsa_node;

  // This is a MPSC queue.
  queue->front = dsa_next;

  // Without recycling.
  dsa_free(area, dsa_front);

  return ret;
}

void
EbiPrintMpscQueue(dsa_area* area, dsa_pointer dsa_queue) {
  EbiMpscQueue queue;
  dsa_pointer dsa_tmp;
  EbiMpscQueueNode tmp;

  queue = (EbiMpscQueue)dsa_get_address(area, dsa_queue);
  dsa_tmp = queue->front;

  ereport(LOG, (errmsg("----Print Queue----")));
  while (dsa_tmp != InvalidDsaPointer) {
    tmp = (EbiMpscQueueNode)dsa_get_address(area, dsa_tmp);
    if (tmp->dsa_node != InvalidDsaPointer) {
      ereport(LOG, (errmsg("%ld", tmp->dsa_node)));
    }
    dsa_tmp = dsa_pointer_atomic_read(&tmp->next);
  }
}

/*
 * SPSC Queue Implementation
 */

EbiSpscQueue
EbiInitSpscQueue(void) {
  EbiSpscQueue queue;

  queue = (EbiSpscQueue)palloc(sizeof(struct EbiSpscQueueData));
  queue->front = queue->rear = NULL;

  return queue;
}

void
EbiDeleteSpscQueue(EbiSpscQueue queue) {
  EbiNode front;
  dsa_pointer dsa_ptr;

  Assert(queue != NULL);

  while (!EbiSpscQueueIsEmpty(queue)) {
    dsa_ptr = EbiSpscQueueFrontDsaPointer(queue);
    front = EbiSpscDequeue(queue);

    EbiDeleteNode(front, dsa_ptr);
  }

  pfree(queue);
}

bool
EbiSpscQueueIsEmpty(EbiSpscQueue queue) {
  return (queue->front == NULL);
}

static EbiSpscQueueNode
EbiAllocSpscQueueNode(EbiNode node, dsa_pointer dsa_ptr) {
  EbiSpscQueueNode new;

  new = (EbiSpscQueueNode)palloc(sizeof(struct EbiSpscQueueNodeData));
  new->node = node;
  new->dsa_ptr = dsa_ptr;
  new->next = NULL;

  return new;
}

void
EbiSpscEnqueue(EbiSpscQueue queue, EbiNode node, dsa_pointer dsa_ptr) {
  EbiSpscQueueNode new;

  new = EbiAllocSpscQueueNode(node, dsa_ptr);

  if (EbiSpscQueueIsEmpty(queue)) {
    queue->rear = queue->front = new;
    return;
  }

  /* Insert at the end */
  queue->rear->next = new;
  queue->rear = new;
}

EbiNode
EbiSpscDequeue(EbiSpscQueue queue) {
  EbiSpscQueueNode orig_front;
  EbiNode ret;

  /* The caller always ensures this property */
  Assert(!EbiSpscQueueIsEmpty(queue));

  /* Temporarily save original front for deletion */
  orig_front = queue->front;
  ret = orig_front->node;

  /* Advance the front */
  queue->front = orig_front->next;

  /* Check if the rear needs modification */
  if (EbiSpscQueueIsEmpty(queue)) {
    queue->rear = NULL;
  }

  /* Deallocate memory */
  pfree(orig_front);

  return ret;
}

EbiNode
EbiSpscQueueFront(EbiSpscQueue queue) {
  /* The caller always ensures this property */
  Assert(!EbiSpscQueueIsEmpty(queue));
  return queue->front->node;
}

dsa_pointer
EbiSpscQueueFrontDsaPointer(EbiSpscQueue queue) {
  /* The caller always ensures this property */
  Assert(!EbiSpscQueueIsEmpty(queue));
  return queue->front->dsa_ptr;
}

#endif /* J3VM */
