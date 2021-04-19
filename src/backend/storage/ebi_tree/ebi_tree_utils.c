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

static dsa_pointer AllocTask(dsa_area* area, dsa_pointer dsa_node);

static EbiListElement EbiListElementInit(dsa_pointer dsa_node);

dsa_pointer
InitQueue(dsa_area* area) {
  dsa_pointer dsa_queue, dsa_sentinel;
  TaskQueue queue;

  dsa_queue =
      dsa_allocate_extended(area, sizeof(TaskQueueStruct), DSA_ALLOC_ZERO);

  dsa_sentinel = AllocTask(area, InvalidDsaPointer);

  queue = (TaskQueue)dsa_get_address(area, dsa_queue);
  queue->head = queue->tail = dsa_sentinel;

  return dsa_queue;
}

static dsa_pointer
AllocTask(dsa_area* area, dsa_pointer dsa_node) {
  dsa_pointer dsa_task;
  TaskNode task;

  dsa_task =
      dsa_allocate_extended(area, sizeof(TaskNodeStruct), DSA_ALLOC_ZERO);
  task = (TaskNode)dsa_get_address(area, dsa_task);
  task->dsa_node = dsa_node;
  pg_atomic_init_u32(&task->next, InvalidDsaPointer);

  return dsa_task;
}

void
DeleteQueue(dsa_area* area, dsa_pointer dsa_queue) {
  TaskQueue queue;

  if (dsa_queue != InvalidDsaPointer) {
    while (Dequeue(area, dsa_queue) != InvalidDsaPointer) {
    }

    queue = (TaskQueue)dsa_get_address(area, dsa_queue);
    dsa_free(area, queue->head);
    dsa_free(area, dsa_queue);
  }
}

bool
QueueIsEmpty(dsa_area* area, dsa_pointer dsa_queue) {
  TaskQueue queue;
  queue = (TaskQueue)dsa_get_address(area, dsa_queue);
  return queue->tail == queue->head;
}

void
Enqueue(dsa_area* area, dsa_pointer dsa_queue, dsa_pointer dsa_node) {
  TaskQueue queue;
  TaskNode tail;
  dsa_pointer dsa_new_tail;
  dsa_pointer dsa_tail;
  bool success;

  queue = (TaskQueue)dsa_get_address(area, dsa_queue);
  dsa_new_tail = AllocTask(area, dsa_node);

  success = false;

  while (!success) {
    uint32 expected = 0;

    dsa_tail = queue->tail;

    tail = (TaskNode)dsa_get_address(area, dsa_tail);

    // Try logical enqueue, not visible to the dequeuer.
    success = pg_atomic_compare_exchange_u32(
        (pg_atomic_uint32*)(&tail->next), &expected, dsa_new_tail);

    // Physical enqueue.
    if (success) {
      // The thread that succeeded in changing the tail is responsible for the
      // physical enqueue. Other threads that fail might retry the loop, but
      // the ones that read the tail before the tail is changed will fail on
      // calling CAS since the next pointer is not nullptr. Thus, only the
      // threads that read the tail after the new tail assignment will be
      // competing for logical enqueue.
      queue->tail = dsa_new_tail;
    } else {
      // Instead of retrying right away, calling yield() will save the CPU
      // from wasting cycles.
      // TODO: uncomment
      // pthread_yield();
    }
  }
}

dsa_pointer
Dequeue(dsa_area* area, dsa_pointer dsa_queue) {
  TaskQueue queue;
  dsa_pointer dsa_head, dsa_next;
  TaskNode head, next;
  dsa_pointer ret;

  queue = (TaskQueue)dsa_get_address(area, dsa_queue);

  dsa_head = queue->head;
  head = (TaskNode)dsa_get_address(area, dsa_head);

  // dsa_next = head->next;
  dsa_next = pg_atomic_read_u32(&head->next);
  if (dsa_next == InvalidDsaPointer) {
    return InvalidDsaPointer;
  }

  next = (TaskNode)dsa_get_address(area, dsa_next);

  ret = next->dsa_node;

  // This is a MPSC queue.
  queue->head = dsa_next;

  // Without recycling.
  dsa_free(area, dsa_head);

  return ret;
}

void
PrintQueue(dsa_area* area, dsa_pointer dsa_queue) {
  TaskQueue queue;
  dsa_pointer dsa_tmp;
  TaskNode tmp;

  queue = (TaskQueue)dsa_get_address(area, dsa_queue);
  dsa_tmp = queue->head;

  ereport(LOG, (errmsg("----Print Queue----")));
  while (dsa_tmp != InvalidDsaPointer) {
    tmp = (TaskNode)dsa_get_address(area, dsa_tmp);
    if (tmp->dsa_node != InvalidDsaPointer) {
      ereport(LOG, (errmsg("%ld", tmp->dsa_node)));
    }
    dsa_tmp = pg_atomic_read_u32(&tmp->next);
  }
}

/*
 * Linked List Implementation
 */

static EbiListElement
EbiListElementInit(dsa_pointer dsa_node) {
  EbiListElement new_element;

  new_element = (EbiListElement)palloc(sizeof(struct EbiListElementData));
  new_element->dsa_node = dsa_node;
  new_element->next = NULL;

  return new_element;
}

void
EbiListInsert(dsa_area* area, EbiList list, dsa_pointer dsa_node) {
  EbiListElement prev, curr, new;
  EbiNode node, curr_node;

  new = EbiListElementInit(dsa_node);

  if (list->head == NULL) {
    list->head = new;
    return;
  }

  node = ConvertToEbiNode(area, dsa_node);

  curr = list->head;
  curr_node = ConvertToEbiNode(area, curr->dsa_node);

  /* When it should be a new head */
  if (curr_node->max_xid > node->max_xid) {
    list->head = new;
    new->next = curr;
    return;
  }

  prev = curr;
  curr = curr->next;

  while (curr != NULL) {
    curr_node = ConvertToEbiNode(area, curr->dsa_node);

    if (curr_node->max_xid > node->max_xid) {
      break;
    }

    prev = curr;
    curr = curr->next;
  }

  prev->next = new;
  new->next = curr;
}

bool
EbiListIsEmpty(EbiList list) {
  return (list->head == NULL);
}

void
EbiListDestroy(dsa_area* area, EbiList list) {
  EbiListElement curr, tmp;

  curr = list->head;
  while (curr != NULL) {
    tmp = curr->next;
    pfree(curr);
    curr = tmp;
  }
  pfree(list);
}

void
EbiListPrint(dsa_area* area, EbiList list) {
  EbiListElement curr;
  EbiNode tmp;

  curr = list->head;

  ereport(LOG, (errmsg("Print List")));
  while (curr != NULL) {
    tmp = ConvertToEbiNode(area, curr->dsa_node);
    ereport(LOG, (errmsg("seg id : %d", tmp->seg_id)));
    curr = curr->next;
  }
}

#endif /* J3VM */
