/*-------------------------------------------------------------------------
 *
 * ebi_tree_utils.h
 *    Includes multiple data structures for EBI-tree such as,
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
#include "storage/ebi_tree.h"
#include "utils/dsa.h"

/*
 * MPSC Queue (used for logical deletion of EBI nodes)
 */

extern dsa_pointer EbiInitMpscQueue(dsa_area* area);
extern void EbiDeleteMpscQueue(dsa_area* area, dsa_pointer dsa_queue);
extern bool EbiMpscQueueIsEmpty(dsa_area* area, dsa_pointer dsa_queue);
extern void EbiMpscEnqueue(dsa_area* area,
						   dsa_pointer dsa_queue,
						   dsa_pointer dsa_node);
extern dsa_pointer EbiMpscDequeue(dsa_area* area, dsa_pointer dsa_queue);
extern void EbiPrintMpscQueue(dsa_area* area, dsa_pointer dsa_queue);

/*
 * SPSC Queue (used for physical deletion of EBI nodes)
 */

extern EbiSpscQueue EbiInitSpscQueue(void);
extern void EbiDeleteSpscQueue(EbiSpscQueue queue);
extern bool EbiSpscQueueIsEmpty(EbiSpscQueue queue);
extern void EbiSpscEnqueue(EbiSpscQueue queue,
						   EbiNode node,
						   dsa_pointer dsa_ptr);
extern EbiNode EbiSpscDequeue(EbiSpscQueue queue);
extern EbiNode EbiSpscQueueFront(EbiSpscQueue queue);
extern dsa_pointer EbiSpscQueueFrontDsaPointer(EbiSpscQueue queue);

#endif /* EBI_TREE_UTILS_H */
