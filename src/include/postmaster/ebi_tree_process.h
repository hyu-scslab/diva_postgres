/*-------------------------------------------------------------------------
 *
 * ebi_tree_process.h
 *	  EBI Tree Process
 *
 *
 *
 * src/include/postmaster/ebi_tree_process.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_PROCESS_H
#define EBI_TREE_PROCESS_H

#include "c.h"
#include "utils/dsa.h"
#include "utils/snapshot.h"

#define EBI_NUM_GC_QUEUE (2)

extern void EbiTreeProcessMain(void) pg_attribute_noreturn();

extern Size EbiTreeShmemSize(void);
extern void EbiTreeShmemInit(void);

extern void EbiTreeDsaInit(void);

typedef struct
{
	pid_t ebitree_pid;             /* PID (0 if not started) */
	dsa_handle handle;             /* dsa handle */
	dsa_pointer ebitree;           /* EbiTree */
	uint32 seg_id;                 /* Next seg_id */
	pg_atomic_uint64 num_versions; /* Number of versions in the tree */
	dsa_pointer unlink_queue;      /* EbiMpscQueue */
	pg_atomic_uint32 curr_slot;    /* Pointer to current slot */
	pg_atomic_uint64 gc_queue_refcnt[EBI_NUM_GC_QUEUE]; /* Slotted GC queue */

	/* Stats */
	TransactionId max_xid;
	uint32 average_ver_len;
	pg_atomic_flag is_updating_stat;
	struct timespec last_timespec;
	bool version_usage_check_flag;
	Size sampled_tuple_size;

} EbiTreeShmemStruct;

extern EbiTreeShmemStruct* EbiTreeShmem;

/* src/backend/storage/ebi_tree/ebi_tree.c */
extern dsa_area* ebitree_dsa_area;


#endif /* EBI_TREE_PROCESS_H */
