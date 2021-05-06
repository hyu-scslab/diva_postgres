/*-------------------------------------------------------------------------
 *
 * ebi_tree_buf.h
 *    EBI Tree Buffer
 *
 *
 *
 * src/include/storage/ebi_tree_buf.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_BUF_H
#define EBI_TREE_BUF_H

#include "port/atomics.h"
#include "storage/ebi_tree.h"

#define EBI_TREE_SEG_PAGESZ ((Size)BLCKSZ)

typedef struct EbiTreeBufTag
{
	EbiTreeSegmentId seg_id; /* TODO: comment on relationship with epoch id */
	EbiTreeSegmentPageId page_id;
} EbiTreeBufTag;

typedef struct EbiTreeBufDesc
{
	/* ID of the cached page. seg_id 0 means this entry is unused */
	EbiTreeBufTag tag;

	/* Whether the page is not yet synced */
	bool is_dirty;

	/*
	 * Buffer entry with refcnt > 0 cannot be evicted.
	 * We use refcnt as a pin. The refcnt of an appending page should be
	 * kept 1 or higher, and the transaction which filled up the page
	 * should decrease it to unpin it.
	 */
	pg_atomic_uint32 refcnt;
} EbiTreeBufDesc;

#define EBI_TREE_BUF_DESC_PAD_TO_SIZE (SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union EbiTreeBufDescPadded {
	EbiTreeBufDesc desc;
	char pad[EBI_TREE_BUF_DESC_PAD_TO_SIZE];
} EbiTreeBufDescPadded;

/* Metadata for EBI tree buffer */
typedef struct EbiTreeBufMeta
{
	/*
	 * Indicate the cache entry which might be a victim for allocating
	 * a new page. Need to use fetch-and-add on this so that multiple
	 * transactions can allocate/evict cache entries concurrently.
	 */
	pg_atomic_uint64 eviction_rr_idx;
} EbiTreeBufMeta;

/* Macros used as helper functions */
#define GetEbiTreeBufDescriptor(id) (&EbiTreeBufDescriptors[(id)].desc)

#define InvalidEbiTreeBuf (INT_MAX)

/* Public functions */
extern Size EbiTreeBufShmemSize(void);
extern void EbiTreeBufInit(void);

extern void EbiTreeAppendVersion(EbiTreeSegmentId seg_id,
								 EbiTreeSegmentOffset seg_offset,
								 Size tuple_size,
								 const void* tuple,
								 LWLock* rwlock);
extern int EbiTreeReadVersionRef(EbiTreeSegmentId seg_id,
								 EbiTreeSegmentOffset seg_offset,
								 Size tuple_size,
								 void** ret_value);

extern void EbiTreeCreateSegmentFile(EbiTreeSegmentId seg_id);
extern void EbiTreeRemoveSegmentFile(EbiTreeSegmentId seg_id);

extern bool EbiTreeBufIsValid(int buf_id);

extern void EbiTreeBufUnref(int buf_id);

#endif /* EBI_TREE_BUF_H */
