/*-------------------------------------------------------------------------
 *
 * pleaf_bufpage.h
 * 		Internal opeartion in pleaf buffer page 
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pleaf_bufpage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLEAF_BUFPAGE_H
#define PLEAF_BUFPAGE_H

#define PLEAF_PAGE_SIZE ((size_t) (1U << 12))
#define PLEAF_PAGE_HEADER_SIZE ((size_t) (1U << 8))

#define PLEAF_PAGE_ID_SIZE (1U << 3)
#define PLEAF_PAGE_ID_START (0)

#define PLEAF_BITMAP_SIZE (1U << 3)
#define PLAEF_BITMAP_START (PLEAF_PAGE_ID_SIZE)

#define PLEAF_VERSION_INDEX_ARR_SIZE ((1U << 8) - (1U << 4))
#define PLEAF_VERSION_INDEX_ARR_START (PLEAF_PAGE_ID_SIZE + PLEAF_BITMAP_SIZE)

#define PLEAF_VERSION_DATA_SIZE (1U << 4)
#define PLEAF_VERSION_DATA_ARR_START (PLEAF_PAGE_HEADER_SIZE)

#define PLEAF_INVALID_PAGE_ID ((uint32_t)(-1))
//#define PLEAF_INVALID_OFFSET  ((uint64_t)(-1))
#define PLEAF_INVALID_OFFSET  (0)
#define PLEAF_INVALID_VERSION_OFFSET ((uint64_t)(-1))

/* Internal Mask */
#define PLEAF_PAGE_ID_MASK (0x00000000FFFFFFFFULL)
#define PLEAF_PAGE_CAP_MASK (0x07U)
#define PLEAF_PAGE_INST_MASK (0x0FU)
#define PLEAF_VERSION_ID_MASK (0x00000000FFFFFFFFULL)

#define PLEAF_VERSION_TYPE_MASK (0x80000000u)
#define PLEAF_VERSION_HEAD_MASK (0x7fff0000u)
#define PLEAF_VERSION_TAIL_MASK (0x0000ffffu)

#define PLEAF_OFFSET_MASK (0x0000FFFFFFFFFFFFULL)

/* 1 for direct, 0 for indirect */
#define PLEAF_DIRECT_ARRAY (0x80000000U)
#define PLEAF_INDIRECT_ARRAY (0x00000000U)

/* Bitmap */
#define PLEAF_BITMAP_FULL (0)
#define PLEAF_BITMAP_INIT ((uint64_t)(-1)) 

/* Initialize stack head */
#define PLEAF_STACK_HEAD_INIT (0x00000000FFFFFFFFULL)

#define PLEAF_OFFSET_TO_PAGE_ID(offset) \
	((uint32_t)(offset / PLEAF_PAGE_SIZE))

#define PLEAF_OFFSET_TO_INTERNAL_OFFSET(offset) \
	(offset & PLEAF_OFFSET_MASK)

#define PLEAF_OFFSET_TO_GEN_NUMBER(offset) \
	((uint16_t)(offset >> 48))

#define PLEAF_ARRAY_INDEX_TO_OFFSET(page_id, cap, array_index) \
	((uint64_t)((page_id * PLEAF_PAGE_SIZE) + \
	PLEAF_PAGE_HEADER_SIZE + \
	cap * array_index * PLEAF_VERSION_DATA_SIZE))

typedef uint64_t PLeafBitmap;

typedef uint64_t PLeafPageMetadata;
typedef uint32_t PLeafPageId;

typedef uint16_t PLeafGenNumber;

typedef uint64_t PLeafVersionId;
typedef uint64_t PLeafVersionOffset;

typedef uint64_t PLeafOffset;

/*
 * Bits in version index (msb to lsb)
 * 1bit : Direct or Indirect
 * 15 bit : head index
 * 16 bit : tail index 
 */
typedef uint32_t PLeafVersionIndexData;
typedef PLeafVersionIndexData* PLeafVersionIndex;

typedef struct {
	/*
	 * Bits in version id (msb to lsb)
	 * 4byte : xmin
	 * 4byte : xmax
	 */
	PLeafVersionId version_id;
	/*
	 * Bits in version offset (msb to lsb)
	 * 4byte : segment id 
	 * 4byte : offset 
	 */
	PLeafVersionOffset version_offset;
} PLeafVersionData;

typedef PLeafVersionData* PLeafVersion;

#define N_VERSION_INDEX_ARR 60

#define ASSERT_ARR_IDX(array_index) \
	assert(((0 <= array_index) && (array_index <= 60)))

#define ASSERT_OFFSET(offset, cap) \
	do { \
		assert(offset != 0); \
		assert(((offset % PLEAF_PAGE_SIZE) - PLEAF_PAGE_HEADER_SIZE) >= 0); \
		assert((((offset % PLEAF_PAGE_SIZE) - PLEAF_PAGE_HEADER_SIZE) \
				% (cap * PLEAF_VERSION_DATA_SIZE)) == 0); \
	} while (0)

typedef struct {
	/*
	 * Bits in next page id (msb to lsb)
	 * 1byte:
	 * 1bit - not-used
	 * 3bit - capacity
	 * 4bit - instance no
	 *
	 * 3byte : not-used
	 *
	 * 4byte : page id
	 */
	PLeafPageMetadata next_page_id;

	/*
	 * Bitmap
	 * 0 : occupied, 1 : free
	 */
	PLeafBitmap bitmap;
	/* Index array */
	PLeafVersionIndexData index_array[N_VERSION_INDEX_ARR];
} PLeafPageHeaderData;

StaticAssertDecl(PLEAF_PAGE_HEADER_SIZE == sizeof(PLeafPageHeaderData),
				"PLeaf-page header size");

typedef PLeafPageHeaderData* PLeafPageHeader;

#define N_VERSION_DATA_ARR 240

typedef struct {
	PLeafPageHeaderData page_header;
	PLeafVersionData version_array[N_VERSION_DATA_ARR];
} PLeafPageData;

StaticAssertDecl(PLEAF_PAGE_SIZE == sizeof(PLeafPageData),
				"PLeaf-page size");

typedef PLeafPageData* PLeafPage;

extern void PLeafPageInitBitmap(PLeafPage page, int cap_index);

#define PLeafPageGetBitmap(page) \
	(&(((PLeafPageHeader)(page))->bitmap))

extern bool PLeafPageSetBitmap(PLeafPage page, 
									PLeafPageId page_id, 
									PLeafOffset* offset);

extern bool PLeafPageUnsetBitmap(PLeafPage page, 
									int array_index);

#define N_PAGE_CAP_ARR 7
#define PLEAF_MAX_CAPACITY 240

extern int PLeafGetProperCapacityIndex(int version_count);

extern int PLeafGetCapacity(int cap_index);

#define PLeafPageGetCapacityIndex(page) \
	((uint8_t)(((((PLeafPageHeader)(page))->next_page_id) >> 60) & PLEAF_PAGE_CAP_MASK))

extern int PLeafPageGetCapacity(PLeafPage page);

#define PLeafPageGetInstNo(page) \
	((uint8_t)(((((PLeafPageHeader)(page))->next_page_id) >> 56) & PLEAF_PAGE_INST_MASK))

extern void PLeafPageSetCapAndInstNo(PLeafPage page, 
										int cap, 
										int inst_no);

#define PLeafPageGetNextPageId(page) \
	((uint32_t)((((PLeafPageHeader)(page))->next_page_id) & PLEAF_PAGE_ID_MASK))

extern void PLeafPageSetNextPageId(PLeafPage page, 
										uint32_t page_id);

#define PLeafPageGetArrayIndex(cap, off) \
	(((off % PLEAF_PAGE_SIZE) - PLEAF_PAGE_HEADER_SIZE) / (cap * PLEAF_VERSION_DATA_SIZE))

#define PLeafPageGetVersionIndex(page, array_index) \
	(&(((PLeafPageHeader)(page))->index_array[array_index]))

#define PLeafPageGetFirstVersion(page, array_index, cap) \
	(&(page->version_array[array_index * cap]))

#define PLeafPageGetVersion(first_version, i) \
	(&(first_version[i]))

// JAESEON: MEMORY BARRIER
#define PLeafPageSetVersion(version, xmin, xmax, version_offset) \
	do { \
		version->version_offset = version_offset; \
		version->version_id = ((((uint64_t)xmin) << 32) | xmax); \
	} while (0)

#define PLeafPageSetVersionXmin(version, xmin) \
	(version->version_id = ((version->version_id & PLEAF_VERSION_ID_MASK) | \
													(((uint64_t)xmin) << 32)))

#define PLeafPageSetVersionXmax(version, xmax) \
	(version->version_id = ((version->version_id & ~PLEAF_VERSION_ID_MASK) | \
													xmax))

#define PLeafPageSetVersionOffset(version, offset) \
	(version->version_offset = offset)

#define PLeafSetVersionIndexType(version_index, flag) \
	(*version_index = ((*version_index & ~PLEAF_VERSION_TYPE_MASK) | flag))

#define PLeafSetVersionIndexHead(version_index, version_head) \
	(*version_index = ((version_head << 16) | \
										 (*version_index & \
											(PLEAF_VERSION_TYPE_MASK | PLEAF_VERSION_TAIL_MASK))))

#define PLeafSetVersionIndexTail(version_index, version_tail) \
	(*version_index = ((*version_index & ~PLEAF_VERSION_TAIL_MASK) | \
										 version_tail))

#define PLeafGetVersionId(version) \
	((uint64_t)(version->version_id))

#define PLeafGetVersionOffset(version) \
	((uint64_t)(version->version_offset))

#define PLeafGetXmin(version_id) \
	((uint32_t)((version_id >> 32) & PLEAF_VERSION_ID_MASK))

#define PLeafGetXmax(version_id) \
	((uint32_t)(version_id & PLEAF_VERSION_ID_MASK))

#define PLeafGetVersionType(version_index_data) \
	(version_index_data & PLEAF_VERSION_TYPE_MASK)

#define PLeafGetVersionIndexHead(version_index_data) \
	((version_index_data & PLEAF_VERSION_HEAD_MASK) >> 16)

#define PLeafGetVersionIndexTail(version_index_data) \
	(version_index_data & PLEAF_VERSION_TAIL_MASK)

extern int PLeafGetProperCapacityIndex(int version_count);

#endif /* PLEAF_BUFPAGE_H */
