/*-------------------------------------------------------------------------
 *
 * ebi_tree_bufpage.h
 *	  EBI Tree Buffer Page
 *
 *
 *
 * src/include/storage/ebi_tree_bufpage.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef EBI_TREE_BUFPAGE_H
#define EBI_TREE_BUFPAGE_H

/*
 * Offset of the ebi tree
 * +----------+----------+
 * |  fileno  |  offset  |
 * +----------+----------+
 */
typedef uint64_t EbiTreeVersionOffset;

#define INVALID_EBI_TREE_VERSION_OFFSET ((EbiTreeVersionOffset)(-1));

#endif /* EBI_TREE_BUFPAGE_H */
