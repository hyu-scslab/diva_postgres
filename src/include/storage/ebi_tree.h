/*-------------------------------------------------------------------------
 *
 * ebi_tree.h
 *	  EBI Tree
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
#include "utils/dsa.h"
#include "utils/snapshot.h"

/*
 * Actual EBI tree structure.
 * */

typedef struct EbiNodeData {
  dsa_pointer parent;
  dsa_pointer left;
  dsa_pointer right;

  uint32 height;
  pg_atomic_uint32 refcnt;

  dsa_pointer left_boundary;
  dsa_pointer right_boundary;
} EbiNodeData;

typedef struct EbiNodeData* EbiNode;

typedef struct EbiTreeData {
  dsa_pointer root;
  dsa_pointer recent_node;
} EbiTreeData;

typedef struct EbiTreeData* EbiTree;

/* Public functions */
extern dsa_pointer EbiIncreaseRefCount(Snapshot snapshot);
extern void EbiDecreaseRefCount(dsa_pointer node);

extern dsa_pointer InitEbiTree(dsa_area* area);
extern void DeleteEbiTree(dsa_pointer dsa_ebitree);

extern void InsertNode(dsa_pointer dsa_ebitree);
extern void DeleteNodes(dsa_pointer dsa_ebitree);

extern bool NeedsNewNode(dsa_pointer dsa_ebitree);

extern bool Sift(TransactionId vmin, TransactionId vmax);

/* Tree Modification */
void DeleteNode(EbiTree ebitree, EbiNode node);

/* Debug */
void PrintEbiTree(dsa_pointer dsa_ebitree);
void PrintEbiTreeRecursive(EbiNode node);

#endif /* EBI_TREE_H */
