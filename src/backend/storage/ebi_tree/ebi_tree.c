/*-------------------------------------------------------------------------
 *
 * ebi_tree.c
 *
 * EBI Tree Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/ebi_tree/ebi_tree.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM
#include "postgres.h"

#include "postmaster/ebi_tree_process.h"
#include "storage/ebi_tree.h"
#include "storage/ebi_tree_buf.h"
#include "storage/mpsc_queue.h"
#include "utils/dynahash.h"
#include "utils/snapmgr.h"

#define NUM_VERSIONS_PER_CHUNK 1000

/* src/include/postmaster/ebitree_process.h */
dsa_area* ebitree_dsa_area;

/* Prototypes for private functions */

/* Allocation */
static dsa_pointer CreateNodeWithHeight(dsa_area* area, uint32 height);
static dsa_pointer CreateNode(dsa_area* area);
static void DeleteNode(EbiTree ebitree, EbiNode node);

/* Insertion */
static dsa_pointer FindInsertionTargetNode(EbiTree ebitree);

/* Deletion */
static void UnlinkFromParent(EbiNode node);
static void PushToGarbageQueue(dsa_pointer dsa_node);
static void CompactNode(EbiTree ebitree, EbiNode node);

/* Reference Counting */
static uint32 IncreaseRefCount(EbiNode node);
static uint32 DecreaseRefCount(EbiNode node);

static void SetLeftBoundary(EbiNode node, Snapshot snapshot);
static void SetRightBoundary(EbiNode node, Snapshot snapshot);
static dsa_pointer SetRightBoundaryRecursive(
    dsa_pointer dsa_node,
    Snapshot snapshot);

/* DSA based version of CopySnapshot in snapmgr.c */
static dsa_pointer DsaCopySnapshot(Snapshot snapshot);

/* Utility */
static EbiTree ConvertToEbiTree(dsa_area* area, dsa_pointer ptr);
static EbiNode ConvertToEbiNode(dsa_area* area, dsa_pointer ptr);

static bool HasParent(EbiNode node);
static bool HasLeftChild(EbiNode node);
static bool IsLeftChild(EbiNode node);
static bool IsLeaf(EbiNode node);

static dsa_pointer Sibling(EbiNode node);

static bool Overlaps(EbiNode node, TransactionId vmin, TransactionId vmax);

dsa_pointer
InitEbiTree(dsa_area* area) {
  dsa_pointer dsa_ebitree, dsa_sentinel;
  EbiTree ebitree;

  dsa_ebitree =
      dsa_allocate_extended(area, sizeof(EbiTreeData), DSA_ALLOC_ZERO);
  ebitree = ConvertToEbiTree(area, dsa_ebitree);

  dsa_sentinel = CreateNode(area);

  ebitree->root = dsa_sentinel;
  ebitree->recent_node = dsa_sentinel;

  return dsa_ebitree;
}

dsa_pointer
CreateNode(dsa_area* area) {
  return CreateNodeWithHeight(area, 0);
}

dsa_pointer
CreateNodeWithHeight(dsa_area* area, uint32 height) {
  dsa_pointer pointer;
  EbiNode node;

  /* Allocate memory in dsa */
  pointer = dsa_allocate_extended(area, sizeof(EbiNodeData), DSA_ALLOC_ZERO);
  node = ConvertToEbiNode(area, pointer);

  Assert(node != NULL);

  /* Initial values */
  node->parent = InvalidDsaPointer;
  node->left = InvalidDsaPointer;
  node->right = InvalidDsaPointer;
  node->proxy = InvalidDsaPointer;
  node->height = height;
  pg_atomic_init_u32(&node->refcnt, 0);
  node->left_boundary = InvalidDsaPointer;
  node->right_boundary = InvalidDsaPointer;

  /* Initialize file segment */
  node->seg_id =
      EbiTreeShmem->seg_id++;  // the dedicated thread, alone, creates nodes
  EbiTreeCreateSegmentFile(node->seg_id);
  pg_atomic_init_u32(&node->seg_offset, 0);

  /* Version counter */
  pg_atomic_init_u64(&node->num_versions, 0);

  return pointer;
}

void
DeleteEbiTree(dsa_pointer dsa_ebitree) {
  if (!DsaPointerIsValid(dsa_ebitree)) {
    return;
  }

  // Wait until all transactions leave

  // Delete each node
}

bool
NeedsNewNode(dsa_pointer dsa_ebitree) {
  EbiTree ebitree;
  EbiNode recent_node;
  bool ret;

  ebitree = ConvertToEbiTree(ebitree_dsa_area, dsa_ebitree);
  recent_node = ConvertToEbiNode(ebitree_dsa_area, ebitree->recent_node);

  // TODO: condition may change corresponding to the unit of epoch.
  ret = DsaPointerIsValid(recent_node->left_boundary);

  return ret;
}

void
InsertNode(dsa_pointer dsa_ebitree) {
  EbiTree ebitree;

  dsa_pointer dsa_target;
  dsa_pointer dsa_new_parent;
  dsa_pointer dsa_new_leaf;

  EbiNode target;
  EbiNode new_parent;
  EbiNode new_leaf;
  Snapshot snap;

  ebitree = ConvertToEbiTree(ebitree_dsa_area, dsa_ebitree);

  /*
   * Find the target ebi_node to perform insertion, make a new parent for the
   * target ebi_node and set its right to the newly inserted node
   *
   *   new_parent
   *    /      \
   * target  new_leaf
   *
   */
  dsa_target = FindInsertionTargetNode(ebitree);
  target = ConvertToEbiNode(ebitree_dsa_area, dsa_target);

  dsa_new_parent = CreateNodeWithHeight(ebitree_dsa_area, target->height + 1);
  new_parent = ConvertToEbiNode(ebitree_dsa_area, dsa_new_parent);

  dsa_new_leaf = CreateNode(ebitree_dsa_area);
  new_leaf = ConvertToEbiNode(ebitree_dsa_area, dsa_new_leaf);

  // Set the left bound of the new parent to the left child's left bound.
  // TODO: snapshot allocation with DSA
  // (before) new_parent->left_boundary = target->left_boundary;
  // (after) SetLeftBoundary(new_parent, target->left_boundary);
  snap = (Snapshot)dsa_get_address(ebitree_dsa_area, target->left_boundary);
  SetLeftBoundary(new_parent, snap);

  /*
   * Connect the original parent as the new parent's parent.
   * In the figure below, connecting nodes 'a' and 'f'.
   * (e = target, f = new_parent, g = new_leaf)
   *
   *     a                   a
   *    / \                 / \
   *   b   \       ->      b   f
   *  / \   \             / \ / \
   * c   d   e           c  d e  g
   *
   */
  if (HasParent(target)) {
    new_parent->parent = target->parent;
  }
  // f->e, f->g
  new_parent->left = dsa_target;
  new_parent->right = dsa_new_leaf;

  /*
   * At this point, the new nodes('f' and 'g') are not visible
   * since they are not connected to the original tree.
   */
  pg_memory_barrier();

  // a->f
  if (HasParent(target)) {
    EbiNode tmp;
    tmp = ConvertToEbiNode(ebitree_dsa_area, target->parent);
    tmp->right = dsa_new_parent;
  }
  // e->f, g->f
  target->parent = dsa_new_parent;
  new_leaf->parent = dsa_new_parent;

  // If the target node is root, the root is changed to the new parent.
  if (target == ConvertToEbiNode(ebitree_dsa_area, ebitree->root)) {
    ebitree->root = dsa_new_parent;
  }

  pg_memory_barrier();

  // Change the last leaf node to the new right node.
  ebitree->recent_node = dsa_new_leaf;
}

static dsa_pointer
FindInsertionTargetNode(EbiTree ebitree) {
  dsa_pointer dsa_tmp;
  dsa_pointer dsa_parent;

  EbiNode tmp;
  EbiNode parent;
  EbiNode left;
  EbiNode right;

  dsa_tmp = ebitree->recent_node;
  tmp = ConvertToEbiNode(ebitree_dsa_area, dsa_tmp);

  dsa_parent = tmp->parent;
  parent = ConvertToEbiNode(ebitree_dsa_area, dsa_parent);

  while (parent != NULL) {
    left = ConvertToEbiNode(ebitree_dsa_area, parent->left);
    right = ConvertToEbiNode(ebitree_dsa_area, parent->right);

    if (left->height > right->height) {
      // Unbalanced, found target node.
      break;
    } else {
      dsa_tmp = dsa_parent;
      tmp = ConvertToEbiNode(ebitree_dsa_area, dsa_tmp);
      dsa_parent = tmp->parent;
      parent = ConvertToEbiNode(ebitree_dsa_area, dsa_parent);
    }
  }

  return dsa_tmp;
}

/**
 * Reference Counting
 */

dsa_pointer
EbiIncreaseRefCount(Snapshot snapshot) {
  EbiTree tree;
  dsa_pointer dsa_recent_node, dsa_sibling, dsa_prev_node;
  EbiNode recent_node;
  EbiNode sibling;
  uint32 refcnt;

  tree = ConvertToEbiTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
  dsa_recent_node = tree->recent_node;
  recent_node = ConvertToEbiNode(ebitree_dsa_area, dsa_recent_node);

  refcnt = IncreaseRefCount(recent_node);

  // The first one to enter the current node should set the boundary.
  if (refcnt == 1) {
    // The next epoch's opening transaction will decrease ref count twice.
    refcnt = IncreaseRefCount(recent_node);

    dsa_sibling = Sibling(recent_node);
    sibling = ConvertToEbiNode(ebitree_dsa_area, dsa_sibling);

    SetLeftBoundary(recent_node, snapshot);

    // When the initial root node stands alone, sibling could be NULL.
    if (sibling != NULL) {
      SetRightBoundary(sibling, snapshot);
      pg_memory_barrier();
      dsa_prev_node = SetRightBoundaryRecursive(dsa_sibling, snapshot);

      // May delete the last recent node if there's no presence of any xacts.
      EbiDecreaseRefCount(dsa_prev_node);
    }
  }

  return dsa_recent_node;
}

static uint32
IncreaseRefCount(EbiNode node) {
  uint32 ret;
  ret = pg_atomic_add_fetch_u32(&node->refcnt, 1);
  return ret;
}

static void
SetLeftBoundary(EbiNode node, Snapshot snapshot) {
  node->left_boundary = DsaCopySnapshot(snapshot);
}

static void
SetRightBoundary(EbiNode node, Snapshot snapshot) {
  node->right_boundary = DsaCopySnapshot(snapshot);
}

/* DSA version of CopySnapshot in snapmgr.c */
static dsa_pointer
DsaCopySnapshot(Snapshot snapshot) {
  dsa_pointer dsa_newsnap;
  Snapshot newsnap;
  Size subxipoff;
  Size size;

  Assert(snapshot != InvalidSnapshot);

  size = subxipoff =
      sizeof(SnapshotData) + snapshot->xcnt * sizeof(TransactionId);
  if (snapshot->subxcnt > 0) size += snapshot->subxcnt * sizeof(TransactionId);

  /* Allocate DSA */
  dsa_newsnap = dsa_allocate_extended(ebitree_dsa_area, size, DSA_ALLOC_ZERO);
  newsnap = (Snapshot)dsa_get_address(ebitree_dsa_area, dsa_newsnap);
  memcpy(newsnap, snapshot, sizeof(SnapshotData));

  newsnap->regd_count = 0;
  newsnap->active_count = 0;
  newsnap->copied = true;

  if (snapshot->xcnt > 0) {
    newsnap->xip = (TransactionId*)(newsnap + 1);
    memcpy(newsnap->xip, snapshot->xip, snapshot->xcnt * sizeof(TransactionId));
  } else
    newsnap->xip = NULL;

  if (snapshot->subxcnt > 0 &&
      (!snapshot->suboverflowed || snapshot->takenDuringRecovery)) {
    newsnap->subxip = (TransactionId*)((char*)newsnap + subxipoff);
    memcpy(
        newsnap->subxip,
        snapshot->subxip,
        snapshot->subxcnt * sizeof(TransactionId));
  } else
    newsnap->subxip = NULL;

  return dsa_newsnap;
}

static dsa_pointer
SetRightBoundaryRecursive(dsa_pointer dsa_node, Snapshot snapshot) {
  EbiNode tmp;
  dsa_pointer ret;

  ret = dsa_node;
  tmp = ConvertToEbiNode(ebitree_dsa_area, ret);

  while (DsaPointerIsValid(tmp->right)) {
    ret = tmp->right;
    tmp = ConvertToEbiNode(ebitree_dsa_area, ret);
    SetRightBoundary(tmp, snapshot);
  }

  return ret;
}

void
EbiDecreaseRefCount(dsa_pointer dsa_node) {
  EbiNode node;
  uint32 refcnt;

  node = ConvertToEbiNode(ebitree_dsa_area, dsa_node);
  refcnt = DecreaseRefCount(node);

  if (refcnt == 0) {
    Enqueue(ebitree_dsa_area, EbiTreeShmem->unlink_queue, dsa_node);
  }
}

static uint32
DecreaseRefCount(EbiNode node) {
  uint32 ret;
  ret = pg_atomic_sub_fetch_u32(&node->refcnt, 1);
  return ret;
}

void
DeleteNodes(dsa_pointer dsa_ebitree, dsa_pointer unlink_queue) {
  dsa_pointer dsa_tmp;
  EbiNode tmp;
  EbiTree ebitree;

  ebitree = ConvertToEbiTree(ebitree_dsa_area, dsa_ebitree);

  dsa_tmp = Dequeue(ebitree_dsa_area, unlink_queue);

  while (DsaPointerIsValid(dsa_tmp)) {
    tmp = ConvertToEbiNode(ebitree_dsa_area, dsa_tmp);

    // Logical deletion
    DeleteNode(ebitree, tmp);

    // Prepare it for physical deletion
    PushToGarbageQueue(dsa_tmp);

    dsa_tmp = Dequeue(ebitree_dsa_area, unlink_queue);
  }
}

static void
DeleteNode(EbiTree ebitree, EbiNode node) {
  EbiNode tmp;

  // Logical deletion, takes it off from the EBI-tree
  UnlinkFromParent(node);

  // Compaction
  CompactNode(ebitree, ConvertToEbiNode(ebitree_dsa_area, node->parent));

  // Remove the segment file related to the node
  EbiTreeRemoveSegmentFile(node->seg_id);

  // Remove proxy nodes' segment files
  tmp = node;
  while (DsaPointerIsValid(tmp->proxy)) {
    tmp = ConvertToEbiNode(ebitree_dsa_area, tmp->proxy);
    EbiTreeRemoveSegmentFile(tmp->seg_id);
  }
}

static void
UnlinkFromParent(EbiNode node) {
  EbiNode parent;
  uint64 num_versions;

  parent = ConvertToEbiNode(ebitree_dsa_area, node->parent);

  if (IsLeftChild(node)) {
    parent->left = InvalidDsaPointer;
  } else {
    parent->right = InvalidDsaPointer;
  }

  /* Version counter */
  num_versions = pg_atomic_read_u64(&node->num_versions);
  num_versions = num_versions - (num_versions % NUM_VERSIONS_PER_CHUNK);
  pg_atomic_sub_fetch_u64(&EbiTreeShmem->num_versions, num_versions);
}

static void
PushToGarbageQueue(dsa_pointer dsa_node) {
  Enqueue(ebitree_dsa_area, EbiTreeShmem->delete_queue, dsa_node);
}

static void
CompactNode(EbiTree ebitree, EbiNode node) {
  uint32 original_height;

  if (HasParent(node) == false) {
    EbiNode root;

    // When the root's child is being compacted
    if (HasLeftChild(node)) {
      ebitree->root = node->left;
    } else {
      ebitree->root = node->right;
    }
    root = ConvertToEbiNode(ebitree_dsa_area, ebitree->root);
    root->parent = InvalidDsaPointer;
  } else {
    EbiNode parent;
    EbiNode tmp;
    dsa_pointer tmp_ptr;

    parent = ConvertToEbiNode(ebitree_dsa_area, node->parent);

    // Compact the one-and-only child and its parent
    if (IsLeftChild(node)) {
      if (HasLeftChild(node)) {
        parent->left = node->left;
      } else {
        parent->left = node->right;
      }
      tmp = ConvertToEbiNode(ebitree_dsa_area, parent->left);
    } else {
      if (HasLeftChild(node)) {
        parent->right = node->left;
      } else {
        parent->right = node->right;
      }
      tmp = ConvertToEbiNode(ebitree_dsa_area, parent->right);
    }
    tmp->parent = node->parent;

    // Parent height propagation
    tmp_ptr = node->parent;
    while (DsaPointerIsValid(tmp_ptr)) {
      EbiNode curr, left, right;

      curr = ConvertToEbiNode(ebitree_dsa_area, tmp_ptr);
      left = ConvertToEbiNode(ebitree_dsa_area, curr->left);
      right = ConvertToEbiNode(ebitree_dsa_area, curr->right);

      original_height = curr->height;

      curr->height = Max(left->height, right->height) + 1;

      if (curr->height == original_height) {
        break;
      }

      tmp_ptr = curr->parent;
    }
  }

  // TODO: compacted_next should be set for GC later on
}

void
DeleteSegments(dsa_pointer delete_queue) {
  dsa_pointer dsa_tmp;
  EbiNode tmp;
  TransactionId oldest_active_xid;

  Assert(DsaPointerIsValid(delete_queue));

#if 0
  oldest_active_xid = EbiTreeGetOldestActiveTransactionId();

  dsa_tmp = Dequeue(ebitree_dsa_area, delete_queue);

  while (DsaPointerIsValid(dsa_tmp)) {
    tmp = ConvertToEbiNode(ebitree_dsa_area, dsa_tmp);

    EbiTreeRemoveSegmentFile(tmp->seg_id);

    dsa_tmp = Dequeue(ebitree_dsa_area, delete_queue);
  }

  PLeafMeta* meta;
  int old_index;

  oldest_active_xid = PLeafGetOldestActiveTransactionId();

  if (!((oldest_active_xid == MyMaxTransactionId) ||
        (meta->generation_max_xid <= oldest_active_xid)))
    return;

  /*
   * XXX:
   * It is important to acquire exclusive latch to solve race condition
   * between PLeafWritePage() and PLeafCleanOldGeneration().
   */
  LWLockAcquire(&PLeafBufferIOLWLockArray[old_index].lock, LW_EXCLUSIVE);
  meta->generation_numbers[old_index] = 0;
  LWLockRelease(&PLeafBufferIOLWLockArray[old_index].lock);

  /* Initialize stacks in an old pool */
  PLeafCleanOldStacks(old_index);

  /* Initialize old file */
  PLeafCleanOldFile(old_index);
#endif
}

EbiNode
Sift(TransactionId vmin, TransactionId vmax) {
  EbiTree ebitree;
  EbiNode curr, left, right;
  bool left_includes, right_includes;
  bool left_exists, right_exists;

  ebitree = ConvertToEbiTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
  curr = ConvertToEbiNode(ebitree_dsa_area, ebitree->root);

  Assert(curr != NULL);

  /* If root's left boundary doesn't set yet, return immediately */
  if (!DsaPointerIsValid(curr->left_boundary)) return NULL;

  /* The version is already dead, may be cleaned */
  if (!Overlaps(curr, vmin, vmax)) return NULL;

  while (!IsLeaf(curr)) {
    left = ConvertToEbiNode(ebitree_dsa_area, curr->left);
    right = ConvertToEbiNode(ebitree_dsa_area, curr->right);

    left_exists = ((left != NULL) && DsaPointerIsValid(left->left_boundary));
    right_exists = ((right != NULL) && DsaPointerIsValid(right->left_boundary));

    if (!left_exists && !right_exists) {
      return NULL;
    } else if (!left_exists) {
      // Only the left is null and the version does not fit into the right
      if (!Overlaps(right, vmin, vmax)) {
        return NULL;
      } else {
        curr = ConvertToEbiNode(ebitree_dsa_area, curr->right);
      }
    } else if (!right_exists) {
      // Only the right is null and the version does not fit into the left
      if (!Overlaps(left, vmin, vmax)) {
        return NULL;
      } else {
        curr = ConvertToEbiNode(ebitree_dsa_area, curr->left);
      }
    } else {
      // Both are not null
      left_includes = Overlaps(left, vmin, vmax);
      right_includes = Overlaps(right, vmin, vmax);

      if (left_includes && right_includes) {
        // Overlaps both child, current interval is where it fits
        break;
      } else if (left_includes) {
        curr = ConvertToEbiNode(ebitree_dsa_area, curr->left);
      } else if (right_includes) {
        curr = ConvertToEbiNode(ebitree_dsa_area, curr->right);
      } else {
        return NULL;
      }
    }
  }

  return curr;
}

static bool
Overlaps(EbiNode node, TransactionId vmin, TransactionId vmax) {
  Snapshot left_snap, right_snap;

  left_snap = (Snapshot)dsa_get_address(ebitree_dsa_area, node->left_boundary);
  right_snap =
      (Snapshot)dsa_get_address(ebitree_dsa_area, node->right_boundary);

  Assert(left_snap != NULL);

  if (right_snap != NULL)
    return XidInMVCCSnapshot(vmax, left_snap) &&
           !XidInMVCCSnapshot(vmin, right_snap);
  else
    return XidInMVCCSnapshot(vmax, left_snap);
}

EbiTreeVersionOffset
EbiTreeSiftAndBind(
    TransactionId vmin,
    TransactionId vmax,
    Size tuple_size,
    const void* tuple,
    LWLock* rwlock) {
  EbiNode node;
  EbiTreeSegmentId seg_id;
  EbiTreeSegmentOffset seg_offset;
  Size aligned_tuple_size;
  EbiTreeVersionOffset ret;
  uint64 num_versions;

  Assert(ebitree_dsa_area != NULL);

  node = Sift(vmin, vmax);

  if (node == NULL) {
    /* Reclaimable */
    return EBI_TREE_INVALID_VERSION_OFFSET;
  }

  aligned_tuple_size = 1 << my_log2(tuple_size);

  seg_id = node->seg_id;
  seg_offset = pg_atomic_fetch_add_u32(&node->seg_offset, aligned_tuple_size);

  // Write version to segment
  EbiTreeAppendVersion(seg_id, seg_offset, tuple_size, tuple, rwlock);

  num_versions = pg_atomic_fetch_add_u64(&node->num_versions, 1);

  // Update global counter if necessary
  if (num_versions % NUM_VERSIONS_PER_CHUNK == 0) {
    pg_atomic_fetch_add_u64(
        &EbiTreeShmem->num_versions, NUM_VERSIONS_PER_CHUNK);
  }

  ret = EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset);

  return ret;
}

int
EbiTreeLookupVersion(
    EbiTreeVersionOffset version_offset,
    Size tuple_size,
    void** ret_value) {
  EbiTreeSegmentId seg_id;
  EbiTreeSegmentOffset seg_offset;
  int buf_id;

  seg_id = EBI_TREE_VERSION_OFFSET_TO_SEG_ID(version_offset);
  seg_offset = EBI_TREE_VERSION_OFFSET_TO_SEG_OFFSET(version_offset);

  Assert(seg_id >= 1);
  Assert(seg_offset >= 0);

  // Read version to ret_value
  buf_id = EbiTreeReadVersionRef(seg_id, seg_offset, tuple_size, ret_value);

  return buf_id;
}

/**
 * Utility Functions
 */

static EbiTree
ConvertToEbiTree(dsa_area* area, dsa_pointer ptr) {
  return (EbiTree)dsa_get_address(area, ptr);
}

static EbiNode
ConvertToEbiNode(dsa_area* area, dsa_pointer ptr) {
  return (EbiNode)dsa_get_address(area, ptr);
}

static bool
HasParent(EbiNode node) {
  return DsaPointerIsValid(node->parent);
}

static bool
IsLeftChild(EbiNode node) {
  EbiNode parent = ConvertToEbiNode(ebitree_dsa_area, node->parent);
  return node == ConvertToEbiNode(ebitree_dsa_area, parent->left);
}

static bool
HasLeftChild(EbiNode node) {
  return DsaPointerIsValid(node->left);
}

static bool
IsLeaf(EbiNode node) {
  return node->height == 0;
}

static dsa_pointer
Sibling(EbiNode node) {
  if (HasParent(node)) {
    EbiNode parent = ConvertToEbiNode(ebitree_dsa_area, node->parent);
    if (IsLeftChild(node)) {
      return parent->right;
    } else {
      return parent->left;
    }
  } else {
    return InvalidDsaPointer;
  }
}

void
PrintEbiTree(dsa_pointer dsa_ebitree) {
  EbiTree ebitree;
  EbiNode root;

  ebitree = ConvertToEbiTree(ebitree_dsa_area, dsa_ebitree);
  root = ConvertToEbiNode(ebitree_dsa_area, ebitree->root);

  ereport(LOG, (errmsg("Print Tree")));
  PrintEbiTreeRecursive(root);
}

void
PrintEbiTreeRecursive(EbiNode node) {
  if (node == NULL) {
    return;
  }
  PrintEbiTreeRecursive(ConvertToEbiNode(ebitree_dsa_area, node->left));
  ereport(
      LOG,
      (errmsg(
          "%p, %d, %d | %ld, %ld",
          node,
          node->height,
          pg_atomic_read_u32(&node->refcnt),
          node->left_boundary,
          node->right_boundary)));
  PrintEbiTreeRecursive(ConvertToEbiNode(ebitree_dsa_area, node->right));
}
#endif
