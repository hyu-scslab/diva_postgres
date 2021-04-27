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
#include "storage/ebi_tree_utils.h"
#include "storage/procarray.h"
#include "utils/dynahash.h"
#include "utils/snapmgr.h"

#define NUM_VERSIONS_PER_CHUNK 1000

/* src/include/postmaster/ebitree_process.h */
dsa_area* ebitree_dsa_area;

/* Prototypes for private functions */

/* Allocation */
static dsa_pointer EbiCreateNodeWithHeight(dsa_area* area, uint32 height);
static dsa_pointer EbiCreateNode(dsa_area* area);

/* Insertion */
static dsa_pointer EbiFindInsertionTargetNode(EbiTree ebitree);

/* Deletion */
static void
EbiUnlinkNode(EbiTree ebitree, dsa_pointer dsa_node, EbiSpscQueue delete_queue);
static void EbiUnlinkFromParent(EbiNode node);
static void EbiPushToGarbageQueue(
    EbiSpscQueue delete_queue,
    EbiNode node,
    dsa_pointer dsa_node);
static void EbiCompactNode(EbiTree ebitree, dsa_pointer dsa_node);
static void EbiDeleteTreeRecursive(EbiNode node, dsa_pointer dsa_node);

static void EbiLinkProxy(dsa_pointer dsa_proxy, dsa_pointer dsa_proxy_target);

/* Reference Counting */
static uint32 EbiIncreaseRefCountInternal(EbiNode node);
static uint32 EbiDecreaseRefCountInternal(EbiNode node);

static void EbiSetLeftBoundary(EbiNode node, Snapshot snapshot);
static void EbiSetRightBoundary(EbiNode node, Snapshot snapshot);
static dsa_pointer EbiSetRightBoundaryRecursive(
    dsa_pointer dsa_node,
    Snapshot snapshot);

/* External function's internal implementation */
static EbiNode EbiSiftInternal(TransactionId vmin, TransactionId vmax);
static bool EbiSegIsAliveInternal(
    dsa_pointer dsa_ebitree,
    EbiTreeSegmentId seg_id);

/* DSA based version of CopySnapshot in snapmgr.c */
static dsa_pointer EbiDsaCopySnapshot(Snapshot snapshot);

/* Utility */
static bool EbiHasParent(EbiNode node);
static bool EbiHasLeftChild(EbiNode node);
static bool EbiIsLeftChild(EbiNode node);
static bool EbiIsLeaf(EbiNode node);

static dsa_pointer EbiSibling(EbiNode node);

static bool EbiOverlaps(EbiNode node, TransactionId vmin, TransactionId vmax);

dsa_pointer
EbiInitTree(dsa_area* area) {
  dsa_pointer dsa_ebitree, dsa_sentinel;
  EbiTree ebitree;

  dsa_ebitree =
      dsa_allocate_extended(area, sizeof(EbiTreeData), DSA_ALLOC_ZERO);
  ebitree = EbiConvertToTree(area, dsa_ebitree);

  dsa_sentinel = EbiCreateNode(area);

  ebitree->root = dsa_sentinel;
  ebitree->recent_node = dsa_sentinel;

  return dsa_ebitree;
}

static dsa_pointer
EbiCreateNode(dsa_area* area) {
  return EbiCreateNodeWithHeight(area, 0);
}

static dsa_pointer
EbiCreateNodeWithHeight(dsa_area* area, uint32 height) {
  dsa_pointer pointer;
  EbiNode node;

  /* Allocate memory in dsa */
  pointer = dsa_allocate_extended(area, sizeof(EbiNodeData), DSA_ALLOC_ZERO);
  node = EbiConvertToNode(area, pointer);

  Assert(node != NULL);

  /* Initial values */
  node->parent = InvalidDsaPointer;
  node->left = InvalidDsaPointer;
  node->right = InvalidDsaPointer;
  node->proxy_target = InvalidDsaPointer;
  node->height = height;
  pg_atomic_init_u32(&node->refcnt, 0);
  node->left_boundary = InvalidDsaPointer;
  node->right_boundary = InvalidDsaPointer;

  /* Initialize file segment */
  node->seg_id =
      ++EbiTreeShmem->seg_id;  // the dedicated thread, alone, creates nodes

  Assert(node->seg_id != 0);

  EbiTreeCreateSegmentFile(node->seg_id);
  pg_atomic_init_u32(&node->seg_offset, 0);

  /* Version counter */
  pg_atomic_init_u64(&node->num_versions, 0);

  return pointer;
}

void
EbiDeleteTree(dsa_pointer dsa_ebitree) {
  EbiTree ebitree;
  dsa_pointer dsa_root;
  EbiNode root;

  if (!DsaPointerIsValid(dsa_ebitree)) {
    return;
  }

  ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

  dsa_root = ebitree->root;
  root = EbiConvertToNode(ebitree_dsa_area, dsa_root);

  /* Delete remaining nodes */
  EbiDeleteTreeRecursive(root, dsa_root);

  dsa_free(ebitree_dsa_area, dsa_ebitree);
}

static void
EbiDeleteTreeRecursive(EbiNode node, dsa_pointer dsa_node) {
  if (node == NULL) {
    return;
  }

  EbiDeleteTreeRecursive(
      EbiConvertToNode(ebitree_dsa_area, node->left), node->left);
  EbiDeleteTreeRecursive(
      EbiConvertToNode(ebitree_dsa_area, node->right), node->right);
  EbiDeleteNode(node, dsa_node);
}

bool
EbiNeedsNewNode(dsa_pointer dsa_ebitree) {
  EbiTree ebitree;
  EbiNode recent_node;
  bool ret;

  ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);
  recent_node = EbiConvertToNode(ebitree_dsa_area, ebitree->recent_node);

  ret = DsaPointerIsValid(recent_node->left_boundary);

  return ret;
}

void
EbiInsertNode(dsa_pointer dsa_ebitree) {
  EbiTree ebitree;

  dsa_pointer dsa_target;
  dsa_pointer dsa_new_parent;
  dsa_pointer dsa_new_leaf;

  EbiNode target;
  EbiNode new_parent;
  EbiNode new_leaf;
  Snapshot snap;

  ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

  /*
   * Find the target ebi_node to perform insertion, make a new parent for the
   * target ebi_node and set its right to the newly inserted node
   *
   *   new_parent
   *    /      \
   * target  new_leaf
   *
   */
  dsa_target = EbiFindInsertionTargetNode(ebitree);
  target = EbiConvertToNode(ebitree_dsa_area, dsa_target);

  dsa_new_parent =
      EbiCreateNodeWithHeight(ebitree_dsa_area, target->height + 1);
  new_parent = EbiConvertToNode(ebitree_dsa_area, dsa_new_parent);

  dsa_new_leaf = EbiCreateNode(ebitree_dsa_area);
  new_leaf = EbiConvertToNode(ebitree_dsa_area, dsa_new_leaf);

  // Set the left bound of the new parent to the left child's left bound.
  snap = (Snapshot)dsa_get_address(ebitree_dsa_area, target->left_boundary);
  EbiSetLeftBoundary(new_parent, snap);

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
  if (EbiHasParent(target)) {
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
  if (EbiHasParent(target)) {
    EbiNode tmp;
    tmp = EbiConvertToNode(ebitree_dsa_area, target->parent);
    tmp->right = dsa_new_parent;
  }
  // e->f, g->f
  target->parent = dsa_new_parent;
  new_leaf->parent = dsa_new_parent;

  // If the target node is root, the root is changed to the new parent.
  if (target == EbiConvertToNode(ebitree_dsa_area, ebitree->root)) {
    ebitree->root = dsa_new_parent;
  }

  pg_memory_barrier();

  // Change the last leaf node to the new right node.
  ebitree->recent_node = dsa_new_leaf;
}

static dsa_pointer
EbiFindInsertionTargetNode(EbiTree ebitree) {
  dsa_pointer dsa_tmp;
  dsa_pointer dsa_parent;

  EbiNode tmp;
  EbiNode parent;
  EbiNode left;
  EbiNode right;

  dsa_tmp = ebitree->recent_node;
  tmp = EbiConvertToNode(ebitree_dsa_area, dsa_tmp);

  dsa_parent = tmp->parent;
  parent = EbiConvertToNode(ebitree_dsa_area, dsa_parent);

  while (parent != NULL) {
    left = EbiConvertToNode(ebitree_dsa_area, parent->left);
    right = EbiConvertToNode(ebitree_dsa_area, parent->right);

    if (left->height > right->height) {
      // Unbalanced, found target node.
      break;
    } else {
      dsa_tmp = dsa_parent;
      tmp = EbiConvertToNode(ebitree_dsa_area, dsa_tmp);
      dsa_parent = tmp->parent;
      parent = EbiConvertToNode(ebitree_dsa_area, dsa_parent);
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

  tree = EbiConvertToTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
  dsa_recent_node = tree->recent_node;
  recent_node = EbiConvertToNode(ebitree_dsa_area, dsa_recent_node);

  refcnt = EbiIncreaseRefCountInternal(recent_node);

  // The first one to enter the current node should set the boundary.
  if (refcnt == 1) {
    // The next epoch's opening transaction will decrease ref count twice.
    refcnt = EbiIncreaseRefCountInternal(recent_node);

    dsa_sibling = EbiSibling(recent_node);
    sibling = EbiConvertToNode(ebitree_dsa_area, dsa_sibling);

    EbiSetLeftBoundary(recent_node, snapshot);

    // When the initial root node stands alone, sibling could be NULL.
    if (sibling != NULL) {
      EbiSetRightBoundary(sibling, snapshot);
      pg_memory_barrier();
      dsa_prev_node = EbiSetRightBoundaryRecursive(dsa_sibling, snapshot);

      // May delete the last recent node if there's no presence of any xacts.
      EbiDecreaseRefCount(dsa_prev_node);
    }
  }

  return dsa_recent_node;
}

static uint32
EbiIncreaseRefCountInternal(EbiNode node) {
  uint32 ret;
  ret = pg_atomic_add_fetch_u32(&node->refcnt, 1);
  return ret;
}

static void
EbiSetLeftBoundary(EbiNode node, Snapshot snapshot) {
  node->left_boundary = EbiDsaCopySnapshot(snapshot);
}

static void
EbiSetRightBoundary(EbiNode node, Snapshot snapshot) {
  node->right_boundary = EbiDsaCopySnapshot(snapshot);
}

/* DSA version of CopySnapshot in snapmgr.c */
static dsa_pointer
EbiDsaCopySnapshot(Snapshot snapshot) {
  dsa_pointer dsa_newsnap;
  Snapshot newsnap;
  Size subxipoff;
  Size size;
  TransactionId* snapxip;
  TransactionId* snapsubxip;

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

  /*
   * Calculate the offset of snapshot->xip, snapshot->subxip,
   * since those values are virtual addresses of the snapshot owner
   */
  snapxip = (TransactionId*)(snapshot + 1);
  snapsubxip = (TransactionId*)((char*)snapshot + subxipoff);

  if (snapshot->xcnt > 0) {
    newsnap->xip = (TransactionId*)(newsnap + 1);
    memcpy(newsnap->xip, snapxip, snapshot->xcnt * sizeof(TransactionId));
  } else
    newsnap->xip = NULL;

  if (snapshot->subxcnt > 0 &&
      (!snapshot->suboverflowed || snapshot->takenDuringRecovery)) {
    newsnap->subxip = (TransactionId*)((char*)newsnap + subxipoff);
    memcpy(
        newsnap->subxip, snapsubxip, snapshot->subxcnt * sizeof(TransactionId));
  } else
    newsnap->subxip = NULL;

  return dsa_newsnap;
}

static dsa_pointer
EbiSetRightBoundaryRecursive(dsa_pointer dsa_node, Snapshot snapshot) {
  EbiNode tmp;
  dsa_pointer ret;

  ret = dsa_node;
  tmp = EbiConvertToNode(ebitree_dsa_area, ret);

  while (DsaPointerIsValid(tmp->right)) {
    ret = tmp->right;
    tmp = EbiConvertToNode(ebitree_dsa_area, ret);
    EbiSetRightBoundary(tmp, snapshot);
  }

  return ret;
}

void
EbiDecreaseRefCount(dsa_pointer dsa_node) {
  EbiNode node;
  uint32 refcnt;

  node = EbiConvertToNode(ebitree_dsa_area, dsa_node);
  refcnt = EbiDecreaseRefCountInternal(node);

  if (refcnt == 0) {
    EbiMpscEnqueue(ebitree_dsa_area, EbiTreeShmem->unlink_queue, dsa_node);
  }
}

static uint32
EbiDecreaseRefCountInternal(EbiNode node) {
  uint32 ret;
  ret = pg_atomic_sub_fetch_u32(&node->refcnt, 1);
  return ret;
}

void
EbiUnlinkNodes(
    dsa_pointer dsa_ebitree,
    dsa_pointer unlink_queue,
    EbiSpscQueue delete_queue) {
  dsa_pointer dsa_tmp;
  EbiTree ebitree;

  ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

  dsa_tmp = EbiMpscDequeue(ebitree_dsa_area, unlink_queue);

  while (DsaPointerIsValid(dsa_tmp)) {
    // Logical deletion
    EbiUnlinkNode(ebitree, dsa_tmp, delete_queue);

    dsa_tmp = EbiMpscDequeue(ebitree_dsa_area, unlink_queue);
  }
}

static void
EbiUnlinkNode(
    EbiTree ebitree,
    dsa_pointer dsa_node,
    EbiSpscQueue delete_queue) {
  EbiNode node;

  node = EbiConvertToNode(ebitree_dsa_area, dsa_node);

  // Logical deletion, takes it off from the EBI-tree
  EbiUnlinkFromParent(node);

  // Prepare it for physical deletion
  EbiPushToGarbageQueue(delete_queue, node, dsa_node);

  // Compaction
  EbiCompactNode(ebitree, node->parent);
}

static void
EbiUnlinkFromParent(EbiNode node) {
  EbiNode parent, curr;
  uint64 num_versions;
  dsa_pointer proxy_target;

  parent = EbiConvertToNode(ebitree_dsa_area, node->parent);

  Assert(parent != NULL);

  if (EbiIsLeftChild(node)) {
    parent->left = InvalidDsaPointer;
  } else {
    parent->right = InvalidDsaPointer;
  }

  /* Version counter */
  curr = node;
  while (curr != NULL) {
    proxy_target = curr->proxy_target;

    num_versions = pg_atomic_read_u64(&curr->num_versions);
    num_versions = num_versions - (num_versions % NUM_VERSIONS_PER_CHUNK);
    pg_atomic_sub_fetch_u64(&EbiTreeShmem->num_versions, num_versions);

    curr = EbiConvertToNode(ebitree_dsa_area, proxy_target);
  }
}

static void
EbiPushToGarbageQueue(
    EbiSpscQueue delete_queue,
    EbiNode node,
    dsa_pointer dsa_node) {
  EbiSpscEnqueue(delete_queue, node, dsa_node);
}

static void
EbiCompactNode(EbiTree ebitree, dsa_pointer dsa_node) {
  EbiNode node, tmp;
  dsa_pointer proxy_target;
  uint32 original_height;

  proxy_target = dsa_node;
  node = EbiConvertToNode(ebitree_dsa_area, dsa_node);

  if (EbiHasParent(node) == false) {
    // When the root's child is being compacted
    EbiNode root;

    if (EbiHasLeftChild(node)) {
      EbiLinkProxy(node->left, proxy_target);
      ebitree->root = node->left;
    } else {
      EbiLinkProxy(node->right, proxy_target);
      ebitree->root = node->right;
    }
    root = EbiConvertToNode(ebitree_dsa_area, ebitree->root);
    root->parent = InvalidDsaPointer;
  } else {
    EbiNode parent;
    dsa_pointer tmp_ptr;

    parent = EbiConvertToNode(ebitree_dsa_area, node->parent);

    // Compact the one-and-only child and its parent
    if (EbiIsLeftChild(node)) {
      if (EbiHasLeftChild(node)) {
        EbiLinkProxy(node->left, proxy_target);
        parent->left = node->left;
      } else {
        EbiLinkProxy(node->right, proxy_target);
        parent->left = node->right;
      }
      tmp = EbiConvertToNode(ebitree_dsa_area, parent->left);
    } else {
      if (EbiHasLeftChild(node)) {
        EbiLinkProxy(node->left, proxy_target);
        parent->right = node->left;
      } else {
        EbiLinkProxy(node->right, proxy_target);
        parent->right = node->right;
      }
      tmp = EbiConvertToNode(ebitree_dsa_area, parent->right);
    }
    tmp->parent = node->parent;

    // Parent height propagation
    tmp_ptr = node->parent;
    while (DsaPointerIsValid(tmp_ptr)) {
      EbiNode curr, left, right;

      curr = EbiConvertToNode(ebitree_dsa_area, tmp_ptr);
      left = EbiConvertToNode(ebitree_dsa_area, curr->left);
      right = EbiConvertToNode(ebitree_dsa_area, curr->right);

      original_height = curr->height;

      curr->height = Max(left->height, right->height) + 1;

      if (curr->height == original_height) {
        break;
      }

      tmp_ptr = curr->parent;
    }
  }
}

static void
EbiLinkProxy(dsa_pointer dsa_proxy, dsa_pointer dsa_proxy_target) {
  EbiNode proxy, new_proxy_target;

  proxy = EbiConvertToNode(ebitree_dsa_area, dsa_proxy);

  if (DsaPointerIsValid(proxy->proxy_target)) {
    new_proxy_target = EbiConvertToNode(ebitree_dsa_area, dsa_proxy_target);
    new_proxy_target->proxy_target = proxy->proxy_target;
  }

  proxy->proxy_target = dsa_proxy_target;
}

void
EbiDeleteNodes(EbiSpscQueue delete_queue) {
  EbiNode front;
  dsa_pointer dsa_ptr;

  while (!EbiSpscQueueIsEmpty(delete_queue)) {
    dsa_ptr = EbiSpscQueueFrontDsaPointer(delete_queue);

    pg_memory_barrier();

    front = EbiSpscDequeue(delete_queue);

    EbiDeleteNode(front, dsa_ptr);
  }
}

void
EbiDeleteNode(EbiNode node, dsa_pointer dsa_ptr) {
  EbiNode curr;
  dsa_pointer dsa_curr, proxy_target;

  curr = node;
  dsa_curr = dsa_ptr;

  while (curr != NULL) {
    proxy_target = curr->proxy_target;

    EbiTreeRemoveSegmentFile(curr->seg_id);

    /* Leftboundary could be unset when EbiDeleteTree is called on shutdown */
    if (DsaPointerIsValid(curr->left_boundary)) {
      dsa_free(ebitree_dsa_area, curr->left_boundary);
    }

    /* Right boundary could be unset in the case of internal nodes */
    if (DsaPointerIsValid(curr->right_boundary)) {
      dsa_free(ebitree_dsa_area, curr->right_boundary);
    }

    dsa_free(ebitree_dsa_area, dsa_curr);

    curr = EbiConvertToNode(ebitree_dsa_area, proxy_target);
    dsa_curr = proxy_target;
  }
}

EbiNode
EbiSift(TransactionId vmin, TransactionId vmax) {
  uint32 my_slot;
  EbiNode ret;

  /* Must be place before entering the EBI-tree */
  my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
  pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

  pg_memory_barrier();

  ret = EbiSiftInternal(vmin, vmax);

  pg_memory_barrier();

  /* Must be place after traversing the EBI-tree */
  pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

  return ret;
}

static EbiNode
EbiSiftInternal(TransactionId vmin, TransactionId vmax) {
  EbiTree ebitree;
  EbiNode curr, left, right;
  bool left_includes, right_includes;
  bool left_exists, right_exists;

  ebitree = EbiConvertToTree(ebitree_dsa_area, EbiTreeShmem->ebitree);
  curr = EbiConvertToNode(ebitree_dsa_area, ebitree->root);

  Assert(curr != NULL);

  /* If root's left boundary doesn't set yet, return immediately */
  if (!DsaPointerIsValid(curr->left_boundary)) return NULL;

  /* The version is already dead, may be cleaned */
  if (!EbiOverlaps(curr, vmin, vmax)) return NULL;

  while ((curr != NULL) && !EbiIsLeaf(curr)) {
    left = EbiConvertToNode(ebitree_dsa_area, curr->left);
    right = EbiConvertToNode(ebitree_dsa_area, curr->right);

    left_exists = ((left != NULL) && DsaPointerIsValid(left->left_boundary));
    right_exists = ((right != NULL) && DsaPointerIsValid(right->left_boundary));

    if (!left_exists && !right_exists) {
      return NULL;
    } else if (!left_exists) {
      // Only the left is null and the version does not fit into the right
      if (!EbiOverlaps(right, vmin, vmax)) {
        return NULL;
      } else {
        curr = EbiConvertToNode(ebitree_dsa_area, curr->right);
      }
    } else if (!right_exists) {
      // Only the right is null and the version does not fit into the left
      if (!EbiOverlaps(left, vmin, vmax)) {
        return NULL;
      } else {
        curr = EbiConvertToNode(ebitree_dsa_area, curr->left);
      }
    } else {
      // Both are not null
      left_includes = EbiOverlaps(left, vmin, vmax);
      right_includes = EbiOverlaps(right, vmin, vmax);

      if (left_includes && right_includes) {
        // Overlaps both child, current interval is where it fits
        break;
      } else if (left_includes) {
        curr = EbiConvertToNode(ebitree_dsa_area, curr->left);
      } else if (right_includes) {
        curr = EbiConvertToNode(ebitree_dsa_area, curr->right);
      } else {
        return NULL;
      }
    }
  }

  return curr;
}

static bool
EbiOverlaps(EbiNode node, TransactionId vmin, TransactionId vmax) {
  Snapshot left_snap, right_snap;

  left_snap = (Snapshot)dsa_get_address(ebitree_dsa_area, node->left_boundary);
  right_snap =
      (Snapshot)dsa_get_address(ebitree_dsa_area, node->right_boundary);

  Assert(left_snap != NULL);

  if (right_snap != NULL)
    return XidInMVCCSnapshotForEBI(vmax, left_snap) &&
           !XidInMVCCSnapshotForEBI(vmin, right_snap);
  else
    return XidInMVCCSnapshotForEBI(vmax, left_snap);
}

EbiTreeVersionOffset
EbiSiftAndBind(
    TransactionId vmin,
    TransactionId vmax,
    Size tuple_size,
    const void* tuple,
    LWLock* rwlock) {
  EbiNode node;
  EbiTreeSegmentId seg_id;
  EbiTreeSegmentOffset seg_offset;
  Size aligned_tuple_size;
  bool found;
  EbiTreeVersionOffset ret;
  uint64 num_versions;

  Assert(ebitree_dsa_area != NULL);

  node = EbiSift(vmin, vmax);

  if (node == NULL) {
    /* Reclaimable */
    return EBI_TREE_INVALID_VERSION_OFFSET;
  }

  aligned_tuple_size = 1 << my_log2(tuple_size);

  /* We currently forbid tuples with sizes that are larger than the page size */
  Assert(aligned_tuple_size <= EBI_TREE_SEG_PAGESZ);

  seg_id = node->seg_id;
  do {
    seg_offset = pg_atomic_fetch_add_u32(&node->seg_offset, aligned_tuple_size);

    /* Checking if the tuple could be written within a single page */
    found = seg_offset / EBI_TREE_SEG_PAGESZ ==
            (seg_offset + aligned_tuple_size - 1) / EBI_TREE_SEG_PAGESZ;
  } while (!found);

  // Write version to segment
  EbiTreeAppendVersion(seg_id, seg_offset, tuple_size, tuple, rwlock);

  num_versions = pg_atomic_add_fetch_u64(&node->num_versions, 1);

  // Update global counter if necessary
  if (num_versions % NUM_VERSIONS_PER_CHUNK == 0) {
    pg_atomic_fetch_add_u64(
        &EbiTreeShmem->num_versions, NUM_VERSIONS_PER_CHUNK);
  }

  ret = EBI_TREE_SEG_TO_VERSION_OFFSET(seg_id, seg_offset);

  return ret;
}

int
EbiLookupVersion(
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

bool
EbiSegIsAlive(dsa_pointer dsa_ebitree, EbiTreeSegmentId seg_id) {
  uint32 my_slot;
  bool ret;

  /* Must be place before entering the EBI-tree */
  my_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
  pg_atomic_fetch_add_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

  pg_memory_barrier();

  ret = EbiSegIsAliveInternal(dsa_ebitree, seg_id);

  pg_memory_barrier();

  /* Must be place after traversing the EBI-tree */
  pg_atomic_fetch_sub_u64(&EbiTreeShmem->gc_queue_refcnt[my_slot], 1);

  return ret;
}

static bool
EbiSegIsAliveInternal(dsa_pointer dsa_ebitree, EbiTreeSegmentId seg_id) {
  EbiTree ebitree;
  dsa_pointer dsa_curr;
  EbiNode curr;

  ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);

  dsa_curr = ebitree->root;
  curr = EbiConvertToNode(ebitree_dsa_area, dsa_curr);

  while (!EbiIsLeaf(curr)) {
    if (curr->seg_id == seg_id) {
      return true;
    }

    if (seg_id < curr->seg_id) {
      dsa_curr = curr->left;
    } else {
      dsa_curr = curr->right;
    }

    /* It has been concurrently removed by the EBI-tree process */
    if (!DsaPointerIsValid(dsa_curr)) {
      return false;
    }

    curr = EbiConvertToNode(ebitree_dsa_area, dsa_curr);
  }

  Assert(EbiIsLeaf(curr));

  while (DsaPointerIsValid(dsa_curr)) {
    curr = EbiConvertToNode(ebitree_dsa_area, dsa_curr);

    if (curr->seg_id == seg_id) {
      return true;
    }

    dsa_curr = curr->proxy_target;
  }

  return false;
}

EbiTree
EbiConvertToTree(dsa_area* area, dsa_pointer ptr) {
  return (EbiTree)dsa_get_address(area, ptr);
}

EbiNode
EbiConvertToNode(dsa_area* area, dsa_pointer ptr) {
  return (EbiNode)dsa_get_address(area, ptr);
}

/**
 * Utility Functions
 */

static bool
EbiHasParent(EbiNode node) {
  return DsaPointerIsValid(node->parent);
}

static bool
EbiIsLeftChild(EbiNode node) {
  EbiNode parent = EbiConvertToNode(ebitree_dsa_area, node->parent);
  return node == EbiConvertToNode(ebitree_dsa_area, parent->left);
}

static bool
EbiHasLeftChild(EbiNode node) {
  return DsaPointerIsValid(node->left);
}

static bool
EbiIsLeaf(EbiNode node) {
  return node->height == 0;
}

static dsa_pointer
EbiSibling(EbiNode node) {
  if (EbiHasParent(node)) {
    EbiNode parent = EbiConvertToNode(ebitree_dsa_area, node->parent);
    if (EbiIsLeftChild(node)) {
      return parent->right;
    } else {
      return parent->left;
    }
  } else {
    return InvalidDsaPointer;
  }
}

void
EbiPrintTree(dsa_pointer dsa_ebitree) {
  EbiTree ebitree;
  EbiNode root;

  ebitree = EbiConvertToTree(ebitree_dsa_area, dsa_ebitree);
  root = EbiConvertToNode(ebitree_dsa_area, ebitree->root);

  ereport(LOG, (errmsg("Print Tree (%d)", root->height)));
  EbiPrintTreeRecursive(root);
}

void
EbiPrintTreeRecursive(EbiNode node) {
  if (node == NULL) {
    return;
  }
  EbiPrintTreeRecursive(EbiConvertToNode(ebitree_dsa_area, node->left));
  ereport(
      LOG,
      (errmsg(
          "[HYU] seg_id: %d, offset: %d",
          node->seg_id,
          pg_atomic_read_u32(&node->seg_offset))));
  EbiPrintTreeRecursive(EbiConvertToNode(ebitree_dsa_area, node->right));
}

#endif
