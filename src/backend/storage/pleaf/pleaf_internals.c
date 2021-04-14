/*-------------------------------------------------------------------------
 *
 * pleaf_internals.c
 * 		Implement helper functions used in pleaf_reader / pleaf_writer 
 *
 * 
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/pleaf/pleaf_internals.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM
#include "postgres.h"

#include "storage/lwlock.h"
#include "utils/snapmgr.h"

#include "storage/pleaf_bufpage.h"
#include "storage/pleaf_internals.h"

#include "storage/ebi_tree.h"

#include <assert.h>

/*
 * PLeafIsVisible
 */
int
PLeafIsVisible(Snapshot snapshot, uint32_t xmin, uint32_t xmax) 
{
	bool xmin_visible;
	bool xmax_visible;

	xmin_visible = XidInMVCCSnapshot(xmin, snapshot);
	xmax_visible = XidInMVCCSnapshot(xmax, snapshot);

	if (xmax_visible)
	{
		if (xmin_visible)
			return PLEAF_LOOKUP_BACKWARD;
		else
			return PLEAF_LOOKUP_FOUND;
	}
	else
	{
		if (xmin_visible)
			return PLEAF_LOOKUP_BACKWARD;
		else
			return PLEAF_LOOKUP_FORWARD;
	}
}

/*
 * PLeafCheckVisibility
 *
 * Visibility check using EBI-tree for now
 */
bool
PLeafCheckVisibility(TransactionId xmin, TransactionId xmax) 
{
	assert(xmin <= xmax);
	return (Sift(xmin, xmax) != NULL);
}

/*
 * PLeafGetVersionInfo
 *
 * Get xmin and xmax value from version id
 */
void
PLeafGetVersionInfo(PLeafVersionId version_id,
						TransactionId* xmin, 
						TransactionId* xmax)
{
	*xmin = PLeafGetXmin(version_id);
	*xmax = PLeafGetXmax(version_id);
	assert(*xmin <= *xmax);
}

/*
 * PLeafCheckAppendness
 *
 * Appendness check in circular array
 */
bool
PLeafCheckAppendness(int cap, uint16_t head, uint16_t tail) 
{
	return ((head == tail) || (head % cap) != (tail % cap));
}

/*
 * PLeafCheckEmptiness
 *
 * Emptiness check in circular array
 */
bool
PLeafCheckEmptiness(uint16_t head, uint16_t tail)
{
	return (head == tail);
}

#endif
