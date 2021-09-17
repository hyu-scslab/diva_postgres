/*-------------------------------------------------------------------------
 *
 * pleaf_internals.c
 * 		Implement helper functions used in pleaf_reader / pleaf_writer 
 *
 * 
 * Copyright (C) 2021 Scalable Computing Systems Lab.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
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
	return (EbiSift(xmin, xmax) != NULL);
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
