/*-------------------------------------------------------------------------
 *
 * pleaf.c
 * 		PLeaf API implementation. 
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
 *    src/backend/storage/pleaf/pleaf.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef DIVA
#include "postgres.h"

#include "utils/snapmgr.h"
#include "storage/lwlock.h"

#include "storage/ebi_tree.h"
#include "storage/ebi_tree_buf.h"
#include "storage/pleaf_stack_helper.h"
#include "storage/pleaf_bufpage.h"
#include "storage/pleaf_buf.h"
#include "storage/pleaf_internals.h"
#include "storage/pleaf.h"

#include <assert.h>

#ifdef DIVA_PRINT
#include "optimizer/cost.h"
#endif
/*
 * PLeafLookupTuple
 *
 * Lookup a visible version locator in p-leaf, 
 * and then get version data from EBI-tree.
 */
int
PLeafLookupTuple(
		PLeafOffset offset,
		Snapshot snapshot,
		Size tuple_size,
		void** ret_value) {

	PLeafPageId page_id;
	PLeafGenNumber gen_no;
	PLeafPage page;
	int frame_id;
	PLeafOffset	internal_offset;
	PLeafVersionOffset version_offset;
	bool version_found;
	int ebi_page_frame_id;
#ifdef DIVA_PRINT
	struct timespec starttime, endtime;
	if (j3vm_print)
		clock_gettime(CLOCK_MONOTONIC, &starttime);
#endif

	/*
	 * Offset value in record
	 * ---- 2 byte -------------- 6 byte -----------
	 * | Generation Number | Page Id + Page Offset |
	 * ---------------------------------------------
	 *  The only one including generation number is the offset in the record
	 *  (not in version locator or internal p-leaf offset).
	 */
	internal_offset = offset;

	gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(internal_offset);
	internal_offset = PLEAF_OFFSET_TO_INTERNAL_OFFSET(offset);

	/* Assertion */
	PLeafIsGenerationNumberValidInLookup(gen_no);
	
	/*
	 * Search a version locator(EBI-tree locator) in p-leaf version array.
	 * The version locator should be in the direct version array.
	 * If the current version array is indirect, keep going to find direct one.
	 */
	while (true) {
		page_id = PLEAF_OFFSET_TO_PAGE_ID(internal_offset);
		page = PLeafGetPage(page_id, gen_no, false, &frame_id);

		/* 
		 * If the value of version_found is true,
		 * it means we find the version locator successfully or fail to find 
		 * the version locator(or offset) with transaction's snapshot.
		 *
		 * Else, it means that we find the version offset in the indirect array.
		 */
		version_found = 
			PLeafLookupVersion(page, &internal_offset, snapshot);

		PLeafReleasePage(frame_id);

		if (version_found) {
			version_offset = internal_offset;
			break;
		}
	}
#ifdef DIVA_PRINT
	if (j3vm_print)
	{
		clock_gettime(CLOCK_MONOTONIC, &endtime);
		pleaf_time += (endtime.tv_sec - starttime.tv_sec) * 1000 +
							(double)(endtime.tv_nsec - starttime.tv_nsec) / 1000000;
	}
#endif

	/* Fail to find the visible version locator */
	if (version_offset == PLEAF_INVALID_VERSION_OFFSET) {
#ifdef DIVA_PRINT
		Assert(false);
#endif
		return -1;
	}
	
	/* Read value from EBI-Tree */
	// API in EBI-Tree
	ebi_page_frame_id = EbiLookupVersion(version_offset, tuple_size, ret_value);
	// Return ebi-page-frame-id
	return ebi_page_frame_id;
}

/*
 * PLeafAppendTuple
 *
 * Append new version to p-leaf version array and new version data to EBI-tree.
 * If the version is invisible, return immediately
 */
void
PLeafAppendTuple(  
		PLeafOffset offset,
		PLeafOffset* ret_offset,
		TransactionId xmin,
		TransactionId xmax,
		Size tuple_size,
		const void* tuple,
		LWLock* rwlock) {

	PLeafVersionOffset version_offset;

	/*
	 * Get the version offset from EBI-tree.
	 * It can be already obsolete version
	 * version_offset = EBI-APPEND-VERSION
	 */
	version_offset = EbiSiftAndBind(xmin, xmax, tuple_size, tuple, 
			rwlock);

	if (version_offset == PLEAF_INVALID_VERSION_OFFSET) {
		return;
	}

	PLeafAppendVersion(offset, ret_offset, xmin, xmax, version_offset, rwlock);
}

/*
 * PLeafIsLeftLookup
 * called by read transactions only 
 */
bool
PLeafIsLeftLookup(uint64 left_offset,
						uint64 right_offset,
						TransactionId xid_bound,
						Snapshot snapshot)
{
	
	PLeafGenNumber left_gen_no, right_gen_no;
	PLeafGenNumber old_gen_no;
	left_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(left_offset);
	right_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(right_offset);

	assert(!(left_gen_no == 0 && right_gen_no == 0));
	assert(left_gen_no != right_gen_no);

	/*
	 * Update : xid-bound -> offset value
	 */
	if (xid_bound == 0)
		return true;

	/*
	 * If either left's or right's generation number is 0,
	 * read operation should be done in non-zero generation number
	 */
	if (right_gen_no == 0)
		return true;
	else if (left_gen_no == 0)
		return false;
	else
	{
		/* Get old generation number */
		old_gen_no = PLeafGetOldGenerationNumber();
		
		if (XidInMVCCSnapshot(xid_bound, snapshot))
			/*
			 * If xid bound is visible to transaction's snapshot, it means that the
			 * visible version is located in old generation.
			 */
			return left_gen_no == old_gen_no; 
		else
			return left_gen_no > right_gen_no;
	}	
}

/*
 * PLeafIsLeftUpdate
 * called by update transaction only
 */
bool
PLeafIsLeftUpdate(uint64 left_offset,
						uint64 right_offset,
						int* ret_status)
{
	PLeafGenNumber left_gen_no, right_gen_no, global_gen_no;

	/*
	 * Get the latest generation number without PLeafBufferIOLWLockArray.
	 * In here, we can get the latest generation number created at the moment,
	 * or the old generation number. However, generation number in record's offset
	 * cannot same be as the latest generation number mentioned above.
	 */
	global_gen_no = PLeafGetLatestGenerationNumber();
	assert(global_gen_no != 0);

	/* Get both generation numbers */
	left_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(left_offset);
	right_gen_no = PLEAF_OFFSET_TO_GEN_NUMBER(right_offset);

	/* Set return status PLEAF_NORMAL 
	 * PLEAF_NORMAL means it doesn't change xid_bound value in meta-tuple.
	 */
	*ret_status = PLEAF_NORMAL;

	/*
	 * If the latest generation number is same as either left or right
	 * generation number, return its direction(i.e. left or right)
	 */
	if (global_gen_no == left_gen_no)
		/* Left side */
		return true;
	else if (global_gen_no == right_gen_no)
		/* Right side */
		return false;
	else if (left_gen_no == 0 && right_gen_no == 0)
		/* Also, it doesn't change xid_bound value in the first update */
		return true;

	/*
	 * PLEAF_SWITCH means it should change xid_bound value in meta-tuple.
	 */
	*ret_status = PLEAF_SWITCH;

	if (PLeafIsGenerationNumberValidInUpdate(left_gen_no, right_gen_no))
	{
		/*
		 * Smaller generation number will be target in this case.
		 * We should guarantee that old one is already cleaned.
		 * See PLeafIsGenerationNumberValidInUpdate()
		 */
		return left_gen_no < right_gen_no;
	}
	else
	{
		/*
		 * If both generation number in the meta-tuple is not same as the 
		 * generation number in PLeafMetadata, it means both of them were cleaned.
		 * Therefore, reset xid_bound value.
		 */
		*ret_status = PLEAF_RESET;
		return true;
	}
}

#endif
