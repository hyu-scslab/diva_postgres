/*-------------------------------------------------------------------------
 *
 * pleaf_mgr.h
 * 		Provisional Index Version Manager	
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pleaf_mgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLEAF_MGR_H
#define PLEAF_MGR_H

#define PLeafGetCurrentTime(timespec) \
	clock_gettime(CLOCK_MONOTONIC, timespec)

#define PLeafGetTimeElapsedInSeconds(now, base) \
	(now.tv_sec - base.tv_sec)

extern void PLeafManagerMain(void) pg_attribute_noreturn();

#endif /* PLEAF_MGR_H */
