/*-------------------------------------------------------------------------
 *
 * pleaf_mgr.c
 * 		Provisional Index Manager Implementation 
 *
 * 
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/pleaf/pleaf_mgr.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM
#include "postgres.h"

#include <signal.h>
#include <unistd.h>
#include <time.h>

#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/interrupt.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "storage/lwlock.h"
#include "utils/snapmgr.h"

#include "storage/pleaf_stack_helper.h"
#include "storage/pleaf_bufpage.h"
#include "storage/pleaf_buf.h"

#include "storage/pleaf_mgr.h"

int PLeafManagerDelay = 1000; /* milli-seconds */
int PLeafGenerationDelay = 10; /* seconds */

void
PLeafManagerMain(void)
{
	sigjmp_buf	local_sigjmp_buf;
	MemoryContext pleafmanager_context;
	struct timespec base, now;

	/*
	 * Properly accept or ignore signals the postmaster might send us
	 *
	 * We have no particular use for SIGINT at the moment, but seems
	 * reasonable to treat like SIGTERM.
	 */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGQUIT, SignalHandlerForCrashExit);
	pqsignal(SIGALRM, SIG_IGN);
	pqsignal(SIGPIPE, SIG_IGN);
	pqsignal(SIGUSR1, procsignal_sigusr1_handler);
	pqsignal(SIGUSR2, SIG_IGN); /* not used */

	/*
	 * Reset some signals that are accepted by postmaster but not here
	 */
	pqsignal(SIGCHLD, SIG_DFL);

	/* We allow SIGQUIT (quickdie) at all times */
	sigdelset(&BlockSig, SIGQUIT);

	/*
	 * Create a memory context that we will do all our work in.  We do this so
	 * that we can reset the context during error recovery and thereby avoid
	 * possible memory leaks.  Formerly this code just ran in
	 * TopMemoryContext, but resetting that would be a really bad idea.
	 */
	pleafmanager_context = AllocSetContextCreate(TopMemoryContext,
											  "PLeaf Manager",
											  ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(pleafmanager_context);

	/*
	 * If an exception is encountered, processing resumes here.
	 *
	 * This code is heavily based on bgwriter.c, q.v.
	 */
	if (sigsetjmp(local_sigjmp_buf, 1) != 0)
	{
		/* Since not using PG_TRY, must reset error stack by hand */
		error_context_stack = NULL;

		/* Prevent interrupts while cleaning up */
		HOLD_INTERRUPTS();

		/* Report the error to the server log */
		EmitErrorReport();

		/*
		 * These operations are really just a minimal subset of
		 * AbortTransaction().  We don't have very many resources to worry
		 * about in walwriter, but we do have LWLocks, and perhaps buffers?
		 */
		LWLockReleaseAll();
		ConditionVariableCancelSleep();
		pgstat_report_wait_end();
		AbortBufferIO();
		UnlockBuffers();
		ReleaseAuxProcessResources(false);
		AtEOXact_Buffers(false);
		AtEOXact_SMgr();
		AtEOXact_Files(false);
		AtEOXact_HashTables(false);

		/*
		 * Now return to normal top-level context and clear ErrorContext for
		 * next time.
		 */
		MemoryContextSwitchTo(pleafmanager_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(pleafmanager_context);

		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();

		/*
		 * Sleep at least 1 second after any error.  A write error is likely
		 * to be repeated, and we don't want to be filling the error logs as
		 * fast as we can.
		 */
		pg_usleep(1000000L);

		/*
		 * Close all open files after any error.  This is helpful on Windows,
		 * where holding deleted files open causes various strange errors.
		 * It's not clear we need it elsewhere, but shouldn't hurt.
		 */
		smgrcloseall();
	}

	/* We can now handle ereport(ERROR) */
	PG_exception_stack = &local_sigjmp_buf;

	/*
	 * Unblock signals (they were blocked when the postmaster forked us)
	 */
	PG_SETMASK(&UnBlockSig);

	/*
	 * Advertise our latch that backends can use to wake us up while we're
	 * sleeping.
	 */
	ProcGlobal->pleafmanagerLatch = &MyProc->procLatch;

	PLeafGetCurrentTime(&base);
	/*
	 * Loop forever
	 */
	for (;;)
	{
		long		cur_timeout;

		cur_timeout = PLeafManagerDelay;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		HandleMainLoopInterrupts();

		/* Do work... */
		PLeafCleanOldGeneration();

		PLeafGetCurrentTime(&now);
		if (PLeafNeedsNewGeneration())
		{
      // TODO: check point for generation change
      /* Create new generation */
			PLeafMakeNewGeneration();
			base = now;
		}

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 cur_timeout,
						 WAIT_EVENT_PLEAF_MANAGER_MAIN);
	}
}



#endif
