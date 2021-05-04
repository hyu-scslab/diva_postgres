/*-------------------------------------------------------------------------
 *
 * ebi_tree_process.c
 *
 * EBI Tree Process Implementation
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/postmaster/ebi_tree_process.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM
#include "postgres.h"

#include <signal.h>
#include <time.h>
#include <unistd.h>

#include "access/xlog.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/ebi_tree_process.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "postmaster/walwriter.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/ebi_tree.h"
#include "storage/ebi_tree_buf.h"
#include "storage/ebi_tree_utils.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timeout.h"

int EbiGenerationMultiplier = 3; /* constant */
int EbiGenerationDelay = 2;     /* seconds */

/*
 * GUC parameters
 */
int EbiTreeDelay = 100; /* milli-seconds */

EbiTreeShmemStruct *EbiTreeShmem = NULL;

/* Delete queue for phsyical GC. Only accessed by the EBI-tree process alone. */
EbiSpscQueue delete_queue[EBI_NUM_GC_QUEUE];

/* Prototypes for private functions */
static void HandleEbiTreeProcessInterrupts(void);

static void EbiTreeProcessInit(void);

static void EbiTreeDsaAttach(void);
static void EbiTreeDsaDetach(void);

static bool EbiNeedsNewNode(void);

/*
 * Main entry point for EBI tree process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
EbiTreeProcessMain(void)
{
	sigjmp_buf local_sigjmp_buf;
	MemoryContext ebitree_context;

	EbiTreeShmem->ebitree_pid = MyProcPid;

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
	ebitree_context = AllocSetContextCreate(
		TopMemoryContext, "EBI Tree", ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(ebitree_context);

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
		MemoryContextSwitchTo(ebitree_context);
		FlushErrorState();

		/* Flush any leaked data in the top-level context */
		MemoryContextResetAndDeleteChildren(ebitree_context);

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
	ProcGlobal->ebitreeLatch = &MyProc->procLatch;

	/* Initialize dsa area */
	EbiTreeDsaInit();

	/* Initialize EBI-tree process's local variables */
	EbiTreeProcessInit();

	/*
	 * Loop forever
	 */
	for (;;)
	{
		long cur_timeout;
		uint32 curr_slot;
		uint32 prev_slot;
		uint64 refcnt;

		/* Clear any already-pending wakeups */
		ResetLatch(MyLatch);

		HandleEbiTreeProcessInterrupts();

		cur_timeout = EbiTreeDelay;
		curr_slot = pg_atomic_read_u32(&EbiTreeShmem->curr_slot);
		prev_slot = (curr_slot + 1) % EBI_NUM_GC_QUEUE;

		/* EBI tree operations */
		if (!EbiMpscQueueIsEmpty(ebitree_dsa_area, EbiTreeShmem->unlink_queue))
		{
			EbiUnlinkNodes(EbiTreeShmem->ebitree,
						   EbiTreeShmem->unlink_queue,
						   delete_queue[curr_slot]);
		}

		pg_memory_barrier();

		refcnt = pg_atomic_read_u64(&EbiTreeShmem->gc_queue_refcnt[prev_slot]);
		if (refcnt == 0)
		{
			EbiDeleteNodes(delete_queue[prev_slot]);

			/* Switch slot */
			pg_atomic_write_u32(&EbiTreeShmem->curr_slot,
								(curr_slot + 1) % EBI_NUM_GC_QUEUE);

			pg_memory_barrier();
		}

		if (EbiNeedsNewNode())
		{
			EbiInsertNode(EbiTreeShmem->ebitree);
		}

		(void)WaitLatch(MyLatch,
						WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						cur_timeout,
						WAIT_EVENT_EBI_TREE_MAIN);
	}
}

static bool
EbiNeedsNewNode(void)
{
	TransactionId curr_max_xid;
	TransactionId duration;
	struct timespec now;
	int elapsed;

	/* Current recent node is not opened by the first visiting transaction */
	if (!EbiRecentNodeIsAlive(EbiTreeShmem->ebitree))
	{
		return false;
	}

	/* Check average version lifetime */
	curr_max_xid = EbiGetMaxTransactionId();
	duration = curr_max_xid - EbiTreeShmem->max_xid;

	Assert(duration > 0);

	if (duration >= EbiTreeShmem->average_ver_len * EbiGenerationMultiplier)
	{
		return true;
	}

	/* Check silent period */
	EbiGetCurrentTime(&now);
	elapsed = EbiGetTimeElapsedInSeconds(now, EbiTreeShmem->last_timespec);

	if (elapsed >= EbiGenerationDelay)
	{
		return true;
	}

	return false;
}

/*
 * Process any new interrupts.
 */
static void
HandleEbiTreeProcessInterrupts(void)
{
	if (ProcSignalBarrierPending) ProcessProcSignalBarrier();

	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	if (ShutdownRequestPending)
	{
		EbiDeleteTree(EbiTreeShmem->ebitree);
		EbiDeleteMpscQueue(ebitree_dsa_area, EbiTreeShmem->unlink_queue);
		for (int i = 0; i < EBI_NUM_GC_QUEUE; i++)
		{
			EbiDeleteSpscQueue(delete_queue[i]);
		}

		EbiTreeDsaDetach();
		/* Normal exit from the EBI tree process is here */
		proc_exit(0); /* done */
	}
}

/* --------------------------------
 *		communication with backends
 * --------------------------------
 */

/*
 * EbiTreeShmemSize
 *		Compute space needed for EBI tree related shared memory
 */
Size
EbiTreeShmemSize(void)
{
	Size size = 0;

	size = add_size(size, sizeof(EbiTreeShmemStruct));

	size = add_size(size, EbiTreeBufShmemSize());

	return size;
}

/*
 * EbiTreeShmemInit
 *		Allocate and initialize EBI tree related shared memory
 */
void
EbiTreeShmemInit(void)
{
	Size size = EbiTreeShmemSize();
	bool found;

	/*
	 * Create or attach to the shared memory state, including hash table
	 */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	EbiTreeShmem =
		(EbiTreeShmemStruct *)ShmemInitStruct(
				"EBI Tree Data", sizeof(EbiTreeShmemStruct), &found);

	if (!found)
	{
		/*
		 * First time through, so initialize.
		 */
		MemSet(EbiTreeShmem, 0, size);
		Assert(EbiTreeShmem != NULL);

		/* Init pg_atomic variables */
		pg_atomic_init_u64(&EbiTreeShmem->num_versions, 0);
		pg_atomic_init_u32(&EbiTreeShmem->curr_slot, 0);
		for (int i = 0; i < EBI_NUM_GC_QUEUE; i++)
		{
			pg_atomic_init_u64(&EbiTreeShmem->gc_queue_refcnt[i], 0);
		}
		pg_atomic_init_flag(&EbiTreeShmem->is_updating_stat);
	}

	EbiTreeBufInit();

	LWLockRelease(AddinShmemInitLock);
}

void
EbiTreeDsaInit(void)
{
	/* This process is selected to create dsa_area itself */

	/*
	 * The first backend process creates the dsa area for EBI tree,
	 * and another backend processes waits the creation and then attach to it.
	 */
	if (EbiTreeShmem->handle == 0)
	{
		uint32 expected = 0;
		if (pg_atomic_compare_exchange_u32(
				(pg_atomic_uint32 *)(&EbiTreeShmem->handle),
				&expected,
				UINT32_MAX))
		{
			dsa_area *area;
			dsa_handle handle;

			/* Initialize dsa area for vcluster */
			area = dsa_create(LWTRANCHE_EBI_TREE);
			handle = dsa_get_handle(area);

			dsa_pin(area);

			/* Allocate a new EBI tree in dsa */
			EbiTreeShmem->ebitree = EbiInitTree(area);

			/* Allocate queues in dsa */
			EbiTreeShmem->unlink_queue = EbiInitMpscQueue(area);

			dsa_detach(area);

			pg_memory_barrier();

			EbiTreeShmem->handle = handle;
		}
	}

	while (EbiTreeShmem->handle == UINT32_MAX)
	{
		/*
		 * Another process is creating an initial dsa area for EBI tree,
		 * so just wait it to finish and then attach to it.
		 */
		usleep(1);
	}
	EbiTreeDsaAttach();
}

static void
EbiTreeDsaAttach(void)
{
	if (ebitree_dsa_area != NULL) return;

	ebitree_dsa_area = dsa_attach(EbiTreeShmem->handle);
	dsa_pin_mapping(ebitree_dsa_area);
}

static void
EbiTreeDsaDetach(void)
{
	if (ebitree_dsa_area == NULL) return;

	dsa_detach(ebitree_dsa_area);
	ebitree_dsa_area = NULL;
}

static void
EbiTreeProcessInit(void)
{
	/* Allocate GC queue */
	for (int i = 0; i < EBI_NUM_GC_QUEUE; i++)
	{
		delete_queue[i] = EbiInitSpscQueue();
	}
}

#endif
