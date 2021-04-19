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
#include "storage/ebi_tree_buf.h"
#include "storage/ebi_tree_utils.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/procsignal.h"
#include "storage/smgr.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/resowner.h"
#include "utils/timeout.h"

/*
 * GUC parameters
 */
int EbiTreeDelay = 200 * 10;

EbiTreeShmemStruct *EbiTreeShmem = NULL;

EbiList delete_list;

/* Prototypes for private functions */
static void HandleEbiTreeProcessInterrupts(void);

static void EbiTreeProcessInit(void);

static void EbiTreeDsaAttach(void);
static void EbiTreeDsaDetach(void);

/*
 * Main entry point for EBI tree process
 *
 * This is invoked from AuxiliaryProcessMain, which has already created the
 * basic execution environment, but not enabled signals yet.
 */
void
EbiTreeProcessMain(void) {
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
  if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
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
  for (;;) {
    long cur_timeout;

    /* Clear any already-pending wakeups */
    ResetLatch(MyLatch);

    HandleEbiTreeProcessInterrupts();

    cur_timeout = EbiTreeDelay;

    // PrintEbiTree(EbiTreeShmem->ebitree);

    /* EBI tree operations */
    if (!QueueIsEmpty(ebitree_dsa_area, EbiTreeShmem->unlink_queue)) {
      UnlinkNodes(
          EbiTreeShmem->ebitree, EbiTreeShmem->unlink_queue, delete_list);
    }

    if (!EbiListIsEmpty(delete_list)) {
      DeleteNodes(delete_list);
    }

    if (NeedsNewNode(EbiTreeShmem->ebitree)) {
      InsertNode(EbiTreeShmem->ebitree);
    }

    (void)WaitLatch(
        MyLatch,
        WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
        cur_timeout,
        WAIT_EVENT_EBI_TREE_MAIN);
  }
}

/*
 * Process any new interrupts.
 */
static void
HandleEbiTreeProcessInterrupts(void) {
  if (ProcSignalBarrierPending) ProcessProcSignalBarrier();

  if (ConfigReloadPending) {
    ConfigReloadPending = false;
    ProcessConfigFile(PGC_SIGHUP);
  }

  if (ShutdownRequestPending) {
    DeleteEbiTree(EbiTreeShmem->ebitree);
    DeleteQueue(ebitree_dsa_area, EbiTreeShmem->unlink_queue);
    EbiListDestroy(ebitree_dsa_area, delete_list);

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
EbiTreeShmemSize(void) {
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
EbiTreeShmemInit(void) {
  Size size = EbiTreeShmemSize();
  bool found;

  /*
   * Create or attach to the shared memory state, including hash table
   */
  LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

  EbiTreeShmem =
      (EbiTreeShmemStruct *)ShmemInitStruct("EBI Tree Data", size, &found);
  EbiTreeShmem->seg_id = 1;
  pg_atomic_init_u64(&EbiTreeShmem->num_versions, 0);

  if (!found) {
    /*
     * First time through, so initialize.
     */
    MemSet(EbiTreeShmem, 0, size);
    Assert(EbiTreeShmem != NULL);
  }

  EbiTreeBufInit();

  LWLockRelease(AddinShmemInitLock);
}

void
EbiTreeDsaInit(void) {
  /* This process is selected to create dsa_area itself */

  /*
   * The first backend process creates the dsa area for EBI tree,
   * and another backend processes waits the creation and then attach to it.
   */
  if (EbiTreeShmem->handle == 0) {
    uint32 expected = 0;
    if (pg_atomic_compare_exchange_u32(
            (pg_atomic_uint32 *)(&EbiTreeShmem->handle),
            &expected,
            UINT32_MAX)) {
      dsa_area *area;
      dsa_handle handle;

      /* Initialize dsa area for vcluster */
      area = dsa_create(LWTRANCHE_EBI_TREE);
      handle = dsa_get_handle(area);

      dsa_pin(area);

      /* Allocate a new EBI tree in dsa */
      EbiTreeShmem->ebitree = InitEbiTree(area);

      /* Allocate queues in dsa */
      EbiTreeShmem->unlink_queue = InitQueue(area);

      dsa_detach(area);

      pg_memory_barrier();

      EbiTreeShmem->handle = handle;
    }
  }
  while (EbiTreeShmem->handle == UINT32_MAX) {
    /*
     * Another process is creating an initial dsa area for EBI tree,
     * so just wait it to finish and then attach to it.
     */
    usleep(1);
  }
  EbiTreeDsaAttach();
}

static void
EbiTreeDsaAttach(void) {
  if (ebitree_dsa_area != NULL) return;

  ebitree_dsa_area = dsa_attach(EbiTreeShmem->handle);
  dsa_pin_mapping(ebitree_dsa_area);
}

static void
EbiTreeDsaDetach(void) {
  if (ebitree_dsa_area == NULL) return;

  dsa_detach(ebitree_dsa_area);
  ebitree_dsa_area = NULL;
}

static void
EbiTreeProcessInit(void) {
  delete_list = (EbiList)palloc(sizeof(struct EbiListData));
}

#endif
