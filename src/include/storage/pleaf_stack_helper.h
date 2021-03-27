/*-------------------------------------------------------------------------
 *
 * pleaf_stack_helper.h
 * 		Concurrent stack with Elimination array
 * 		Implementation of Elimination array
 *
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/pleaf_stack_helper.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PLEAF_STACK_HELPER_H
#define PLEAF_STACK_HELPER_H


#define N_ELIMINATION_ARRAY (8)
#define ELIM_ARRAY_DUR (double)(0.000001) // micro-sec

#define EXCHANGER_INIT (0)
#define EXCHANGER_FAIL ((uint64_t)(-1))

#define GetExchangerStatus(v) \
	((uint32_t)((v & ~HELPER_VALUE_MASK) >> 32))

#define HELPER_VALUE_MASK (0x00000000FFFFFFFFULL)

#define GetExchangerValue(v) \
	(v & HELPER_VALUE_MASK)

#define SetExchangerNewValue(v, s) \
	((((uint64_t)s) << 32) | v)

#define EL_EMPTY 0
#define EL_WAITING 1
#define EL_BUSY 2

typedef struct 
{
	uint64_t arr[N_ELIMINATION_ARRAY];
} EliminationArrayData;

typedef EliminationArrayData* EliminationArray;

extern uint64_t ElimArrayVisit(EliminationArray elim_array, 
												uint64_t value);

#endif /* PLEAF_STACK_HELPER_H */
