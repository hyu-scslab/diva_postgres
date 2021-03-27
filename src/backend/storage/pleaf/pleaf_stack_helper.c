/*-------------------------------------------------------------------------
 *
 * pleaf_stack_helper.c
 * 		Elimination array implementation 
 *
 * 
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/backend/storage/pleaf/pleaf_stack_helper.c
 *
 *-------------------------------------------------------------------------
 */
#ifdef J3VM
#include "postgres.h"

#include <time.h>
#include <stdlib.h>
#include <stdbool.h>
#include <unistd.h>
#include <assert.h>

#include "storage/pleaf_stack_helper.h"

/*
 * ElimArrayExchange
 *
 * Conventional exchange function in elimination array
 */
static uint64_t
ElimArrayExchange(uint64_t* exchanger, uint64_t value) 
{
	uint64_t exchanger_value;
	uint64_t new_value;
	int retry;

	retry = 0;

	do {
		/* Read exchanger's value */
		exchanger_value = *exchanger;

		switch (GetExchangerStatus(exchanger_value)) {
			case EL_EMPTY:
				new_value = SetExchangerNewValue(value, EL_WAITING);
				/* Try to set my value */
				if (__sync_bool_compare_and_swap(
							exchanger, exchanger_value, new_value)) {
					
					/* If success, wait for other exchangers */
					do {
						retry++;
						sleep(ELIM_ARRAY_DUR);
						exchanger_value = *exchanger;

						/* If someone exchanges value, set it to null and return */
						if (GetExchangerStatus(exchanger_value) == EL_BUSY) {
							*exchanger = EXCHANGER_INIT;
							return GetExchangerValue(exchanger_value);
						}
					} while (retry <= 2);

					/* After timeout, one more chance to exchange values */
					if (__sync_bool_compare_and_swap(
								exchanger, new_value, EXCHANGER_INIT)) {
						return EXCHANGER_FAIL;
					}

					exchanger_value = *exchanger;
					*exchanger = EXCHANGER_INIT;

					return GetExchangerValue(exchanger_value);
				}
				break;

			case EL_WAITING:
				new_value = SetExchangerNewValue(value, EL_BUSY);
				/* Try to set my value */
				if (__sync_bool_compare_and_swap(
							exchanger, exchanger_value, new_value)) {

					/* If success, return its old value */
					return GetExchangerValue(exchanger_value); 
				}

				break;
			case EL_BUSY:
				break;
			default:
				assert(false);
		}
	} while(0);

	return EXCHANGER_FAIL;
	assert(false);
}
/*
 * ElimArrayVisit
 *
 * Conventional visit function in elimination array
 *
 * Called only in stack push and pop
 */
uint64_t
ElimArrayVisit(EliminationArray elimination_array, uint64_t value) 
{
	int pos;
	/* static duration */
	srand((unsigned int)time(NULL));
	pos = rand() % N_ELIMINATION_ARRAY;
	return ElimArrayExchange(&elimination_array->arr[pos], value);
}
#endif
