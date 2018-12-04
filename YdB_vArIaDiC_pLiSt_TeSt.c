/****************************************************************
 *								*
 * Copyright (c) 2018 YottaDB LLC. and/or its subsidiaries.	*
 * All rights reserved.						*
 *								*
 *	This source code contains the intellectual property	*
 *	of its copyright holder(s), and is made available	*
 *	under a license.  If you do not know the terms of	*
 *	the license, please stop and do not read further.	*
 *								*
 ****************************************************************/

/* C test routine for variadic plist structures - print the contents of pass-ed in plist (driven from yottadb package) */

#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include "libyottadb.h"
/* Note these defines also appear in the yottadb package (yottadb.go) so if they change there, change them here */
#define expectedargs	3
#define expectedval	42
#define expectedbuf1	"Buffer one"
#define expectedbuf2	"Buffer two"
#define debugFlag	0

int YdB_vArIaDiC_pLiSt_TeSt(int argcnt, ...);

/* Test routine only ever intended to be called from the TestVariadicPlist() routine in yottadb.go */
int YdB_vArIaDiC_pLiSt_TeSt(int argcnt, ...)
{
	va_list		var;
	int		num;
	ydb_buffer_t	*buft1, *buft2;

	if (argcnt != expectedargs)
	{
		printf("VPLST: FAIL test - Argument count is wrong - expected arg cont %d but received %d\n", expectedargs, argcnt);
		return 1;
	}
	va_start(var, argcnt);
	num = va_arg(var, int);		/* Pull in what we hope is expectedval */
	if (expectedval != num)
	{
		printf("VPLST: FAIL test - First parameter is wrong - expected %d but received %d\n", expectedval, num);
		return 1;
	}
	buft1 = va_arg(var, ydb_buffer_t *);
	if (debugFlag)
	{
		printf("VPLST:\n");
		printf("VPLST: Address of buft1: 0x%016lx\n", (unsigned long)buft1);
		printf("VPLST:   buf_addr:       0x%016lx\n", (unsigned long)buft1->buf_addr);
		printf("VPLST:   len_alloc:      %d\n", buft1->len_alloc);
		printf("VPLST:   len_used:       %d\n", buft1->len_used);
	}
	if ((buft1->len_used != strlen(expectedbuf1)) || (0 != memcmp(expectedbuf1, buft1->buf_addr, buft1->len_used)))
	{
		printf("VPLST: FAIL test - Buffer1 content is wrong - expected %s but received %.*s\n",
		       expectedbuf1, buft1->len_used, buft1->buf_addr);
		return 1;
	} else if (debugFlag)
		printf("VPLST:   value:          %.*s\n", buft1->len_used, buft1->buf_addr);
	if (debugFlag)
		printf("VPLST:\n");
	buft2 = va_arg(var, ydb_buffer_t *);
	if (debugFlag)
	{
		printf("VPLST: Address of buft2: 0x%016lx\n", (unsigned long)buft2);
		printf("VPLST:   buf_addr:       0x%016lx\n", (unsigned long)buft2->buf_addr);
		printf("VPLST:   len_alloc:      %d\n", buft2->len_alloc);
		printf("VPLST:   len_used:       %d\n", buft2->len_used);
	}
	if ((buft2->len_used != strlen(expectedbuf2)) || (0 != memcmp(expectedbuf2, buft2->buf_addr, buft2->len_used)))
	{
		printf("VPLST: FAIL test - Buffer2 content is wrong - expected %s but received %.*s\n",
		       expectedbuf2, buft2->len_used, buft2->buf_addr);
		return 1;
	} else if (debugFlag)
		printf("VPLST:   value:          %.*s\n", buft2->len_used, buft2->buf_addr);
	if (debugFlag)
		printf("VPLST:\n");
	//printf("VPLST: PASS\n");
	return 0;
}
