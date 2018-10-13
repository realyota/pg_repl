/*-------------------------------------------------------------------------
 *
 * ddl_repl.c
 *
 *
 * Copyright (c) 2008-2017, Maciej B¹k
 *
 * IDENTIFICATION
 *	  contrib/ddl_repl/ddl_repl.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

//#include <limits.h>

#include "utils/guc.h"
#include "catalog/objectaccess.h"

PG_MODULE_MAGIC;

/* GUC variables */
//static char * depostgresql_message = "Unknown error";
static bool ddlrepl_enabled; /* whether replicate ddl commands across cluster */

/* Saved hook values in case of unload */
/* Hooks for DCL commands */
static object_access_hook_type prev_object_access_hook_type = NULL;

/* Hooks for DML commands */
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

void		_PG_init(void);
void		_PG_fini(void);

static void depostgresql_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void depostgresql_ExecutorRun(QueryDesc *queryDesc,
					ScanDirection direction,
					uint64 count);
static void depostgresql_ExecutorFinish(QueryDesc *queryDesc);
static void depostgresql_ExecutorEnd(QueryDesc *queryDesc);


/*
 * Module load callback
 */
void
_PG_init(void)
{

	/* define GUC variables */
	DefineCustomBoolVariable("ddl_repl.enabled",
	   "Selects whether ddl replication across cluster is enabled.",
							 NULL,
							 &ddlrepl_enabled,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* Install hooks saving previously ones*/
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = explain_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = explain_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = explain_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = explain_ExecutorEnd;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks restoring prevorious. */
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}

/*
 * ExecutorStart hook: start up logging if needed
 */
static void
depostgreql_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (ddlrepl_enabled)
		elog(ERROR, "This is a test error!");
}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
depostgresql_ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count)
{

}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
depostgresql_ExecutorFinish(QueryDesc *queryDesc)
{

}

/*
 * ExecutorEnd hook: log results if needed
 */
static void
depostgresql_ExecutorEnd(QueryDesc *queryDesc)
{

}
