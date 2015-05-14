
/*-------------------------------------------------------------------------
 *
 *		  foreign-data wrapper for Redis
 *
 * Copyright (c) 2011,2013 PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Authors: Dave Page <dpage@pgadmin.org>
 *          Andrew Dunstan <andrew@dunslane.net>
 *
 * IDENTIFICATION
 *		  redis_fdw/redis_fdw.c
 *
 *-------------------------------------------------------------------------
 */

/* Debug mode */
/* #define DEBUG */

#include "postgres.h"

/* check that we are compiling for the right postgres version */
#if PG_VERSION_NUM < 90500 
#error wrong Postgresql version this branch is only for 9.5
#endif


#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <hiredis/hiredis.h>

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

#define PROCID_TEXTEQ 67

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct RedisFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for redis_fdw.
 *
 */
static struct RedisFdwOption valid_options[] =
{

	/* Connection options */
	{"address", ForeignServerRelationId},
	{"port", ForeignServerRelationId},
	{"password", UserMappingRelationId},
	{"database", ForeignTableRelationId},

	/* table options */
	{"singleton_key", ForeignTableRelationId},
	{"tablekeyprefix", ForeignTableRelationId},
	{"tablekeyset", ForeignTableRelationId},
	{"tabletype", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

typedef enum
{
	PG_REDIS_SCALAR_TABLE = 0,
	PG_REDIS_HASH_TABLE,
	PG_REDIS_LIST_TABLE,
	PG_REDIS_SET_TABLE,
	PG_REDIS_ZSET_TABLE
} redis_table_type;

typedef struct redisTableOptions
{
	char *address;
	int   port;
	char *password;
	int   database;
	char *keyprefix;
	char *keyset;
	char *singleton_key;
	redis_table_type table_type;
} redisTableOptions, *RedisTableOptions;


	
typedef struct
{
	char	   *svr_address;
	int			svr_port;
	char	   *svr_password;
	int			svr_database;
}	RedisFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct RedisFdwExecutionState
{
	AttInMetadata *attinmeta;
	redisContext *context;
	redisReply *reply;
	long long	row;
	char	   *address;
	int			port;
	char	   *password;
	int			database;
	char       *keyprefix;
	char       *keyset;
	char       *qual_value;
	char       *singleton_key;
	redis_table_type table_type;
	char       *cursor_search_string;
	char       *cursor_id;
}	RedisFdwExecutionState;

/* initial cursor */
#define ZERO "0"
/* redis default is 10 - let's fetch 1000 at a time */
#define COUNT " COUNT 1000"

/*
 * SQL functions
 */
extern Datum redis_fdw_handler(PG_FUNCTION_ARGS);
extern Datum redis_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(redis_fdw_handler);
PG_FUNCTION_INFO_V1(redis_fdw_validator);

/*
 * FDW callback routines
 */
static void redisGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid);
static void redisGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid);
static ForeignScan *redisGetForeignPlan(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *tlist,
					List *scan_clauses);
static void redisExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void redisBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *redisIterateForeignScan(ForeignScanState *node);
static inline TupleTableSlot *redisIterateForeignScanMulti(ForeignScanState *node);
static inline TupleTableSlot *redisIterateForeignScanSingleton(ForeignScanState *node);
static void redisReScanForeignScan(ForeignScanState *node);
static void redisEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static bool redisIsValidOption(const char *option, Oid context);
static void redisGetOptions(Oid foreigntableid, RedisTableOptions options); 
static void redisGetQual(Node *node, TupleDesc tupdesc, char **key, char **value, bool *pushdown);
static char *process_redis_array(redisReply *reply,	redis_table_type type);
/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
redis_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#ifdef DEBUG
	elog(NOTICE, "redis_fdw_handler");
#endif

	fdwroutine->GetForeignRelSize = redisGetForeignRelSize;
	fdwroutine->GetForeignPaths = redisGetForeignPaths;
	fdwroutine->GetForeignPlan = redisGetForeignPlan;
	/* can't ANALYSE redis */
	fdwroutine->AnalyzeForeignTable = NULL;
	fdwroutine->ExplainForeignScan = redisExplainForeignScan;
	fdwroutine->BeginForeignScan = redisBeginForeignScan;
	fdwroutine->IterateForeignScan = redisIterateForeignScan;
	fdwroutine->ReScanForeignScan = redisReScanForeignScan;
	fdwroutine->EndForeignScan = redisEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
redis_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *svr_address = NULL;
	int			svr_port = 0;
	char	   *svr_password = NULL;
	int			svr_database = 0;
	redis_table_type tabletype = PG_REDIS_SCALAR_TABLE;
	char       *tablekeyprefix = NULL;
	char       *tablekeyset = NULL;
	char       *singletonkey = NULL;
	ListCell   *cell;

#ifdef DEBUG
	elog(NOTICE, "redis_fdw_validator");
#endif

	/*
	 * Check that only options supported by redis_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!redisIsValidOption(def->defname, catalog))
		{
			struct RedisFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s", 
							 buf.len ? buf.data : "<none>")
					 ));
		}

		if (strcmp(def->defname, "address") == 0)
		{
			if (svr_address)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: "
									   "address (%s)", defGetString(def))
								));

			svr_address = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: port (%s)", 
								defGetString(def))
						 ));

			svr_port = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "password") == 0)
		{
			if (svr_password)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: password")
								));

			svr_password = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			if (svr_database)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: database "
								"(%s)", defGetString(def))
						 ));

			svr_database = atoi(defGetString(def));
		}
		else if (strcmp(def->defname, "singleton_key ") == 0)
		{
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyset(%s) and "
								"singleton_key (%s)", tablekeyset, 
								defGetString(def))
						 ));
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyprefix(%s) and "
								"singleton_key (%s)", tablekeyprefix, 
								defGetString(def))
						 ));
			if (singletonkey )
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"singleton_key (%s)", defGetString(def))
						 ));

			singletonkey = defGetString(def);
		}
		else if (strcmp(def->defname, "tablekeyprefix") == 0)
		{
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyset(%s) and "
								"tablekeyprefix (%s)", tablekeyset, 
								defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: singleton_key(%s) and "
								"tablekeyprefix (%s)", singletonkey, 
								defGetString(def))
						 ));
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"tablekeyprefix (%s)", defGetString(def))
						 ));

			tablekeyprefix = defGetString(def);
		}
		else if (strcmp(def->defname, "tablekeyset") == 0)
		{
			if (tablekeyprefix)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: tablekeyprefix (%s) and "
								"tablekeyset (%s)", tablekeyprefix, 
								defGetString(def))
						 ));
			if (singletonkey)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting options: singleton_key(%s) and "
								"tablekeyset (%s)", singletonkey, 
								defGetString(def))
						 ));
			if (tablekeyset)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: "
								"tablekeyset (%s)", defGetString(def))
						 ));

			tablekeyset = defGetString(def);
		}
		else if (strcmp(def->defname, "tabletype") == 0)
		{
			char *typeval = defGetString(def);
			if (tabletype)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: tabletype "
								"(%s)", typeval)));
			if (strcmp(typeval,"hash") == 0)
				tabletype = PG_REDIS_HASH_TABLE;
			else if (strcmp(typeval,"list") == 0)
				tabletype = PG_REDIS_LIST_TABLE;
			else if (strcmp(typeval,"set") == 0)
				tabletype = PG_REDIS_SET_TABLE;
			else if (strcmp(typeval,"zset") == 0)
				tabletype = PG_REDIS_ZSET_TABLE;
			else
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("invalid tabletype (%s) - must be hash, "
								"list, set or zset", typeval)));
		}
	}

	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
redisIsValidOption(const char *option, Oid context)
{
	struct RedisFdwOption *opt;

#ifdef DEBUG
	elog(NOTICE, "redisIsValidOption");
#endif

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a redis_fdw foreign table.
 */
static void
redisGetOptions(Oid foreigntableid,RedisTableOptions table_options) 
{
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *mapping;
	List	   *options;
	ListCell   *lc;

#ifdef DEBUG
	elog(NOTICE, "redisGetOptions");
#endif

	/*
	 * Extract options from FDW objects. We only need to worry about server
	 * options for Redis
	 *
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	mapping = GetUserMapping(GetUserId(), table->serverid);

	options = NIL;
	options = list_concat(options, table->options);
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

//	table_options->table_type = PG_REDIS_SCALAR_TABLE;

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "address") == 0)
			table_options->address = defGetString(def);

		if (strcmp(def->defname, "port") == 0)
			table_options->port = atoi(defGetString(def));

		if (strcmp(def->defname, "password") == 0)
			table_options->password = defGetString(def);

		if (strcmp(def->defname, "database") == 0)
			table_options->database = atoi(defGetString(def));

		if (strcmp(def->defname, "tablekeyprefix") == 0)
			table_options->keyprefix = defGetString(def);

		if (strcmp(def->defname, "tablekeyset") == 0)
			table_options->keyset = defGetString(def);

		if (strcmp(def->defname, "singleton_key") == 0)
			table_options->singleton_key = defGetString(def);

		if (strcmp(def->defname, "tabletype") == 0)
		{
			char *typeval = defGetString(def);

			if (strcmp(typeval,"hash") == 0)
				table_options->table_type = PG_REDIS_HASH_TABLE;
			else if (strcmp(typeval,"list") == 0)
				table_options->table_type = PG_REDIS_LIST_TABLE;
			else if (strcmp(typeval,"set") == 0)
				table_options->table_type = PG_REDIS_SET_TABLE;
			else if (strcmp(typeval,"zset") == 0)
				table_options->table_type = PG_REDIS_ZSET_TABLE;
		}
	}

	/* Default values, if required */
	if (!table_options->address)
		table_options->address = "127.0.0.1";

	if (!table_options->port)
		table_options->port = 6379;

	if (!table_options->database)
		table_options->database = 0;
}


static void
redisGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid)
{
	RedisFdwPlanState *fdw_private;
	redisTableOptions table_options;

	redisContext *context;
	redisReply *reply;
	struct timeval timeout = {1, 500000};

#ifdef DEBUG
	elog(NOTICE, "redisGetForeignRelSize");
#endif

	/*
	 * Fetch options. Get everything so we don't need to re-fetch it later in
	 * planning.
	 */
	fdw_private = (RedisFdwPlanState *) palloc(sizeof(RedisFdwPlanState));
	baserel->fdw_private = (void *) fdw_private;

	table_options.address = NULL;
	table_options.port = 0;
	table_options.password = NULL;
	table_options.database = 0;
	table_options.keyprefix = NULL;
	table_options.keyset = NULL;
	table_options.singleton_key = NULL;
	table_options.table_type = PG_REDIS_SCALAR_TABLE;

	redisGetOptions(foreigntableid, &table_options);
	fdw_private->svr_address = table_options.address;
	fdw_private->svr_password = table_options.password;
	fdw_private->svr_port = table_options.port;
	fdw_private->svr_database = table_options.database;

	/* Connect to the database */
	context = redisConnectWithTimeout(table_options.address, table_options.port,
									  timeout);

	if (context->err)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to connect to Redis: %d", context->err)
				 ));

	/* Authenticate */
	if (table_options.password)
	{
		reply = redisCommand(context, "AUTH %s", table_options.password);

		if (!reply)
		{
			redisFree(context);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed to authenticate to redis: %d", 
							context->err)));
		}

		freeReplyObject(reply);
	}

	/* Select the appropriate database */
	reply = redisCommand(context, "SELECT %d", table_options.database);

	if (!reply)
	{
		redisFree(context);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to select database %d: %d", 
						table_options.database,  context->err)
				 ));
	}

	/* Execute a query to get the table size */
#if 0
	/*
	 * KEYS is potentiallyexpensive, so this test is disabled and we use a
	 * fairly dubious heuristic instead.
	 */
	if (table_options.keyprefix)
	{
		/* it's a pity there isn't an NKEYS command in Redis */
		int len = strlen(table_options.keyprefix) + 2;
		char *buff = palloc(len * sizeof(char));
		snprintf(buff,len, "%s*",table_options.keyprefix);
		reply = redisCommand(context,"KEYS %s",buff);
	} else
#endif
	if (table_options.singleton_key)
	{
		switch (table_options.table_type)
		{
			case PG_REDIS_SCALAR_TABLE:
				baserel->rows = 1;
				return;
			case PG_REDIS_HASH_TABLE:
				reply = redisCommand(context, "HLEN %s",table_options.singleton_key);
				break;
			case PG_REDIS_LIST_TABLE:
				reply = redisCommand(context, "LLEN %s",table_options.singleton_key);
				break;
			case PG_REDIS_SET_TABLE:
				reply = redisCommand(context, "SCARD %s",table_options.singleton_key);
				break;
			case PG_REDIS_ZSET_TABLE:
				reply = redisCommand(context, "ZCARD %s",table_options.singleton_key);
				break;
			default:
				;
		}
	}
	else if (table_options.keyset)
	{ 
		reply = redisCommand(context,"SCARD %s",table_options.keyset);
	}
	else
	{
		reply = redisCommand(context, "DBSIZE");
	}

	if (!reply)
	{
		redisFree(context);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to get the database size: %d", context->err)
				 ));
	}

#if 0
	if (reply->type == REDIS_REPLY_ARRAY)
		baserel->rows = reply->elements;
	else
#endif
		if (table_options.keyprefix)
			baserel->rows = reply->integer / 20;
		else
			baserel->rows = reply->integer;

	freeReplyObject(reply);
	redisFree(context);


}

/*
 * redisGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in redis.
 */
static void
redisGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid)
{
	RedisFdwPlanState *fdw_private = baserel->fdw_private;

	Cost		startup_cost,
				total_cost;

#ifdef DEBUG
	elog(NOTICE, "redisGetForeignPaths");
#endif

	if (strcmp(fdw_private->svr_address, "127.0.0.1") == 0 ||
		strcmp(fdw_private->svr_address, "localhost") == 0)
		startup_cost = 10;
	else
		startup_cost = 25;

	total_cost = startup_cost + baserel->rows;


	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,		/* no pathkeys */
									 NULL,		/* no outer rel either */
									 NIL));		/* no fdw_private data */

}

static ForeignScan *
redisGetForeignPlan(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *tlist,
					List *scan_clauses)
{
	Index		scan_relid = baserel->relid;

#ifdef DEBUG
	elog(NOTICE, "redisGetForeignPlan");
#endif

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL,	/* no expressions to evaluate */
							NIL,	/* no private state either */
							NIL);   /* no custom tlist */
}

/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
redisExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	redisReply *reply;

	RedisFdwExecutionState *festate = 
		(RedisFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "redisExplainForeignScan");
#endif

	if (!es->costs)
		return;

	/* 
	 * Execute a query to get the table size 
	 *
	 * See above for more details.
	 */
   
	if (festate->keyset)
	{ 
		reply = redisCommand(festate->context, "SCARD %s", festate->keyset);
	}
	else
	{
		reply = redisCommand(festate->context, "DBSIZE");
	}

	if (!reply)
	{
		redisFree(festate->context);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
		 errmsg("failed to get the table size: %d", festate->context->err)
				 ));
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		char	   *err = pstrdup(reply->str);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to get the table size: %s", err)
				 ));
	}

	ExplainPropertyLong("Foreign Redis Table Size", 
						festate->keyprefix ? reply->integer / 20 : 
						reply->integer, 
						es);

	freeReplyObject(reply);
}

/*
 * redisBeginForeignScan
 *		Initiate access to the database
 */
static void
redisBeginForeignScan(ForeignScanState *node, int eflags)
{
	redisTableOptions table_options;
	redisContext *context;
	redisReply *reply;
	char	   *qual_key = NULL;
	char	   *qual_value = NULL;
	bool		pushdown = false;
	RedisFdwExecutionState *festate;
	struct timeval timeout = {1, 500000};

#ifdef DEBUG
	elog(NOTICE, "BeginForeignScan");
#endif

	table_options.address = NULL;
	table_options.port = 0;
	table_options.password = NULL;
	table_options.database = 0;
	table_options.keyprefix = NULL;
    table_options.keyset = NULL;
	table_options.singleton_key = NULL;
    table_options.table_type = PG_REDIS_SCALAR_TABLE;


   /* Fetch options  */
   redisGetOptions(RelationGetRelid(node->ss.ss_currentRelation), 
				   &table_options);

	/* Connect to the server */
	context = redisConnectWithTimeout(table_options.address, 
									  table_options.port, timeout);

	if (context->err)
	{
		redisFree(context);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to connect to Redis: %s", context->errstr)
				 ));
	}

	/* Authenticate */
	if (table_options.password)
	{
		reply = redisCommand(context, "AUTH %s", table_options.password);

		if (!reply)
		{
			redisFree(context);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			   errmsg("failed to authenticate to redis: %s", context->errstr)
					 ));
		}

		freeReplyObject(reply);
	}

	/* Select the appropriate database */
	reply = redisCommand(context, "SELECT %d", table_options.database);

	if (!reply)
	{
		redisFree(context);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to select database %d: %s", 
						table_options.database, context->errstr)
				 ));
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		char	   *err = pstrdup(reply->str);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to select database %d: %s", 
						table_options.database, err)
				 ));
	}

	freeReplyObject(reply);

	/* See if we've got a qual we can push down */
	if (node->ss.ps.plan->qual)
	{
		ListCell   *lc;

		foreach(lc, node->ss.ps.qual)
		{
			/* Only the first qual can be pushed down to Redis */
			ExprState  *state = lfirst(lc);

			redisGetQual((Node *) state->expr, 
						 node->ss.ss_currentRelation->rd_att, 
						 &qual_key, &qual_value, &pushdown);
			if (pushdown)
				break;
		}
	}

	/* Stash away the state info we have already */
	festate = (RedisFdwExecutionState *) palloc(sizeof(RedisFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->context = context;
	festate->reply = NULL;
	festate->row = 0;
	festate->address = table_options.address;
	festate->port = table_options.port;
	festate->keyprefix = table_options.keyprefix;
	festate->keyset = table_options.keyset;
	festate->singleton_key = table_options.singleton_key;
	festate->table_type = table_options.table_type;
	festate->cursor_id = NULL;
	festate->cursor_search_string = NULL;
	
	festate->qual_value = pushdown ? qual_value : NULL;

	/* OK, we connected. If this is an EXPLAIN, bail out now */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Execute the query */
	if (festate->singleton_key)
	{

	  /*
	   * We're not using cursors for now for singleton key tables.
	   * The theory is that we don't expect them to be so large
	   * in normal use that we would get any significant benefit from
	   * doing so, and in any case scanning them in a single step is not
	   * going to tie things up like scannoing the whole Redis database
	   * could.
	   */

		switch (table_options.table_type)
		{
			case PG_REDIS_SCALAR_TABLE:
				reply = redisCommand(context,"GET %s",festate->singleton_key);
				break;
			case PG_REDIS_HASH_TABLE:
				/* the singleton case where a qual pushdown makes most sense */
				if (qual_value && pushdown)
					reply = redisCommand(context,"HGET %s %s",festate->singleton_key, qual_value);
				else
					reply = redisCommand(context,"HGETALL %s",festate->singleton_key);
				break;
			case PG_REDIS_LIST_TABLE:
				reply = redisCommand(context, "LRANGE %s 0 -1",table_options.singleton_key);
				break;
			case PG_REDIS_SET_TABLE:
				reply = redisCommand(context, "SMEMBERS %s",table_options.singleton_key);
				break;
			case PG_REDIS_ZSET_TABLE:
				reply = redisCommand(context, "ZRANGE %s 0 -1 WITHSCORES",table_options.singleton_key);
				break;
			default:
				;
		}
	}
	else if (qual_value && pushdown)
	{
		/* 
		 * if we have a qual, make sure it's a member of the keyset or has
		 * the right prefix if either of these options is specified.
		 *
		 * If not set row to -1 to indicate failure
		 */
		if (festate->keyset)
		{
			redisReply *sreply;
			sreply = redisCommand(context, "SISMEMBER %s %s", 
								  festate->keyset, qual_value);
			if(!sreply)
			{
				redisFree(festate->context);
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						 errmsg("failed to list keys: %s", context->errstr)
							));
			}
			if (sreply->type == REDIS_REPLY_ERROR)
			{
				char	   *err = pstrdup(sreply->str);
				
				freeReplyObject(sreply);
				ereport(ERROR,
						(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
						 errmsg("failed to list keys: %s", err)
							));

			}
			
			if (sreply->integer != 1)
				festate->row =-1;

		}
		else if (festate->keyprefix)
		{
			if (strncmp(qual_value, festate->keyprefix, 
						strlen(festate->keyprefix)) != 0)
				festate->row = -1;
		}

		/*
		 * For a qual we don't want to scan at all,
		 * just check that the key exists. We do this
		 * check in adddition to the keyset/keyprefix checks,
		 * is any, so we know the item is really there.
		 */

		reply = redisCommand(context,"EXISTS %s",qual_value);
		if (reply->integer == 0)
			festate->row = -1;

	}
	else
	{
		/* no qual - do a cursor scan */
		if (festate->keyset)
		{
			festate->cursor_search_string = "SSCAN %s %s" COUNT;
			reply = redisCommand(context, festate->cursor_search_string,
								 festate->keyset, ZERO);
		}
		else if (festate->keyprefix)
		{
			festate->cursor_search_string = "SCAN %s MATCH %s*" COUNT;
			reply = redisCommand(context, festate->cursor_search_string,
								 ZERO, festate->keyprefix);
		}
		else
		{
			festate->cursor_search_string = "SCAN %s" COUNT;
			reply = redisCommand(context, festate->cursor_search_string, ZERO);
		}
	}

	if (!reply)
	{
		redisFree(festate->context);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to list keys: %s", context->errstr)
				 ));
	}
	else if (reply->type == REDIS_REPLY_ERROR)
	{
		char	   *err = pstrdup(reply->str);
		
		freeReplyObject(reply);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed somehow: %s", err)
					));
	}

	/* Store the additional state info */
	festate->attinmeta = 
		TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);

	if (festate->singleton_key)
	{
		festate->reply = reply;
	}
	else if (festate->row > -1 && festate->qual_value == NULL)
	{
		redisReply *cursor = reply->element[0];

		if (cursor->type == REDIS_REPLY_STRING)
		{
			if (cursor->len == 1 && cursor->str[0] == '0')
				festate->cursor_id = NULL;
			else
				festate->cursor_id = pstrdup(cursor->str);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("wrong reply type %d", cursor->type)
						));
		}

		/* for cursors, this is the list of elements */
		festate->reply = reply->element[1];
	}
}

/*
 * redisIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 *
 * We have now spearated this into two streams of logic - one
 * for singleton key tables and one for multi-key tables.
 */

static TupleTableSlot *
redisIterateForeignScan(ForeignScanState *node)
{
	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;
	if (festate->singleton_key)
		return redisIterateForeignScanSingleton(node);
	else
		return redisIterateForeignScanMulti(node);
}

static inline TupleTableSlot *
redisIterateForeignScanMulti(ForeignScanState *node)
{
	bool		found;
	redisReply *reply = 0;
	char	   *key;
	char	   *data = 0;
	char	  **values;
	HeapTuple	tuple;

	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

#ifdef DEBUG
	elog(NOTICE, "redisIterateForeignScanMulti");
#endif

	/* Cleanup */
	ExecClearTuple(slot);

	/* Get the next record, and set found */
	found = false;

	/*
	 * If we're out of rows on the cursor, fetch the next set.
	 * Keep going until we get a result back that actually has some rows.
	 */
	while (festate->cursor_id != NULL &&
		   festate->row >= festate->reply->elements)
	{
		redisReply *creply;
		redisReply *cursor;

		Assert(festate->qual_value == NULL);

		if (festate->keyset)
		{
			creply = redisCommand(festate->context,
								  festate->cursor_search_string,
								  festate->keyset, festate->cursor_id);
		}
		else if (festate->keyprefix)
		{
			creply = redisCommand(festate->context,
								  festate->cursor_search_string,
								  festate->cursor_id, festate->keyprefix);
		}
		else
		{
			creply = redisCommand(festate->context,
								  festate->cursor_search_string,
								  festate->cursor_id);
		}

		if (!creply)
		{
			redisFree(festate->context);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to list keys: %s",
							festate->context->errstr)
						));
		}
		else if (creply->type == REDIS_REPLY_ERROR)
		{
			char	   *err = pstrdup(creply->str);

			freeReplyObject(creply);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("failed somehow: %s", err)
						));
		}

		cursor  = creply->element[0];

		if (cursor->type == REDIS_REPLY_STRING)
		{
			if (cursor->len == 1 && cursor->str[0] == '0')
				festate->cursor_id = NULL;
			else
				festate->cursor_id = pstrdup(cursor->str);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
					 errmsg("wrong reply type %d", cursor->type)
						));
		}

		festate->reply = creply->element[1];
		festate->row = 0;
	}

	/*
	 * -1 means we failed the qual test, so there are no rows
	 * or we've already processed the qual
	 */

	if (festate->row >  -1 &&
		(festate->qual_value != NULL ||
		 (festate->row < festate->reply->elements)))
	{
		/*
		 * Get the row, check the result type, and handle accordingly. If it's
		 * nil, we go ahead and get the next row.
		 */
		do
		{

			key = festate->qual_value != NULL ?
				festate->qual_value :
				festate->reply->element[festate->row]->str;
			switch(festate->table_type)
			{
				case PG_REDIS_HASH_TABLE:
					reply = redisCommand(festate->context, 
										  "HGETALL %s", key);
					break;
				case PG_REDIS_LIST_TABLE:
					reply = redisCommand(festate->context, 
										  "LRANGE %s 0 -1", key);
					break;
				case PG_REDIS_SET_TABLE:
					reply = redisCommand(festate->context, 
										  "SMEMBERS %s", key);
					break;
				case PG_REDIS_ZSET_TABLE:
					reply = redisCommand(festate->context, 
										  "ZRANGE %s 0 -1", key);
					break;
				case PG_REDIS_SCALAR_TABLE:
				default:
					reply = redisCommand(festate->context, 
										 "GET %s", key);
			}

			if (!reply)
			{
				freeReplyObject(festate->reply);
				redisFree(festate->context);
				ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
								errmsg("failed to get the value for key \"%s\": %s",
									   key, festate->context->errstr)
								));
			}

			festate->row++;

		} while ((reply->type == REDIS_REPLY_NIL ||
				  reply->type == REDIS_REPLY_STATUS ||
				  reply->type == REDIS_REPLY_ERROR) &&
				 festate->qual_value == NULL &&
				 festate->row < festate->reply->elements);

		if (festate->qual_value != NULL || festate->row <= festate->reply->elements)
		{
			/*
			 * Now, deal with the different data types we might have got from
			 * Redis.
			 */

			switch (reply->type)
			{
				case REDIS_REPLY_INTEGER:
					data = (char *) palloc(sizeof(char) * 64);
					snprintf(data, 64, "%lld", reply->integer);
					found = true;
					break;

				case REDIS_REPLY_STRING:
					data = reply->str;
					found = true;
					break;

				case REDIS_REPLY_ARRAY:
					data = process_redis_array(reply, festate->table_type);
					found = true;
					break;
			}
		}

		/* make sure we don't try to process the qual row twice */
		if (festate->qual_value != NULL)
			festate->row = -1;
	}

	/* Build the tuple */
	if (found)
	{
		values = (char **) palloc(sizeof(char *) * 2);
		values[0] = key;
		values[1] = data;
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}

	/* Cleanup */
	if (reply)
		freeReplyObject(reply);

	return slot;
}

static inline TupleTableSlot *
redisIterateForeignScanSingleton(ForeignScanState *node)
{
	bool		found;
	char	   *key = NULL;
	char	   *data = NULL;
	char	  **values;
	HeapTuple	tuple;

	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

#ifdef DEBUG
	elog(NOTICE, "redisIterateForeignScanSingleton");
#endif

	/* Cleanup */
	ExecClearTuple(slot);

	if (festate->row < 0)
		return slot;

	/* Get the next record, and set found */
	found = false;

	if (festate->table_type == PG_REDIS_SCALAR_TABLE)
	{
		festate->row = -1; /* just one row for a scalar */
		switch (festate->reply->type)
		{
			case REDIS_REPLY_INTEGER:
				key = (char *) palloc(sizeof(char) * 64);
				snprintf(key, 64, "%lld", festate->reply->integer);
				found = true;
				break;
				
			case REDIS_REPLY_STRING:
				key = festate->reply->str;
				found = true;
				break;
				
			case REDIS_REPLY_ARRAY:
				freeReplyObject(festate->reply);
				redisFree(festate->context);
				ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
								errmsg("not expecting an array for a singleton scalar table")
							));
				break;
		}
	}
	else if (festate->table_type == PG_REDIS_HASH_TABLE && festate->qual_value)
	{
		festate->row = -1; /* just one row for qual'd search in a hash */
		key = festate->qual_value;
		switch (festate->reply->type)
		{
			case REDIS_REPLY_INTEGER:
			    data = (char *) palloc(sizeof(char) * 64);
				snprintf(data, 64, "%lld", festate->reply->integer);
				found = true;
				break;
				
			case REDIS_REPLY_STRING:
				data = festate->reply->str;
				found = true;
				break;
				
			case REDIS_REPLY_ARRAY:
				freeReplyObject(festate->reply);
				redisFree(festate->context);
				ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
								errmsg("not expecting an array for a single hash property: %s", festate->qual_value)
							));
				break;
		}
	}
	else if (festate->row < festate->reply->elements)
	{
		/* everything else comes in as an array reply type */
		found = true;
		key = festate->reply->element[festate->row]->str;
		festate->row++;
		if (festate->table_type == PG_REDIS_HASH_TABLE || 
			festate->table_type == PG_REDIS_ZSET_TABLE)
		{
			redisReply *dreply = festate->reply->element[festate->row];
			
			switch (dreply->type)
			{
				case REDIS_REPLY_INTEGER:
					data = (char *) palloc(sizeof(char) * 64);
					snprintf(key, 64, "%lld", dreply->integer);
					break;

				case REDIS_REPLY_STRING:
					data = dreply->str;
					break;

				case REDIS_REPLY_ARRAY:
					freeReplyObject(festate->reply);
					redisFree(festate->context);
					ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
									errmsg("not expecting array for a hash value or zset score")
								));
					break;
			}
			festate->row++;
		}
	}

	/* Build the tuple */
	values = (char **) palloc(sizeof(char *) * 2);

	if (found)
	{
		values[0] = key;
		values[1] = data;
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}

	return slot;
}

/*
 * redisEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
redisEndForeignScan(ForeignScanState *node)
{
	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "redisEndForeignScan");
#endif

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		if (festate->reply)
			freeReplyObject(festate->reply);

		if (festate->context)
			redisFree(festate->context);
	}
}

/*
 * redisReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
redisReScanForeignScan(ForeignScanState *node)
{
	RedisFdwExecutionState *festate = (RedisFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "redisReScanForeignScan");
#endif

	if (festate->row > -1)
		festate->row = 0;
}

static void
redisGetQual(Node *node, TupleDesc tupdesc, char **key, char **value, bool *pushdown)
{
	*key = NULL;
	*value = NULL;
	*pushdown = false;

	if (!node)
		return;

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		Node	   *left,
				   *right;
		Index		varattno;

		if (list_length(op->args) != 2)
			return;

		left = list_nth(op->args, 0);

		if (!IsA(left, Var))
			return;

		varattno = ((Var *) left)->varattno;

		right = list_nth(op->args, 1);

		if (IsA(right, Const))
		{
			StringInfoData buf;

			initStringInfo(&buf);

			/* And get the column and value... */
			*key = NameStr(tupdesc->attrs[varattno - 1]->attname);
			*value = TextDatumGetCString(((Const *) right)->constvalue);

			/*
			 * We can push down this qual if: - The operatory is TEXTEQ - The
			 * qual is on the key column
			 */
			if (op->opfuncid == PROCID_TEXTEQ && strcmp(*key, "key") == 0)
				*pushdown = true;

			return;
		}
	}

	return;
}


static char *
process_redis_array(redisReply *reply,	redis_table_type type) 
{
    StringInfo res = makeStringInfo();
    int i;
    bool need_sep = false;

    appendStringInfoChar(res,'{');
    for (i = 0; i < reply->elements; i++)
    {
        redisReply *ir = reply->element[i];
        if (need_sep)
            appendStringInfoChar(res,',');
        need_sep = true;
        if (ir->type == REDIS_REPLY_ARRAY)
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE), /* ??? */
                     errmsg("nested array returns not yet supported")));
        switch (ir->type)
        {
            case REDIS_REPLY_STATUS:
            case REDIS_REPLY_STRING:
			{
				char *buff;
				char *crs;
				int i;
                pg_verifymbstr(ir->str, ir->len, false);
				buff = palloc(ir->len * 2 + 3);
				crs = buff;
				*crs++ = '"';
				for (i = 0; i < ir->len; i++)
				{
					if (ir->str[i] == '"' || ir->str[i] == '\\')
						*crs++ = '\\';
					*crs++ = ir->str[i];
				}
				*crs++ = '"';
				*crs = '\0';
                appendStringInfoString(res,buff);
				pfree(buff);
			}
                break;
            case REDIS_REPLY_INTEGER:
                appendStringInfo(res,"%lld",ir->integer);
                break;
            case REDIS_REPLY_NIL:
                appendStringInfoString(res,"NULL");
                break;
            default:
                break;
        }
    }
    appendStringInfoChar(res,'}');
    
    return res->data;
}
