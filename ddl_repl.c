/*-------------------------------------------------------------------------
*
* ddl_repl.c
*
*
* Copyright (c) 2017-2018, Maciej B¹k
*
* IDENTIFICATION
*	  contrib/ddl_repl/ddl_repl.c
* TODO:
* - closing connection on rollback
* - check for create extension sql
* - commit/rollback statements are send and parsed alone
*
*-------------------------------------------------------------------------
*/
#include "postgres.h"
#include "c.h"

#include "utils/memutils.h"
#include "libpq-fe.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "tcop/utility.h"
#include "executor/spi.h"
#include "pg_config_manual.h"

PG_MODULE_MAGIC;

/*
* Connection cache hash table entry
*
* The lookup key in this hash table is the id of ddl_repl.nodes table. We use just one
* connection per node.
*/
//typedef char * CachedConnKey;

typedef struct CachedConnKey
{
	char	 * node_name;
} CachedConnKey;

typedef struct CachedConnEntry
{
	char key[NAMEDATALEN];
	PGconn	   *conn;
} CachedConnEntry;

bool hasContextQuery;

/* GUC variables */
static bool ddl_repl_enabled;        /* whether replicate ddl commands across cluster */
static bool distributed_transaction;
static bool commit_on_error;
static bool simulate_sharding;
static bool only_repl_users;


PGDLLEXPORT void _PG_init(void);
PGDLLEXPORT void _PG_fini(void);
static void InitializeHashedConnections();
void ProcessQueryLogic(const char *queryString);

/* Hooks for DCL commands */
static ProcessUtility_hook_type next_ProcessUtility_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

static void ddl_repl_ProcessUtility_hook(Node *parsetree, const char *queryString, ProcessUtilityContext context,
	ParamListInfo params, DestReceiver *dest, char *completionTag);

/* Connection cache */
static HTAB *Connections = NULL;

const DEF_DEBUG_NOTICE = NOTICE;

/*
* Module load callback
*/
void _PG_init(void)
{
	/* define GUC variables */
	DefineCustomBoolVariable("ddl_repl.enabled",
		"Selects whether ddl replication across cluster is enabled.",
		NULL,
		&ddl_repl_enabled,
		true,
		PGC_SUSET,
		0,
#if PG_VERSION_NUM >= 90100
		NULL,
#endif
		NULL,
		NULL);

	/*DefineCustomBoolVariable("ddl_repl.distributed_transaction",
		"Selects whether ddl replication uses two pase commit algorithm.",
		NULL,
		&distributed_transaction,
		true,
		PGC_SUSET,
		0,
#if PG_VERSION_NUM >= 90100
		NULL,
#endif
		NULL,
		NULL);*/

	/*DefineCustomBoolVariable("ddl_repl.commit_on_error",
		"Commit DDL changes even when there was an exception on one of the node",
		NULL,
		&commit_on_error,
		false,
		PGC_SUSET,
		0,
#if PG_VERSION_NUM >= 90100
		NULL,
#endif
		NULL,
		NULL);*/

	/*DefineCustomBoolVariable("ddl_repl.simulate_sharding",
		"When a table is inherited CREATE TABLE statements on master node generates CREATE FOREIGN TABLE on subscriber. " 
		"This requires postgres_fdw. Foreign server definition will be crated by node name if not found on the target.",
		NULL,
		&simulate_sharding,
		true,
		PGC_SUSET,
		0,
#if PG_VERSION_NUM >= 90100
		NULL,
#endif
		NULL,
		NULL);*/

	DefineCustomBoolVariable("ddl_repl.only_repl_users",
		"Selects whether ddl replication is enabled only for user with granted ddl_repl role.",
		NULL,
		&only_repl_users,
		false,
		PGC_SUSET,
		0,
#if PG_VERSION_NUM >= 90100
		NULL,
#endif
		NULL,
		NULL);

	EmitWarningsOnPlaceholders("ddl_repl");

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = ddl_repl_ProcessUtility_hook;
}

static void RemoveHashedConnection(char *node_name)
{
	CachedConnEntry *entry;
	char    	*key;
	bool	     	found;

        key = pstrdup(node_name);
        truncate_identifier(key, strlen(key), false);

	entry = hash_search(Connections, (void *) key, HASH_REMOVE, &found);

	if (found)
		entry->conn = NULL;
}


/*
* When using ddl_repl in transaction block things get more complicated. We need 
* to save established connection to any of the node to commit it later. simple
* hash structure can be used to achieve this goal, thanks to PostgreSQL developers
* we do not need to code it separetly, just use build-in functions.
*/
static void InitializeHashedConnections()
{
	if (Connections == NULL)
	{
		/* hash table creation */
		HASHCTL		ctl;
		MemSet(&ctl, 0, sizeof(ctl));

		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(CachedConnEntry);

		/* this will allocate hashed connection list in PostgreSQL cache context */
		//ctl.hcxt = CacheMemoryContext;
		Connections = hash_create("ddl_repl connections", 8, &ctl, HASH_ELEM);		
	}
}

/*
* Searches saved connections by node id. Returns null if there is no match.
*/
PGconn * GetConnection(const char *node_name)
{
	CachedConnEntry *entry;
	HASH_SEQ_STATUS status;
	char   *key;

	key = pstrdup(node_name);
	truncate_identifier(key, strlen(key), false);

	int num_categories = hash_get_num_entries(Connections);	

	if (num_categories > 0 && Connections)
	{		
 		hash_seq_init(&status, Connections);
	}

	entry = (CachedConnEntry*) hash_search(Connections, key, HASH_FIND, NULL);
	if (entry) { 		
		return entry->conn;
	} else { 		
		return NULL;	
    }
}

/*
* Creates connections by given dsn. It automatically adds it to hashed connections 
* for later usage. This method does not validate status of created connection
* it should be done manually in the calling the code block
*/
PGconn * CreateConnection(const char *dsn)
{
	/* connect to node using libpq */
	PGconn *conn = PQconnectdb(dsn);	

	/* connection to node fails - do not proceed when we need cluster consistency */
	return conn;
}


PGconn * CreateAndSaveConnection(const char *node_name, const char *dsn)
{
	PGconn *conn;
	CachedConnEntry * entry;
	char			* key;
	bool	     	found;

	key = pstrdup(node_name);
	truncate_identifier(key, strlen(key), true);	

	entry = hash_search(Connections, key, HASH_ENTER, &found);

	if (found) 
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("duplicate connection name"))); 

	conn = CreateConnection(dsn);
	entry->conn = conn;   
	strlcpy(entry->key,node_name,sizeof(entry->key));     

	return conn;
}

static void ddl_repl_ProcessUtility_hook(Node *parsetree,
	const char *queryString,
	ProcessUtilityContext context,
	ParamListInfo params,
	DestReceiver *dest,
	char *completionTag)
{	
    
	if  (
	      ddl_repl_enabled &&
			!(IsA(parsetree, VariableSetStmt) ||
			IsA(parsetree, ExecuteStmt) ||
			IsA(parsetree, PrepareStmt) ||
			IsA(parsetree, DeallocateStmt) ||
			IsA(parsetree, VariableShowStmt) ||
			strcmp(queryString, "BEGIN") == 0)				
	    )
	{   
	
       						
		/* main extension logic */
		PG_TRY();
		{  	
			InitializeHashedConnections();		
			
			if (context == PROCESS_UTILITY_QUERY)
				OpenConnections();
		
		
			if (prev_ProcessUtility_hook)
				prev_ProcessUtility_hook (parsetree, queryString, context, params,
					dest, completionTag);
			else
				standard_ProcessUtility(parsetree, queryString, context, params,
					dest, completionTag);						
						
			if (context == PROCESS_UTILITY_QUERY)
		 	   hasContextQuery = true;
			
			if ((!hasContextQuery && context == PROCESS_UTILITY_TOPLEVEL)  ||  context == PROCESS_UTILITY_QUERY) {
				ProcessQueryLogic(queryString);
			}
				
			if (context == PROCESS_UTILITY_TOPLEVEL) {			   
				CloseConnections();				 
				hasContextQuery = false;
			}
		}
		PG_CATCH();
		{
			Rollback();
			PG_RE_THROW();
		}
		PG_END_TRY();					
	} else 
	    if (prev_ProcessUtility_hook)
			prev_ProcessUtility_hook (parsetree, queryString, context, params,
				dest, completionTag);
		else
			standard_ProcessUtility(parsetree, queryString, context, params,
				dest, completionTag);	
	
}

/* TODO: DRY! */
void Rollback()
{
	HASH_SEQ_STATUS status;
	CachedConnEntry *entry;
	
	// LWLockAcquire???
	
	hash_seq_init(&status, Connections);
	while ((entry = hash_seq_search(&status)) != NULL)
	{		
		if (entry) {			
			PGresult *res = PQexec(entry->conn, "ROLLBACK");			
			if (PQresultStatus(res) != PGRES_COMMAND_OK) 
			{				
				PQfinish(entry->conn);
				RemoveHashedConnection(entry->key);					
				elog(ERROR, "ddl_repl: Node %s query failed %s", entry->key, PQerrorMessage(entry->conn));					
			} else
			  elog(NOTICE, "ddl_repl: rollback send to %s", entry->key, PQerrorMessage(entry->conn));		
			  
			PQfinish(entry->conn);
			RemoveHashedConnection(entry->key);
		}
	}		
}

void CloseConnections() 
{
	HASH_SEQ_STATUS status;
	CachedConnEntry *entry;
	
	// LWLockAcquire???
	
	hash_seq_init(&status, Connections);
	while ((entry = hash_seq_search(&status)) != NULL)
	{		
		if (entry) {			
			PGresult *res = PQexec(entry->conn, "COMMIT");			
			if (PQresultStatus(res) != PGRES_COMMAND_OK) 
			{				
				PQfinish(entry->conn);
				RemoveHashedConnection(entry->key);					
				elog(ERROR, "ddl_repl: Node %s query failed %s", entry->key, PQerrorMessage(entry->conn));					
			} else
			  elog(NOTICE, "ddl_repl: commit send to %s", entry->key, PQerrorMessage(entry->conn));		
			  
			PQfinish(entry->conn);
			RemoveHashedConnection(entry->key);
		}
	}	
}

void OpenConnections() 
{
/* connect to the database using Server Programming Interface */
	if (SPI_connect() == SPI_OK_CONNECT)
	{
		/* select available cluster nodes */
		int spi_result = SPI_execute("SELECT node_name, dsn FROM ddl_repl.nodes WHERE active IS TRUE", false, 0);

		/* save how many records were fetched */
		uint64 available_nodes_count = SPI_processed;

		//elog(DEF_DEBUG_NOTICE, "ddl_repl: node count to replicate command: %d", SPI_processed);

		/* if any rows where fetched process cluster ddl replication */
		if (available_nodes_count > 0 && SPI_tuptable != NULL)
		{
			/* get SPI variables */
			TupleDesc tupdesc = SPI_tuptable->tupdesc;
			SPITupleTable *tuptable = SPI_tuptable;
			uint64 j;
			
			for (j = 0; j < available_nodes_count; j++)
			{

				/* get current row heap */
				HeapTuple tuple = tuptable->vals[j];

				/* get current row values */
				char *node_name = SPI_getvalue(tuple, tupdesc, 1);
				char *node_dsn = SPI_getvalue(tuple, tupdesc, 2);

				/* connect to node using libpq */				
				PGconn *conn = GetConnection(node_name);

				if (conn == NULL) 
					conn = CreateAndSaveConnection(node_name, node_dsn);									
					
				PGresult *res = PQexec(conn, "BEGIN");
				if (PQresultStatus(res) != PGRES_COMMAND_OK)
				{
					elog(ERROR, "ddl_repl: Node %s query failed %s", node_name, PQerrorMessage(conn));					
					PQfinish(conn);
					RemoveHashedConnection(node_name);					
				} 
			}
		
		}
		else elog(DEF_DEBUG_NOTICE, "ddl_repl: there is no nodes to replicate command");

	  /* close SPI connection in the end */
		SPI_finish();
							
	}
	else elog(ERROR, "ddl_repl: could not connect using SPI");
}

void ProcessQueryLogic(const char *queryString) 
{
	
	HASH_SEQ_STATUS status;
	CachedConnEntry *entry;
	
	// LWLockAcquire???
	
	hash_seq_init(&status, Connections);		
	
	while ((entry = hash_seq_search(&status)) != NULL)
	{					
		if (entry) 
		{			
			PGconn *conn = entry->conn;
			/* TODO: add extra connection options
				keywords[n] = "fallback_application_name";
				values[n] = "postgres_fdw";
				n++;
			*/		
			/* connection to node fails - do not proceed when we need cluster consistency */
			if (PQstatus(conn) != CONNECTION_OK)
			{
				elog(ERROR, "ddl_repl: Node %s Connection to database failed: %s", entry->key, PQerrorMessage(conn));
				PQfinish(conn);
			}				

			/* push query to node */		
			PGresult *res = PQexec(conn, queryString);				

			if (PQresultStatus(res) != PGRES_COMMAND_OK) 
			{
				elog(ERROR, "ddl_repl: Node %s query failed %s", entry->key, PQerrorMessage(conn));					
				PQfinish(conn);
				RemoveHashedConnection(entry->key);		
			} else
				elog(DEF_DEBUG_NOTICE, "ddl_repl: query replicated to node %s", entry->key);					                                                     
			PQclear(res);			
		}
	}	
}

/*
* Module unload callback
*/
void _PG_fini(void)
{
	/* Uninstall hooks */	
	ProcessUtility_hook = prev_ProcessUtility_hook;
}
