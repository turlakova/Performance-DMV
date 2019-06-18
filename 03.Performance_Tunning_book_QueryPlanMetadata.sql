--Chapter 3 Query Plan Metadata
SELECT * FROM sys.dm_exec_query_stats;
GO

SELECT * FROM sys.dm_exec_procedure_stats;
GO

--Flushing the cache?
--Clear the cache for all databases
DBCC FREEPROCCACHE

--Clear cache for a single database
DBCC FLUSHPROCINDB('db_name');
GO

--Viewing the Text of cached queries and query plans
CREATE PROCEDURE ShowQueryText
AS
	SELECT TOP 10
	object_id ,
	name
	FROM sys.objects ;
	--waitfor delay '00:00:00'
	SELECT TOP 10
	object_id ,
	name
	FROM sys.objects ;
	SELECT TOP 10
	object_id ,
	name
	FROM sys.procedures ;
	GO

EXEC dbo.ShowQueryText ;
GO

SELECT deqp.dbid ,
	deqp.objectid ,
	deqp.encrypted ,
	deqp.query_plan
FROM sys.dm_exec_query_stats deqs
CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
WHERE objectid = OBJECT_ID('ShowQueryText', 'p');
GO

---Dissecting the SQl text
SELECT deqs.plan_handle,
	deqs.sql_handle,
	execText.text
FROM sys.dm_exec_query_stats deqs
	CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) AS execText
WHERE execText.text LIKE 'CREATE PROCEDURE ShowQueryText%';
GO

--Extracting the SQL text for individual queries in a batch
SELECT CHAR(13) + CHAR(10)
		+ CASE WHEN deqs.statement_start_offset = 0
		AND deqs.statement_end_offset = -1
		THEN '-- see objectText column--'
		ELSE '-- query --' + CHAR(13) + CHAR(10)
			+ SUBSTRING(execText.text, deqs.statement_start_offset / 2,
				( ( CASE WHEN deqs.statement_end_offset = -1
				THEN DATALENGTH(execText.text)
				ELSE deqs.statement_end_offset
				END ) - deqs.statement_start_offset ) / 2)
		END AS queryText ,
		deqp.query_plan
FROM sys.dm_exec_query_stats deqs
	CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) AS exectext
	CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) deqp
WHERE exectext.text LIKE 'CREATE PROCEDURE ShowQueryText%';
GO

--Returning the plan using sys.dm_exec_text_query_plan
SELECT deqp.dbid,
		deqp.objectid,
		CAST(detqp.query_plan AS XML) AS singleStatementPlan,
		deqp.query_plan AS batch_query_plan,
		--this won't actually work in all cases because nominal plans aren't
		--cached, so you won't see a plan for waitfor if you uncoment it
		ROW_NUMBER() OVER(ORDER BY Statement_start_offset) AS query_position,
		CASE WHEN deqs.statement_start_offset = 0
			AND deqs.statement_end_offset = -1
		THEN '-- see objectText column ---'
		ELSE '-- query --' + CHAR(13) + CHAR(10)
			+ SUBSTRING(execText.text, deqs.statement_start_offset / 2,
			((CASE WHEN deqs.statement_end_offset = -1
				THEN DATALENGTH(execText.text)
				ELSE deqs.statement_end_offset
				END) - deqs.statement_start_offset) / 2)
		END AS queryText
FROM sys.dm_exec_query_stats deqs
	CROSS APPLY sys.dm_exec_text_query_plan(deqs.plan_handle, deqs.statement_start_offset, deqs.statement_end_offset) AS detqp
	CROSS APPLY sys.dm_exec_query_plan(deqs.plan_handle) AS deqp
	CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) AS execText
WHERE deqp.objectid = OBJECT_ID('ShowQueryText', 'p');
GO

--Cached Query Plan Statistics
--Retrieving the plans for compiled objects
SELECT refcounts,
		usecounts,
		size_in_bytes,
		cacheobjtype,
		objtype
FROM sys.dm_exec_cached_plans
WHERE objtype IN('proc', 'prepared');
GO

--Investigating plan reuse
--Total number of cached plans
SELECT COUNT(*)
FROM sys.dm_exec_cached_plans;
GO

--An overview of plan reuse
SELECT MAX(CASE WHEN usecounts BETWEEN 10 AND 100 THEN '10-100'
			WHEN usecounts BETWEEN 101 AND 1000 THEN '101-1000'
			WHEN usecounts BETWEEN 1001 AND 5000 THEN '1001-5000'
			WHEN usecounts BETWEEN 5001 AND 10000 THEN '5001-10000'
			ELSE CAST(usecounts AS VARCHAR(100))
		END) AS usecounts ,
		COUNT(*) AS countInstance
FROM sys.dm_exec_cached_plans
GROUP BY CASE WHEN usecounts BETWEEN 10 AND 100 THEN 50
			WHEN usecounts BETWEEN 101 AND 1000 THEN 500
			WHEN usecounts BETWEEN 1001 AND 5000 THEN 2500
			WHEN usecounts BETWEEN 5001 AND 10000 THEN 7500
			ELSE usecounts
		END
ORDER BY CASE WHEN usecounts BETWEEN 10 AND 100 THEN 50
			WHEN usecounts BETWEEN 101 AND 1000 THEN 500
			WHEN usecounts BETWEEN 1001 AND 5000 THEN 2500
			WHEN usecounts BETWEEN 5001 AND 10000 THEN 7500
			ELSE usecounts
		END DESC ;
GO

--Examining frequently used plans
--query the most reused plans
SELECT TOP 2 WITH TIES
	decp.usecounts,
	decp.cacheobjtype,
	decp.objtype,
	deqp.query_plan,
	dest.text
FROM sys.dm_exec_cached_plans decp
	CROSS APPLY sys.dm_exec_query_plan(decp.plan_handle) AS deqp
	CROSS APPLY sys.dm_exec_sql_text(decp.plan_handle) AS dest
ORDER BY decp.usecounts DESC;
GO

--Examining plan reuse for a single procedure
SELECT usecounts ,
		cacheobjtype ,
		objtype ,
		OBJECT_NAME(dest.objectid)
FROM sys.dm_exec_cached_plans decp
CROSS APPLY sys.dm_exec_sql_text(decp.plan_handle) AS dest
WHERE dest.objectid = OBJECT_ID('<procedureName>')
AND dest.dbid = DB_ID()
ORDER BY usecounts DESC ;
GO

--Examining ad hoc single-use plans
SELECT p.[text],
	cp.size_in_bytes
FROM sys.dm_exec_cached_plans AS cp
	CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS p
WHERE cp.cacheobjtype = 'Compiled Plan'
	AND cp.objtype = 1
ORDER BY cp.size_in_bytes DESC;
GO

--Query plan attributes
--Examining plan attributes
SELECT CAST(depa.attribute AS VARCHAR(30)) AS attribute,
		CAST(depa.value AS VARCHAR(30)) AS value,
		depa.is_cache_key
FROM (SELECT TOP 1 *
		FROM sys.dm_exec_cached_plans
		ORDER BY usecounts DESC) decp
		OUTER APPLY sys.dm_exec_plan_attributes(decp.plan_handle) depa
WHERE is_cache_key = 1
ORDER BY usecounts DESC;
GO

--Gathering Query Execution Statistics
SELECT * FROM sys.dm_exec_query_stats;
GO

--Finding the CPU-intensive queries
SELECT TOP 3
		total_worker_time ,
		execution_count ,
		total_worker_time / execution_count AS [Avg CPU Time] ,
		CASE WHEN deqs.statement_start_offset = 0
				AND deqs.statement_end_offset = -1
			THEN '-- see objectText column--'
			ELSE '-- query --' + CHAR(13) + CHAR(10)
					+ SUBSTRING(execText.text, deqs.statement_start_offset / 2,
					( ( CASE WHEN deqs.statement_end_offset = -1
					THEN DATALENGTH(execText.text)
					ELSE deqs.statement_end_offset
					END ) - deqs.statement_start_offset ) / 2)
END AS queryText
FROM sys.dm_exec_query_stats deqs
CROSS APPLY sys.dm_exec_sql_text(deqs.plan_handle) AS execText
ORDER BY deqs.total_worker_time DESC ;
GO

--Grouping by sql_handle to see query stats at the batch level
SELECT TOP 100
		SUM(total_logical_reads) AS total_logical_reads ,
		COUNT(*) AS num_queries , --number of individual queries in batch
		--not all usages need be equivalent, in the case of looping
		--or branching code
		MAX(execution_count) AS execution_count ,
		MAX(execText.text) AS queryText
FROM sys.dm_exec_query_stats deqs
	CROSS APPLY sys.dm_exec_sql_text(deqs.sql_handle) AS execText
GROUP BY deqs.sql_handle
HAVING AVG(total_logical_reads / execution_count) <> SUM(total_logical_reads)/ SUM(execution_count)
ORDER BY 1 DESC;
GO

--Investigating expensive cached stored procedures
--Top cached Sps by total logical reads
--Logical reads relate to memory presure
SELECT TOP (25)
	p.name AS [SP Name],
	deps.total_logical_reads,
	deps.total_logical_reads / deps.execution_count  AS [AvgLogicalReads],
	deps.execution_count,
	ISNULL(deps.execution_count / DATEDIFF(Second, deps.cached_time, GETDATE()), 0) AS [Calls/Second],
	deps.total_elapsed_time,
	deps.total_elapsed_time / deps.execution_count AS [avg_elapsed_time],
	deps.cached_time
FROM sys.procedures AS p
	INNER JOIN sys.dm_exec_procedure_stats AS deps
		ON p.object_id = deps.object_id
WHERE deps.database_id = DB_ID()
ORDER BY deps.total_logical_reads DESC;
GO

--Physical_reads
SELECT TOP (25)
	p.name AS [SP Name],
	deps.total_physical_reads,
	deps.total_physical_reads / deps.execution_count  AS [AvgPhysicalReads],
	deps.execution_count,
	ISNULL(deps.execution_count / DATEDIFF(Second, deps.cached_time, GETDATE()), 0) AS [Calls/Second],
	deps.total_elapsed_time,
	deps.total_elapsed_time / deps.execution_count AS [avg_elapsed_time],
	deps.cached_time
FROM sys.procedures AS p
	INNER JOIN sys.dm_exec_procedure_stats AS deps
		ON p.object_id = deps.object_id
WHERE deps.database_id = DB_ID()
ORDER BY deps.total_physical_reads DESC;
GO

--Getting Aggregate Query optimization statistics for all optimizations
SELECT * FROM sys.dm_exec_query_optimizer_info
GO
