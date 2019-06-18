--05.Index
--Querying index use
SELECT DB_NAME(dd.database_id) AS [database_name],
		OBJECT_NAME(dd.object_id) AS [object_name],
		ai.name AS index_name,
		dd.last_user_seek + dd.user_scans + dd.user_lookups AS user_reads
FROM sys.dm_db_index_usage_stats dd
	INNER JOIN sys.indexes ai
		ON dd.object_id = ai.object_id
		 AND dd.index_id = ai.index_id;
GO

SELECT DB_NAME(ddius.[database_id]) AS [database_name] ,
		ddius.[database_id] ,
		ddius.[object_id] ,
		ddius.[index_id]
FROM sys.[dm_db_index_usage_stats] ddius
INNER JOIN sys.[indexes] asi
ON ddius.[object_id] = asi.[object_id]
AND ddius.[index_id] = asi.[index_id];
GO

SELECT * FROM sys.columns;
GO
SELECT * FROM sys.index_columns;
GO
SELECT * FROM sys.objects;
GO
SELECT * FROM sys.partitions;
GO
SELECT * FROM sys.sysusers;
GO

--Index Strategy
--Investigating index usage
SELECT * FROM sys.dm_db_index_usage_stats;
GO

--Usage stats for indexes that have been used to resolve a query
SELECT OBJECT_NAME(ddius.object_id, ddius.database_id) AS [object_name],
		ddius.index_id,
		ddius.user_seeks,
		ddius.user_scans,
		ddius.user_lookups,
		ddius.user_seeks + ddius.user_scans + ddius.user_lookups AS user_reads,
		ddius.user_updates AS user_writes,
		ddius.last_user_scan,
		ddius.last_user_update
FROM sys.dm_db_index_usage_stats ddius
WHERE ddius.database_id > 4 -- filter out system table
	AND OBJECTPROPERTY(ddius.object_id, 'IsUsertable') = 1
	AND ddius.index_id > 0 -- filter out heaps
ORDER BY ddius.user_scans DESC;
GO

--Identify indexes that have never been accessed
--List unused indexes
SELECT OBJECT_NAME(i.object_id) AS [Table name],
		i.name
FROM sys.indexes AS i
	INNER JOIN sys.objects AS o
		ON i.object_id = o.object_id
WHERE i.index_id NOT IN (SELECT ddius.index_id
							FROM sys.dm_db_index_usage_stats AS ddius
							WHERE ddius.object_id = i.object_id
									AND i.index_id = ddius.index_id AND database_id = DB_ID())
		AND o.type = 'U'
ORDER BY OBJECT_NAME(i.object_id) ASC;
GO

--Indentify indexes that are being maintained but not used
SELECT '[' + DB_NAME() + '].[' + su.[name] + '].[' + o.[name] + ']' AS [statement] ,
		i.[name] AS [index_name] ,
		ddius.[user_seeks] + ddius.[user_scans] + ddius.[user_lookups] AS [user_reads] ,
		ddius.[user_updates] AS [user_writes] ,
		SUM(SP.rows) AS [total_rows],
		'DROP INDEX [' + i.[name] + '] ON [' + su.[name] + '].[' + o.[name] + '] WITH ( ONLINE = OFF )' AS [drop_command]
FROM sys.dm_db_index_usage_stats ddius
	INNER JOIN sys.indexes i 
		ON ddius.object_id = i.object_id AND i.index_id = ddius.index_id
	INNER JOIN sys.partitions sp
		ON ddius.object_id = sp.object_id AND sp.index_id = ddius.index_id
	INNER JOIN sys.objects o ON ddius.object_id = o.object_id
	INNER JOIN sys.sysusers su ON o.schema_id = su.uid
WHERE ddius.database_id = DB_ID() -- current database only
	AND OBJECTPROPERTY(ddius.object_id, 'IsUserTable') = 1 AND ddius.index_id > 0
GROUP BY su.name, o.name, i.name, ddius.[user_seeks] + ddius.[user_scans] + ddius.[user_lookups], ddius.[user_updates]
HAVING ddius.[user_seeks] + ddius.[user_scans] + ddius.[user_lookups] = 0
ORDER BY ddius.user_updates DESC, su.name, o.name, i.name
GO

--How old are the index usage stats
SELECT name, DATEDIFF(DAY, sd.crdate, GETDATE()) AS days_history
FROM sys.sysdatabases sd
WHERE sd.name = 'tempdb';
GO

--Potentially inefficient non-clustered indexes(writes > reads)
SELECT OBJECT_NAME(ddius.object_id) AS [Table Name],
		i.name AS [Index name],
		i.index_id,
		ddius.user_updates AS [Total Writes],
		ddius.user_seeks + ddius.user_scans + ddius.user_lookups AS [Total Reads],
		ddius.user_updates - (ddius.user_seeks + ddius.user_scans + ddius.user_lookups) AS [Difference]
FROM sys.dm_db_index_usage_stats AS ddius WITH(NOLOCK)
	INNER JOIN sys.indexes AS i WITH (NOLOCK)
		ON ddius.object_id = i.object_id AND i.index_id = ddius.index_id
WHERE OBJECTPROPERTY(ddius.object_id, 'IsUserTable') = 1 AND ddius.database_id  = DB_ID()
	AND ddius.user_updates > (ddius.user_seeks + ddius.user_scans + ddius.user_lookups) 
	AND i.index_id > 1
ORDER BY [Difference] DESC, [Total Writes] DESC, [Total Reads] DESC;
GO

--Determine usage patterns of current indexes(index_operational_stats DMF)
--Detailed activity information for indexes not used for user reads
--Detailed write information for unused indexes
SELECT '[' + DB_NAME() + '].[' + su.name + '].[' + o.name + ']' AS statement,
		i.name AS [index_name],
		ddius.user_seeks + ddius.user_scans + ddius.user_lookups AS user_reads,
		ddius.user_updates AS user_writes,
		ddios.leaf_insert_count,
		ddios.leaf_delete_count,
		ddios.leaf_update_count,
		ddios.nonleaf_insert_count,
		ddios.nonleaf_delete_count,
		ddios.nonleaf_update_count
FROM sys.dm_db_index_usage_stats ddius
	INNER JOIN sys.indexes i ON ddius.object_id = i.object_id AND i.index_id = ddius.index_id
	INNER JOIN sys.partitions sp ON ddius.object_id = sp.object_id AND sp.index_id = ddius.index_id
	INNER JOIN sys.objects o ON ddius.object_id = o.object_id
	INNER JOIN sys.sysusers su ON o.schema_id = su.uid
	INNER JOIN sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) AS ddios
		ON ddius.index_id = ddios.index_id AND ddius.object_id = ddios.object_id
		AND sp.partition_number = ddios.partition_number AND ddius.database_id = ddios.database_id
WHERE OBJECTPROPERTY(ddius.object_id, 'IsUserTable') = 1 AND ddius.index_id > 0
		AND ddius.user_seeks + ddius.user_scans + ddius.user_lookups = 0
ORDER BY ddius.user_updates DESC, su.name, o.name, i.name;
GO

--Identify locking and blocking at the row level
SELECT '[' + DB_NAME() + '].[' + su.name + '].[' + o.name + ']' AS statement,
		i.name AS index_name,
		ddios.page_latch_wait_count,
		ddios.page_io_latch_wait_count,
		ddios.partition_number,
		ddios.row_lock_count,
		ddios.row_lock_wait_count,
		CAST(100.0 * ddios.row_lock_wait_count / ddios.row_lock_count AS DECIMAL(15, 2)) AS [%_times_blocked],
		CAST(1.0 * ddios.row_lock_wait_in_ms / ddios.row_lock_wait_count AS DECIMAL(15, 2)) AS avg_row_lock_waits_in_ms
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) ddios
	INNER JOIN sys.indexes i ON ddios.object_id = i.object_id AND i.index_id = ddios.index_id
	INNER JOIN sys.objects o ON ddios.object_id = o.object_id
	INNER JOIN sys.sysusers su ON o.schema_id = su.uid
WHERE ddios.row_lock_wait_count > 0
	AND OBJECTPROPERTY(ddios.object_id, 'IsUserTable') = 1 AND i.index_id > 0
ORDER BY ddios.row_lock_wait_count DESC;
GO

--Identify latch waits
SELECT '[' + DB_NAME() + '].[' + OBJECT_SCHEMA_NAME(ddios.object_id) + '].[' + OBJECT_NAME(ddios.object_id) + ']' AS [object_name],
		i.name AS index_name,
		ddios.page_io_latch_wait_count,
		ddios.page_io_latch_wait_in_ms,
		(ddios.page_io_latch_wait_in_ms / ddios.page_io_latch_wait_count) AS avg_page_io_latch_wait_in_ms
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) ddios
	INNER JOIN sys.indexes i 
		ON ddios.object_id = i.object_id AND i.index_id = ddios.index_id
WHERE ddios.page_io_latch_wait_count > 0
	AND OBJECTPROPERTY(i.object_id, 'IsUserTable') = 1
ORDER BY ddios.page_io_latch_wait_count DESC, avg_page_io_latch_wait_in_ms DESC;
GO

--Identify lock escalations
--This query provides information regarding how frequently these escalation attempts were made, 
--and the percentage success in performing the escalation
SELECT OBJECT_NAME(ddios.object_id, ddios.database_id) AS [object_name],
		i.name AS index_name,
		ddios.index_id,
		ddios.partition_number,
		ddios.index_lock_promotion_attempt_count,
		ddios.index_lock_promotion_count,
		(ddios.index_lock_promotion_attempt_count / ddios.index_lock_promotion_count) AS percent_success
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) ddios
	INNER JOIN sys.indexes i
		ON ddios.object_id = i.object_id AND i.index_id = ddios.index_id
WHERE ddios.index_lock_promotion_count > 0
ORDER BY index_lock_promotion_count DESC;
GO

--Identify indexes associated with lock contention
SELECT OBJECT_NAME(ddios.object_id, ddios.database_id) AS [object_name],
		i.name AS index_name,
		ddios.index_id,
		ddios.partition_number,
		ddios.page_lock_wait_count,
		ddios.page_lock_wait_in_ms,
		CASE WHEN ddmid.database_id IS NULL THEN 'N'
			ELSE 'Y'
		END AS missing_index_identified
FROM sys.dm_db_index_operational_stats(DB_ID(), NULL, NULL, NULL) ddios
	INNER JOIN sys.indexes i
		ON ddios.object_id = i.object_id AND i.index_id = ddios.index_id
	LEFT OUTER JOIN (SELECT DISTINCT database_id,
									object_id
					FROM sys.dm_db_missing_index_details) AS ddmid
					ON ddmid.database_id = ddios.database_id AND ddmid.object_id = ddios.object_id
WHERE ddios.page_lock_wait_in_ms > 0
ORDER BY ddios.page_lock_wait_count DESC;
GO

--Find missing index
--Finding the most benefical missing indexes
SELECT dbmigs.user_seeks * dbmigs.avg_total_user_cost * (dbmigs.avg_user_impact * 0.01) AS index_advantage,
		dbmigs.last_user_seek,
		dbmid.statement AS [Database.Schema.Table],
		dbmid.equality_columns,
		dbmid.inequality_columns,
		dbmid.included_columns,
		dbmigs.unique_compiles,
		dbmigs.user_seeks,
		dbmigs.avg_total_user_cost,
		dbmigs.avg_user_impact
FROM sys.dm_db_missing_index_group_stats AS dbmigs WITH(NOLOCK)
	INNER JOIN sys.dm_db_missing_index_groups AS dbmig WITH(NOLOCK)
		ON dbmigs.group_handle = dbmig.index_group_handle
	INNER JOIN sys.dm_db_missing_index_details AS dbmid WITH(NOLOCK)
		ON dbmig.index_handle = dbmid.index_handle
WHERE dbmid.database_id = DB_ID()
ORDER BY index_advantage DESC;
GO

--Index Maintenance(sys.dm_db_index_physical_stats - DMF) - identify index fragmentation
--Detecting and fixing fragmentation
SELECT '[' + DB_NAME() + '].[' + OBJECT_SCHEMA_NAME(ddips.object_id, DB_ID()) + '].['
		+ OBJECT_NAME(ddips.object_id, DB_ID()) + ']' AS [statement],
		i.name,
		ddips.index_type_desc,
		ddips.partition_number,
		ddips.alloc_unit_type_desc,
		ddips.index_depth,
		ddips.index_level,
		ddips.avg_fragmentation_in_percent,
		ddips.avg_fragment_size_in_pages,
		--CAST(ddips.avg_fragmentation_in_percent AS SMALLINT) AS avg_fragmentation_in_percent,
		--CAST(ddips.avg_fragment_size_in_pages AS SMALLINT) AS avg_fragment_size_in_pages,
		ddips.fragment_count,
		ddips.page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') AS ddips
	INNER JOIN sys.indexes i 
		ON ddips.object_id = i.object_id AND ddips.index_id = i.index_id
WHERE ddips.avg_fragmentation_in_percent > 5
	AND ddips.page_count > 1000
ORDER BY ddips.avg_fragmentation_in_percent, OBJECT_NAME(ddips.object_id, DB_ID()), i.name;
GO


