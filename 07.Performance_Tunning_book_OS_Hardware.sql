--07. OS and Hardware interaction
SELECT * FROM sys.dm_os_wait_stats;
GO

--Reset the wait stattistics
DBCC SQLPERF('sys.dm_os_wait_stats', CLEAR);
GO

SELECT TOP 10
		wait_type ,
		waiting_tasks_count ,
		wait_time_ms / 1000.0 AS wait_time_sec ,
		CASE WHEN waiting_tasks_count = 0 THEN NULL
			ELSE wait_time_ms / 1000.0 / waiting_tasks_count
		END AS avg_wait_time_sec ,
		max_wait_time_ms / 1000.0 AS max_wait_time_sec ,
		( wait_time_ms - signal_wait_time_ms ) / 1000.0 AS resource_wait_time_sec
FROM sys.dm_os_wait_stats
WHERE wait_type NOT IN --tasks that are actually good or expected
		--to be waited on
		( 'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
		'SLEEP_SYSTEMTASK', 'WAITFOR' )
ORDER BY waiting_tasks_count DESC;
GO

--Report on top resource waits
WITH Waits
		AS ( SELECT wait_type ,
				wait_time_ms / 1000. AS wait_time_sec ,
				100. * wait_time_ms / SUM(wait_time_ms) OVER ( ) AS pct ,
				ROW_NUMBER() OVER ( ORDER BY wait_time_ms DESC ) AS rn
			FROM sys.dm_os_wait_stats
			WHERE wait_type NOT IN ( 'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP',
					'RESOURCE_QUEUE', 'SLEEP_TASK',
					'SLEEP_SYSTEMTASK',
					'SQLTRACE_BUFFER_FLUSH', 'WAITFOR',
					'LOGMGR_QUEUE', 'CHECKPOINT_QUEUE' )
			)
SELECT wait_type ,
CAST(wait_time_sec AS DECIMAL(12, 2)) AS wait_time_sec ,
CAST(pct AS DECIMAL(12, 2)) AS wait_time_percentage
FROM Waits
WHERE pct > 1
ORDER BY wait_time_sec DESC;
GO

--Investigating locking waits
SELECT wait_type,
		waiting_tasks_count,
		wait_time_ms,
		max_wait_time_ms
FROM sys.dm_os_wait_stats
WHERE wait_type LIKE 'LCK%' AND waiting_tasks_count > 0
ORDER BY waiting_tasks_count DESC;
GO

--Investigating CPU pressure
--Total waits are wait_time_ms(high signal waits indicates CPU pressure)
--if you record total signal waits above roughly 10–15%, this is a
--pretty good indicator of CPU pressure
SELECT CAST(100.0 * SUM(signal_wait_time_ms) / SUM(wait_time_ms) AS NUMERIC(20,2)) AS [%signal (cpu) waits],
		CAST(100.0 * SUM(wait_time_ms - signal_wait_time_ms) / SUM(wait_time_ms) AS NUMERIC(20,2)) AS [%resource waits]
FROM sys.dm_os_wait_stats;
GO

--SQL Server Performance Counters
SELECT * FROM sys.dm_os_performance_counters;
GO

SELECT DISTINCT
	cntr_type
FROM sys.dm_os_performance_counters
ORDER BY cntr_type;
GO

--Directly usable counter types
DECLARE @PERF_COUNTER_LARGE_RAWCOUNT INT
SELECT @PERF_COUNTER_LARGE_RAWCOUNT = 65792

SELECT object_name,
		counter_name,
		instance_name,
		cntr_value
FROM sys.dm_os_performance_counters
WHERE cntr_type = @PERF_COUNTER_LARGE_RAWCOUNT
ORDER BY object_name, counter_name, instance_name;
GO

--Monitoring shrinkage and growth of the transaction log
--Monitoring changes in the size of the transaction log
DECLARE @object_name SYSNAME
SET @object_name = CASE WHEN @@servicename = 'MSSQLSERVER' THEN 'SQLServer'
						ELSE 'MSSQL$' + @@serviceName
					END + ':Database'

DECLARE @PERF_COUNTER_LARGE_ROWCOUNT INT
SELECT @PERF_COUNTER_LARGE_ROWCOUNT = 65792

SELECT object_name,
		counter_name,
		instance_name,
		cntr_value
FROM sys.dm_os_performance_counters
WHERE cntr_type = @PERF_COUNTER_LARGE_ROWCOUNT
	AND object_name = @object_name AND counter_name IN ('Log Growths', 'Log Shrinks')
	AND cntr_value > 0
ORDER BY object_name, counter_name, instance_name;
GO

--Deprecated feature use
DECLARE @object_name SYSNAME
SET @object_name = CASE WHEN @@servicename = 'MSSQLSERVER' THEN 'SQLServer'
						ELSE 'MSSQL$' + @@serviceName
					END + ':Database'

DECLARE @PERF_COUNTER_LARGE_ROWCOUNT INT
SELECT @PERF_COUNTER_LARGE_ROWCOUNT = 65792

SELECT object_name,
		counter_name,
		instance_name,
		cntr_value
FROM sys.dm_os_performance_counters
WHERE cntr_type = @PERF_COUNTER_LARGE_ROWCOUNT
	AND object_name = @object_name 
	AND cntr_value > 0;
GO

--Ratios
--Returning the values of ratio PerfMon counters
DECLARE @PERF_LARGE_RAW_FRACTION INT ,
	@PERF_LARGE_RAW_BASE INT
SELECT @PERF_LARGE_RAW_FRACTION = 537003264 ,
	@PERF_LARGE_RAW_BASE = 1073939712

SELECT dopc_fraction.object_name ,
		dopc_fraction.instance_name ,
		dopc_fraction.counter_name ,
		--when divisor is 0, return I return NULL to indicate
		--divide by 0/no values captured
		CAST(dopc_fraction.cntr_value AS FLOAT)
		/ CAST(CASE dopc_base.cntr_value
			WHEN 0 THEN NULL
			ELSE dopc_base.cntr_value
		END AS FLOAT) AS cntr_value
FROM sys.dm_os_performance_counters AS dopc_base
		JOIN sys.dm_os_performance_counters AS dopc_fraction
			ON dopc_base.cntr_type = @PERF_LARGE_RAW_BASE
			AND dopc_fraction.cntr_type = @PERF_LARGE_RAW_FRACTION
			AND dopc_base.object_name = dopc_fraction.object_name
			AND dopc_base.instance_name = dopc_fraction.instance_name
			AND ( REPLACE(dopc_base.counter_name,
			'base', '') = dopc_fraction.counter_name
			--Worktables From Cache has "odd" name where
			--Ratio was left off
			OR REPLACE(dopc_base.counter_name,
			'base', '') = ( REPLACE(dopc_fraction.counter_name,
			'ratio', '') )
			)
ORDER BY dopc_fraction.object_name ,dopc_fraction.instance_name ,dopc_fraction.counter_name;
GO

--Returning the current value for the buffer cache hit ratio
DECLARE @object_name SYSNAME
SET @object_name = CASE WHEN @@servicename = 'MSSQLSERVER' THEN 'SQLServer'
						ELSE 'MSSQL$' + @@serviceName
						END + ':Buffer Manager'

DECLARE @PERF_LARGE_RAW_FRACTION INT ,
	@PERF_LARGE_RAW_BASE INT
SELECT @PERF_LARGE_RAW_FRACTION = 537003264 ,
	@PERF_LARGE_RAW_BASE = 1073939712


SELECT dopc_fraction.object_name ,
		dopc_fraction.instance_name ,
		dopc_fraction.counter_name ,
		--when divisor is 0, return I return NULL to indicate
		--divide by 0/no values captured
		CAST(dopc_fraction.cntr_value AS FLOAT)
		/ CAST(CASE dopc_base.cntr_value
		WHEN 0 THEN NULL
		ELSE dopc_base.cntr_value
		END AS FLOAT) AS cntr_value
FROM sys.dm_os_performance_counters AS dopc_base
	JOIN sys.dm_os_performance_counters AS dopc_fraction
		ON dopc_base.cntr_type = @PERF_LARGE_RAW_BASE
		AND dopc_fraction.cntr_type = @PERF_LARGE_RAW_FRACTION
		AND dopc_base.object_name = dopc_fraction.object_name
		AND dopc_base.instance_name = dopc_fraction.instance_name
		AND ( REPLACE(dopc_base.counter_name,
		'base', '') = dopc_fraction.counter_name
		--Worktables From Cache has "odd" name where
		--Ratio was left off
		OR REPLACE(dopc_base.counter_name,
		'base', '') = ( REPLACE(dopc_fraction.counter_name,
		'ratio', '') )
		)
WHERE dopc_fraction.object_name = @object_name
	AND dopc_fraction.instance_name = ''
	AND dopc_fraction.counter_name = 'Buffer cache hit ratio'
ORDER BY dopc_fraction.object_name ,dopc_fraction.instance_name ,dopc_fraction.counter_name;
GO

--Per second averages
DECLARE @PERF_COUNTER_BULK_COUNT INT
SELECT @PERF_COUNTER_BULK_COUNT = 272696576

--Holds initial state
DECLARE @baseline TABLE
	(
		object_name NVARCHAR(256) ,
		counter_name NVARCHAR(256) ,
		instance_name NVARCHAR(256) ,
		cntr_value BIGINT ,
		cntr_type INT ,
		time DATETIME DEFAULT ( GETDATE() )
	)

DECLARE @current TABLE
	(
		object_name NVARCHAR(256) ,
		counter_name NVARCHAR(256) ,
		instance_name NVARCHAR(256) ,
		cntr_value BIGINT ,
		cntr_type INT ,
		time DATETIME DEFAULT ( GETDATE() )
	)

--capture the initial state of bulk counters
INSERT INTO @baseline
	( object_name ,
		counter_name ,
		instance_name ,
		cntr_value ,
		cntr_type
	)
	SELECT object_name ,
		counter_name ,
		instance_name ,
		cntr_value ,
		cntr_type
		FROM sys.dm_os_performance_counters AS dopc
		WHERE cntr_type = @PERF_COUNTER_BULK_COUNT

WAITFOR DELAY '00:00:05' --the code will work regardless of delay chosen

--get the followon state of the counters
INSERT INTO @current
	( object_name ,
		counter_name ,
		instance_name ,
		cntr_value ,
		cntr_type
	)
	SELECT object_name ,
		counter_name ,
		instance_name ,
		cntr_value ,
		cntr_type
FROM sys.dm_os_performance_counters AS dopc
WHERE cntr_type = @PERF_COUNTER_BULK_COUNT

SELECT dopc.object_name ,
		dopc.instance_name ,
		dopc.counter_name ,
		--ms to second conversion factor
		1000 *
		--current value less the previous value
		( ( dopc.cntr_value - prev_dopc.cntr_value )
		--divided by the number of milliseconds that pass
		--casted as float to get fractional results. Float
		--lets really big or really small numbers to work
		/ CAST(DATEDIFF(ms, prev_dopc.time, dopc.time) AS FLOAT) )
		AS cntr_value
		--simply join on the names of the counters
FROM @current AS dopc
	JOIN @baseline AS prev_dopc 
		ON prev_dopc.object_name = dopc.object_name AND prev_dopc.instance_name = dopc.instance_name
		AND prev_dopc.counter_name = dopc.counter_name
WHERE dopc.cntr_type = @PERF_COUNTER_BULK_COUNT
	AND 1000 * ( ( dopc.cntr_value - prev_dopc.cntr_value )
	/ CAST(DATEDIFF(ms, prev_dopc.time, dopc.time) AS FLOAT) )
	/* default to only showing non-zero values */ <> 0
ORDER BY dopc.object_name ,dopc.instance_name ,dopc.counter_name;
GO

--Average number of operations
--Returning the values for the 'average number ofoperations' PerfMon counters.
DECLARE @PERF_AVERAGE_BULK INT ,
	@PERF_LARGE_RAW_BASE INT

SELECT @PERF_AVERAGE_BULK = 1073874176 ,
	@PERF_LARGE_RAW_BASE = 1073939712

SELECT dopc_avgBulk.object_name ,
		dopc_avgBulk.instance_name ,
		dopc_avgBulk.counter_name ,
		CAST(dopc_avgBulk.cntr_value AS FLOAT)
		--when divisor is 0, return NULL to indicate
		--divide by 0
		/ CAST(CASE dopc_base.cntr_value
		WHEN 0 THEN NULL
		ELSE dopc_base.cntr_value
		END AS FLOAT) AS cntr_value
FROM sys.dm_os_performance_counters dopc_base
	JOIN sys.dm_os_performance_counters dopc_avgBulk
		ON dopc_base.cntr_type = @PERF_LARGE_RAW_BASE
		AND dopc_avgBulk.cntr_type = @PERF_AVERAGE_BULK
		AND dopc_base.object_name = dopc_avgBulk.object_name
		AND dopc_base.instance_name = dopc_avgBulk.instance_name
		--Average Wait Time has (ms) in name,
		--so it has handled "special"
		AND ( REPLACE(dopc_base.counter_name,
		'base', '') = dopc_avgBulk.counter_name
		OR REPLACE(dopc_base.counter_name,
		'base', '') = REPLACE(dopc_avgBulk.counter_name,
		'(ms)', '')
		)
ORDER BY dopc_avgBulk.object_name ,dopc_avgBulk.instance_name ,dopc_avgBulk.counter_name;
GO

--Monitoring Machine Characteristics
SELECT * FROM sys.dm_os_sys_info;
GO

SELECT COUNT(*) FROM sys.dm_os_schedulers;
GO

SELECT COUNT(*) FROM sys.dm_os_schedulers
WHERE status = 'VISIBLE ONLINE';
GO

--CPU configuration details
SELECT cpu_count AS [Logical CPU Count],
		hyperthread_ratio,
		cpu_count / hyperthread_ratio AS [Physical CPU Count],
		physical_memory_kb / 1048576 AS [Physical Memory (KB)],
		sqlserver_start_time
FROM sys.dm_os_sys_info;
GO

--Investigating CPU usage
SELECT * FROM sys.dm_os_schedulers;
GO

--Investigating scheduler activity
--Get AVG task count and AVG runnable task count
SELECT AVG(current_tasks_count) AS [AVG Task Count],
	AVG(runnable_tasks_count) AS [AVG Runnable task count]
FROM sys.dm_os_schedulers
WHERE scheduler_id < 255 AND status = 'VISIBLE ONLINE';
GO

--Investigating potential disk I/O or CPU pressure
SELECT scheduler_id,
		cpu_id,
		status,
		is_online,
		is_idle,
		current_tasks_count,
		runnable_tasks_count,
		current_workers_count,
		active_workers_count,
		work_queue_count,
		pending_disk_io_count,
		load_factor
FROM sys.dm_os_schedulers
WHERE scheduler_id < 255 
	--AND runnable_tasks_count > 0
	AND pending_disk_io_count > 0
GO

--Insufficient threads
--Are there sufficient worker threads for the workload?
SELECT AVG(work_queue_count)
FROM sys.dm_os_schedulers
WHERE status = 'VISIBLE ONLINE';
GO

--Context switching
SELECT scheduler_id ,
		preemptive_switches_count ,
		context_switches_count ,
		idle_switches_count ,
		failed_to_create_worker
FROM sys.dm_os_schedulers
WHERE scheduler_id < 255;
GO

--Is NUMA enabled?
SELECT CASE COUNT(DISTINCT parent_node_id)
		WHEN 1 THEN 'NUMA disabled'
		ELSE 'NUMA enabled'
		END
FROM sys.dm_os_schedulers
WHERE parent_node_id <> 32;
GO

--Investigating Memory Usage
SELECT * FROM sys.dm_exec_query_memory_grants;
GO

/*
In the Buffer Manager set of counters, the Page Life Expectancy counter is
especially useful for detecting memory pressure. This value should generally be 300 or
greater, indicating that pages stay in RAM for an average of 300 seconds, or 5 minutes. If
it is significantly lower for sustained periods, it indicates that SQL Server is being forced
to flush the cache to free up memory.
*/

--System-wide memory use
SELECT * FROM sys.dm_os_sys_memory;
GO

--System memory usage
SELECT total_physical_memory_kb / 1024 AS total_physical_memory_mb,
		available_physical_memory_kb / 1024 AS available_physical_memory_mb,
		total_page_file_kb / 1024 AS total_page_file_mb,
		available_page_file_kb / 1024 AS available_page_file_mb ,
		system_memory_state_desc
FROM sys.dm_os_sys_memory;
GO

--Process memory use
--Memory usage by the SQL Server process
SELECT physical_memory_in_use_kb,
		virtual_address_space_committed_kb,
		virtual_address_space_available_kb,
		page_fault_count,
		process_physical_memory_low,
		process_virtual_memory_low
FROM sys.dm_os_process_memory;
GO

--Memory use in the buffer pool
SELECT * FROM sys.dm_os_buffer_descriptors;
GO

--Memory allocation in the buffer pool
-- Get total buffer usage by database
SELECT DB_NAME(database_id) AS [Database Name] ,
		COUNT(*) * 8 / 1024.0 AS [Cached Size (MB)]
FROM sys.dm_os_buffer_descriptors
WHERE database_id > 4 -- exclude system databases
		AND database_id <> 32767 -- exclude ResourceDB
GROUP BY DB_NAME(database_id)
ORDER BY [Cached Size (MB)] DESC ;
GO

-- Breaks down buffers by object (table, index) in the buffer pool
SELECT OBJECT_NAME(p.[object_id]) AS [ObjectName] ,
		p.index_id ,
		COUNT(*) / 128 AS [Buffer size(MB)] ,
		COUNT(*) AS [Buffer_count]
FROM sys.allocation_units AS a
	INNER JOIN sys.dm_os_buffer_descriptors AS b 
		ON a.allocation_unit_id = b.allocation_unit_id
	INNER JOIN sys.partitions AS p ON a.container_id = p.hobt_id
WHERE b.database_id = DB_ID() AND p.[object_id] > 100 -- exclude system objects
GROUP BY p.[object_id] ,p.index_id
ORDER BY buffer_count DESC ;
GO

--Memory clerks and memory grants
--Buffer pool usage for instance
SELECT TOP (20) [type], SUM(pages_kb) AS [SPA Mem, kb]
FROM sys.dm_os_memory_clerks
GROUP BY [type]
ORDER BY SUM(pages_kb) DESC;
GO

SELECT * FROM sys.dm_exec_query_memory_grants;
GO

-- Shows the memory required by both running (non-null grant_time)
-- and waiting queries (null grant_time)
-- SQL Server 2008 version
SELECT DB_NAME(st.dbid) AS [DatabaseName] ,
		mg.requested_memory_kb ,
		mg.ideal_memory_kb ,
		mg.request_time ,
		mg.grant_time ,
		mg.query_cost ,
		mg.dop ,
		st.[text]
FROM sys.dm_exec_query_memory_grants AS mg
CROSS APPLY sys.dm_exec_sql_text(plan_handle) AS st
WHERE mg.request_time < COALESCE(grant_time, '99991231')
ORDER BY mg.requested_memory_kb DESC ;
GO

--Investigate memory using cache counters
--Returning the cache counters
SELECT type ,
		name ,
		pages_kb ,
		pages_in_use_kb ,
		entries_count ,
		entries_in_use_count
FROM sys.dm_os_memory_cache_counters
ORDER BY type,name;
GO

--Investigating the use of the plan cache
SELECT name ,
		type ,
		entries_count ,
		entries_in_use_count
FROM sys.dm_os_memory_cache_counters
WHERE type IN ( 'CACHESTORE_SQLCP', 'CACHESTORE_OBJCP' )
--ad hoc plans and object plans
ORDER BY name ,type;
GO

--Investigating Latching
SELECT * FROM sys.dm_os_latch_stats;
GO

--Reset the latch statistics
DBCC SQLPERF ('sys.dm_os_latch_stats', CLEAR);
GO

SELECT latch_class ,
		waiting_requests_count AS waitCount ,
		wait_time_ms AS waitTime ,
		max_wait_time_ms AS maxWait
FROM sys.dm_os_latch_stats
ORDER BY wait_time_ms DESC;
GO
