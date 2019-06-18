--06. Physical disk statistics and utilization
SELECT * FROM sys.dm_db_partition_stats;
GO

SELECT * FROM sys.dm_io_virtual_file_stats(DB_ID(), NULL);
GO

SELECT * FROM sys.dm_io_pending_io_requests;
GO

SELECT * FROM sys.dm_db_file_space_usage;
GO

--Minimizing I/O
--Tunning the Disk I/O subsytem.
--Getting Physical statistics on your tables and indexes
SELECT * FROM sys.dm_db_partition_stats;
GO

--Size and structure
--Total number of rows in a table
SELECT OBJECT_SCHEMA_NAME(ddps.object_id) + '.' + OBJECT_NAME(ddps.object_id) AS name,
		SUM(ddps.row_count) AS row_count
FROM sys.dm_db_partition_stats AS ddps
	JOIN sys.indexes i 
		ON i.object_id = ddps.object_id AND i.index_id = ddps.index_id
WHERE i.type_desc IN ('CLUSTERED', 'HEAP')
	AND OBJECTPROPERTY(ddps.object_id, 'IsMSShipped') = 0
GROUP BY ddps.object_id;
GO

--Number of rows per partition
SELECT i.name,
		i.type_desc,
		dps.row_count,
		partition_id
FROM sys.dm_db_partition_stats AS dps
	JOIN sys.indexes AS i 
		ON i.object_id = dps.object_id AND i.index_id = dps.index_id
WHERE OBJECT_ID('salesOrder') = dps.object_id
GO

--Physical characteristics of each partition
SELECT OBJECT_NAME(i.object_id) AS Object_name,
		ddps.index_id,
		ddps.partition_number,
		ddps.row_count,
		ddps.used_page_count,
		ddps.in_row_reserved_page_count,
		ddps.lob_reserved_page_count,
		CASE pf.boundary_value_on_right
			WHEN 1 THEN 'less than'
			ELSE 'less than or equal to'
		END AS comparison,
		value
FROM sys.dm_db_partition_stats AS ddps
	JOIN sys.indexes AS i 
		ON ddps.object_id = i.object_id AND ddps.index_id = i.index_id
	JOIN sys.partition_schemes ps
		ON ps.data_space_id = i.data_space_id
	JOIN sys.partition_functions pf
		ON pf.function_id = ps.function_id
	LEFT OUTER JOIN sys.partition_range_values prv
		ON pf.function_id = prv.function_id AND ddps.partition_number = prv.boundary_id
WHERE OBJECT_NAME(ddps.object_id) = 'salesOrder' AND ddps.index_id IN (0, 1); --Clustered table or Heap
GO

--Investigating fragmentation --sys.dm_db_index_physical_stats
--Fragmentation statistics for the testClusteredIdentity clustered table
SELECT avg_fragmentation_in_percent,
		fragment_count,
		avg_fragment_size_in_pages
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'DETAILED')
WHERE index_type_desc = 'CLUSTERED INDEX'
	AND index_level = 0 --the other levels are the index pages
	AND OBJECT_NAME(object_id) = 'testClusteredIdentity';
GO

SELECT avg_fragmentation_in_percent,
		fragment_count,
		avg_fragment_size_in_pages
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'DETAILED')
WHERE index_type_desc = 'CLUSTERED INDEX'
	AND index_level = 0 --the other levels are the index pages
	AND OBJECT_NAME(object_id) = 'testClustered';
GO

--Fragmentation in heaps
SELECT avg_fragmentation_in_percent,
		fragment_count,
		avg_fragment_size_in_pages,
		forwarded_record_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'DETAILED')
WHERE index_type_desc = 'HEAP'
	AND index_level = 0 --the other levels are the index pages
	AND OBJECT_NAME(object_id) = 'testHeap';
GO

--Diagnosing I/O Bottlenecks
--Investigating physical I/O and I/O stalls
--Capturing baseline disk I/O statistics from sys.dm_io_virtual_file_stats in a temporary table
SELECT DB_NAME(mf.database_id) AS DatabaseName,
		mf.physical_name,
		divfs.num_of_reads,
		divfs.num_of_bytes_read,
		divfs.io_stall_read_ms,
		divfs.num_of_writes,
		divfs.num_of_bytes_written,
		divfs.io_stall_write_ms,
		divfs.io_stall,
		size_on_disk_bytes,
		GETDATE() AS baselineDate
INTO #baseline
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs
	JOIN sys.master_files AS mf
		ON mf.database_id = divfs.database_id AND mf.file_id = divfs.file_id;
GO

SELECT physical_name ,
		num_of_reads ,
		num_of_bytes_read ,
		io_stall_read_ms
FROM #baseline
WHERE databaseName = 'Axe_Credit';
GO

--Capturing 10 seconds of disk I/O statistics, since the baseline measurment.
WITH currentLine
	AS ( SELECT DB_NAME(mf.database_id) AS databaseName ,
			mf.physical_name ,
			num_of_reads ,
			num_of_bytes_read ,
			io_stall_read_ms ,
			num_of_writes ,
			num_of_bytes_written ,
			io_stall_write_ms ,
			io_stall ,
			size_on_disk_bytes ,
			GETDATE() AS currentlineDate
		FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs
			JOIN sys.master_files AS mf
				ON mf.database_id = divfs.database_id
					AND mf.file_id = divfs.file_id
	)
SELECT currentLine.databaseName ,
		LEFT(currentLine.physical_name, 1) AS drive ,
		currentLine.physical_name ,
		DATEDIFF(millisecond,baseLineDate,currentLineDate) AS elapsed_ms,
			currentLine.io_stall - #baseline.io_stall AS io_stall_ms ,
			currentLine.io_stall_read_ms - #baseline.io_stall_read_ms AS io_stall_read_ms ,
			currentLine.io_stall_write_ms - #baseline.io_stall_write_ms AS io_stall_write_ms ,
			currentLine.num_of_reads - #baseline.num_of_reads AS num_of_reads ,
			currentLine.num_of_bytes_read - #baseline.num_of_bytes_read AS num_of_bytes_read ,
			currentLine.num_of_writes - #baseline.num_of_writes AS num_of_writes ,
			currentLine.num_of_bytes_written - #baseline.num_of_bytes_written AS num_of_bytes_written
FROM currentLine
	INNER JOIN #baseline ON #baseLine.databaseName = currentLine.databaseName
	AND #baseLine.physical_name = currentLine.physical_name
WHERE #baseline.databaseName = 'Axe_Credit';
GO

--Viewing pending I/O requests
SELECT * FROM sys.dm_io_pending_io_requests;
GO

--Returning pending I/O requests
SELECT mf.physical_name,
		dipir.io_pending,
		dipir.io_pending_ms_ticks
FROM sys.dm_io_pending_io_requests AS dipir
	JOIN sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs
		ON dipir.io_handle = divfs.file_handle
	JOIN sys.master_files AS mf
		ON divfs.database_id = mf.database_id AND divfs.file_id = mf.file_id
ORDER BY dipir.io_pending, --Show I/O completed by the OS first
		dipir.io_pending_ms_ticks DESC;
GO

--Finding the Read:Write Ratio
--The read:write ratio by database, for amount of data transferred
--uses a LIKE comparison to only include desired databases, rather than
--using the database_id parameter of sys.dm_io_virtual_file_stats
--if you have a rather large number of databases, this may not be the
--optimal way to execute the query, but this gives you flexibility
--to look at multiple databases simultaneously.
DECLARE @databaseName SYSNAME
SET @databaseName = '%'
--'%' gives all databases

SELECT CAST(SUM(num_of_bytes_read) AS DECIMAL)
		/ ( CAST(SUM(num_of_bytes_written) AS DECIMAL)
		+ CAST(SUM(num_of_bytes_read) AS DECIMAL) ) AS RatioOfReads ,
		CAST(SUM(num_of_bytes_written) AS DECIMAL)
		/ ( CAST(SUM(num_of_bytes_written) AS DECIMAL)
		+ CAST(SUM(num_of_bytes_read) AS DECIMAL) ) AS RatioOfWrites ,
		SUM(num_of_bytes_read) AS TotalBytesRead ,
		SUM(num_of_bytes_written) AS TotalBytesWritten
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs
WHERE DB_NAME(database_id) LIKE @databaseName;
GO

--The read:write ratio, by drive, for amount of data transferred
DECLARE @databaseName SYSNAME
SET @databaseName = '%'
--'%' gives all databases

SELECT LEFT(physical_name, 1) AS drive ,
		CAST(SUM(num_of_bytes_read) AS DECIMAL)
		/ ( CAST(SUM(num_of_bytes_written) AS DECIMAL)
		+ CAST(SUM(num_of_bytes_read) AS DECIMAL) ) AS RatioOfReads ,
		CAST(SUM(num_of_bytes_written) AS DECIMAL)
		/ ( CAST(SUM(num_of_bytes_written) AS DECIMAL)
		+ CAST(SUM(num_of_bytes_read) AS DECIMAL) ) AS RatioOfWrites ,
		SUM(num_of_bytes_read) AS TotalBytesRead ,
		SUM(num_of_bytes_written) AS TotalBytesWritten
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs
	JOIN sys.master_files AS mf ON mf.database_id = divfs.database_id
		AND mf.file_id = divfs.file_id
WHERE DB_NAME(divfs.database_id) LIKE @databaseName
GROUP BY LEFT(mf.physical_name, 1);
GO

--Number of read and write operations
DECLARE @databaseName SYSNAME
SET @databaseName = 'Axe_Credit'
--obviously not the real name
--'%' gives all databases

SELECT CAST(SUM(num_of_reads) AS DECIMAL)
		/ ( CAST(SUM(num_of_writes) AS DECIMAL)
		+ CAST(SUM(num_of_reads) AS DECIMAL) ) AS RatioOfReads ,
		CAST(SUM(num_of_writes) AS DECIMAL)
		/ ( CAST(SUM(num_of_reads) AS DECIMAL)
		+ CAST(SUM(num_of_writes) AS DECIMAL) ) AS RatioOfWrites ,
		SUM(num_of_reads) AS TotalReadOperations ,
		SUM(num_of_writes) AS TotalWriteOperations
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS divfs
WHERE DB_NAME(database_id) LIKE @databaseName;
GO

--Number of reads and writes at the table level
DECLARE @databaseName SYSNAME
SET @databaseName = 'Axe_Credit' -- '%' gives all databases

SELECT CASE
		WHEN (SUM(user_updates + user_seeks + user_scans + user_lookups) = 0)
			THEN NULL
			ELSE (CAST(SUM(user_seeks + user_scans + user_lookups) AS DECIMAL)
				/ CAST(SUM(user_updates + user_seeks + user_scans + user_lookups) AS DECIMAL))
		END AS RatioOfReads,
		CASE
		WHEN(SUM(user_updates + user_seeks + user_scans + user_lookups) = 0)
		THEN NULL
		ELSE (CAST(SUM(user_updates) AS DECIMAL)
			/CAST(SUM(user_updates + user_seeks + user_scans + user_lookups) AS DECIMAL))
		END AS RatioOfWrites,
		SUM(user_updates + user_seeks + user_scans + user_lookups) AS TotalReadOperations,
		SUM(user_updates) AS TotalWriteOperations
FROM sys.dm_db_index_usage_stats AS ddius
WHERE DB_NAME(database_id) LIKE @databaseName;
GO

-- Read:write ratio per object
SELECT OBJECT_NAME(ddius.object_id) AS object_name,
		CASE
		WHEN (SUM(user_updates + user_seeks + user_scans + user_lookups) = 0)
			THEN NULL
			ELSE (CAST(SUM(user_seeks + user_scans + user_lookups) AS DECIMAL)
				/ CAST(SUM(user_updates + user_seeks + user_scans + user_lookups) AS DECIMAL))
		END AS RatioOfReads,
		CASE
		WHEN(SUM(user_updates + user_seeks + user_scans + user_lookups) = 0)
		THEN NULL
		ELSE (CAST(SUM(user_updates) AS DECIMAL)
			/CAST(SUM(user_updates + user_seeks + user_scans + user_lookups) AS DECIMAL))
		END AS RatioOfWrites,
		SUM(user_updates + user_seeks + user_scans + user_lookups) AS TotalReadOperations,
		SUM(user_updates) AS TotalWriteOperations
FROM sys.dm_db_index_usage_stats AS ddius
	JOIN sys.indexes AS i
		ON ddius.object_id = i.object_id AND ddius.index_id = i.index_id
WHERE i.type_desc IN ('CLUSTERED', 'HEAP') --only works in current db
GROUP BY ddius.object_id
ORDER BY OBJECT_NAME(ddius.object_id)
--DB_NAME(database_id) LIKE @databaseName;
GO

--Getting Stats about tempdb Usage
SELECT * FROM sys.dm_db_file_space_usage;
GO

--Overview of tempdb utilization
SELECT mf.physical_name,
		mf.size AS entire_file_page_count,
		dfsu.version_store_reserved_page_count,
		dfsu.unallocated_extent_page_count,
		dfsu.user_object_reserved_page_count,
		dfsu.internal_object_reserved_page_count,
		dfsu.mixed_extent_page_count
FROM sys.dm_db_file_space_usage dfsu
	JOIN sys.master_files AS mf
		ON mf.database_id = dfsu.database_id AND mf.file_id = dfsu.file_id;
GO

--tempdb file size and version store usage
SELECT SUM(mf.size) AS entire_page_count,
		SUM(dfsu.version_store_reserved_page_count) AS version_store_reserved_page_count
FROM sys.dm_db_file_space_usage dfsu
	JOIN sys.master_files AS mf
		ON mf.database_id = dfsu.database_id AND mf.file_id = dfsu.file_id;
GO
