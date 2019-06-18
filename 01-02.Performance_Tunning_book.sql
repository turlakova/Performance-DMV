USE master;
GO

SELECT [name] ,
	CASE [type]
		WHEN 'V' THEN 'DMV'
		WHEN 'IF' THEN 'DMF'
	END AS [DMO Type]
FROM [sys].[sysobjects]
WHERE [name] LIKE 'dm_%'
ORDER BY [name];
GO --179 rows

SELECT * FROM sysprocesses;
GO

SELECT * FROM master.dbo.sysobjects;
GO

--new

SELECT * FROM sys.sysobjects;
GO
SELECT * FROM sys.objects;
GO
SELECT * FROM sys.sysindexes;
GO
SELECT * FROM sys.indexes;
GO
SELECT * FROM sys.tables;
GO

SELECT * FROM sys.sysprocesses;
GO
SELECT * FROM sys.dm_exec_requests
WHERE session_id > 50;
GO

SELECT * FROM sys.dm_exec_connections;
GO

SELECT * FROM sys.dm_exec_sessions
WHERE session_id > 50;
GO

EXEC sp_who2;

--DMO Security and Permissions
--VIEW SERVER STATE ; VIEW DATABASE STATE

--Performance tunning with DMOs
--wait statistics
SELECT wait_type,
	SUM(wait_time_ms / 100) AS [wait_time_s]
FROM sys.dm_os_wait_stats DOWS
WHERE wait_type NOT IN ('SLEEP_TASK', 'BROKER_TASK_STOP',
						'SQLTRACE_BUFFER_FLUSH', 'CLR_AUTO_EVENT',
						'CLR_MANUAL_EVENT', 'LAZYWRITER_SLEEP')
GROUP BY wait_type
ORDER BY SUM(wait_time_ms) DESC;
GO

SELECT * FROM sys.dm_exec_query_stats;
GO

--02.Connections, Sessions and Requests
SELECT * FROM sys.dm_os_threads;
GO

--Get a count of SQL connections by IP address
SELECT dec.client_net_address,
		des.program_name,
		des.host_name,
		des.login_name,
		COUNT(dec.session_id) AS connection_count
FROM sys.dm_exec_sessions AS des
	INNER JOIN sys.dm_exec_connections AS dec
		ON des.session_id = dec.session_id
--WHERE LEFT(des.host_name, 2) = 'WK'
GROUP BY dec.client_net_address, des.program_name, des.host_name, des.login_name
--HAVING COUNT(dec.session_id) > 1
ORDER BY des.program_name, dec.client_net_address
GO

--connected by SSMS
SELECT dec.client_net_address,
		des.login_name,
		des.host_name,
		des.program_name,
		dest.text
FROM sys.dm_exec_sessions des
	INNER JOIN sys.dm_exec_connections dec
		ON des.session_id = dec.session_id
	CROSS APPLY sys.dm_exec_sql_text(dec.most_recent_sql_handle) dest
WHERE des.program_name LIKE 'Microsoft SQL Server Management Studio%'
ORDER BY des.program_name, dec.client_net_address;
GO

--Session level settings
SELECT des.text_size,
		des.language,
		des.date_format,
		des.date_first,
		des.quoted_identifier,
		des.arithabort,
		des.ansi_null_dflt_on,
		des.ansi_defaults,
		des.ansi_warnings,
		des.ansi_padding,
		des.ansi_nulls,
		des.concat_null_yields_null,
		des.transaction_isolation_level,
		des.lock_timeout,
		des.deadlock_priority
FROM sys.dm_exec_sessions des
WHERE des.session_id = @@SPID
GO

--Logins with more than one session
SELECT login_name,
	COUNT(session_id) AS session_count
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
GROUP BY login_name
ORDER BY login_name;
GO

--Identify sessions with context switching
SELECT session_id,
	login_name,
	original_login_name
FROM sys.dm_exec_sessions
WHERE is_user_process = 1
	AND login_name <> original_login_name;
GO

--Identify inactive sessions
DECLARE @days_old SMALLINT
SELECT @days_old = 5

SELECT des.session_id,
		des.login_time,
		des.last_request_start_time,
		des.last_request_end_time,
		des.status,
		des.program_name,
		des.cpu_time,
		des.total_elapsed_time,
		des.memory_usage,
		des.total_scheduled_time,
		des.total_elapsed_time,
		des.reads,
		des.writes,
		des.logical_reads,
		des.row_count,
		des.is_user_process
FROM sys.dm_exec_sessions AS des
	INNER JOIN sys.dm_tran_session_transactions AS dtst
		ON des.session_id = dtst.session_id
WHERE des.is_user_process = 1
	AND DATEDIFF(dd, des.last_request_end_time, GETDATE()) > @days_old
	AND des.status != 'Running'
ORDER BY des.last_request_end_time;
GO

--Identify idle sessions with orphaned transactions
SELECT des.session_id,
		des.login_time,
		des.last_request_start_time,
		des.last_request_end_time,
		des.host_name,
		des.login_name
FROM sys.dm_exec_sessions des
	INNER JOIN sys.dm_tran_session_transactions dtst
		ON des.session_id = dtst.session_id
	LEFT JOIN sys.dm_exec_requests der
		ON dtst.session_id = der.session_id
WHERE der.session_id IS NULL
ORDER BY des.session_id;
GO

--Requests - overview of sys.dm_exec_requests
--Returning the SQL text of ad hoc queries
SELECT dest.text,
		dest.dbid,
		dest.objectid
FROM sys.dm_exec_requests AS der
	CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest
WHERE session_id = @@SPID

--Isolating the executing statements within a SQL handle
--Parsing the SQL text using statement_start_offset and statement_end_offset
SELECT der.statement_start_offset,
		der.statement_end_offset,
		SUBSTRING(dest.text, der.statement_start_offset / 2,
			(CASE WHEN der.statement_end_offset = -1
				THEN DATALENGTH(dest.text)
				ELSE der.statement_end_offset
				END - der.statement_start_offset) / 2) AS statement_executing,
		dest.text AS [full statement code]
FROM sys.dm_exec_requests der
	INNER JOIN sys.dm_exec_sessions des
		ON des.session_id = der.session_id
	CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) dest
WHERE des.is_user_process = 1 AND der.session_id <> @@SPID
ORDER BY der.session_id;
GO

--Investigating work done by requests
SELECT der.session_id,
	der.blocking_session_id,
	DB_NAME(der.database_id) AS database_name,
	deqp.query_plan,
	SUBSTRING(dest.text, der.statement_start_offset / 2,
			(CASE WHEN der.statement_end_offset = -1
				THEN DATALENGTH(dest.text)
				ELSE der.statement_end_offset
				END - der.statement_start_offset) / 2) AS statement_executing,
	der.cpu_time,
	der.granted_query_memory,
	der.wait_time,
	der.total_elapsed_time,
	der.reads,
	der.logical_reads,
	der.wait_type,
	der.last_wait_type,
	der.wait_resource
FROM sys.dm_exec_requests der
	INNER JOIN sys.dm_exec_sessions des
		ON des.session_id = der.session_id
	CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) dest
	CROSS APPLY sys.dm_exec_query_plan(der.plan_handle) deqp
WHERE des.is_user_process = 1 AND der.session_id <> @@SPID
ORDER BY der.cpu_time DESC;
--ORDER BY der.granted_query_memory DESC;
--ORDER BY der.wait_time DESC;
--ORDER BY der.total_elapsed_time DESC;
--ORDER BY der.reads DESC;
GO

--Dissecting user activity
--Who is running what at this time
SELECT dest.text AS [Command Text],
	des.login_name,
	des.host_name,
	des.program_name,
	der.session_id,
	der.blocking_session_id,
	dec.client_net_address,
	der.status,
	der.command,
	DB_NAME(der.database_id) AS DatabaseName
FROM sys.dm_exec_requests der
	INNER JOIN sys.dm_exec_connections dec
		ON der.session_id = dec.session_id
	INNER JOIN sys.dm_exec_sessions des
		ON des.session_id = der.session_id
	CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest
WHERE des.is_user_process = 1;
GO

--A WHOLE INFORMATION OF ACTIVITY, RESOURCE IMPACT, PROCESSING HEALTH AND CURRENTLY RUNNING
SELECT des.session_id,
	des.status,
	des.login_name,
	des.host_name,
	der.blocking_session_id,
	DB_NAME(der.database_id) AS database_name,
	der.command,
	des.cpu_time,
	des.reads,
	des.writes,
	des.logical_reads,
	dec.last_write,
	des.program_name,
	der.wait_type,
	der.wait_time,
	der.last_wait_type,
	der.wait_resource,
	der.percent_complete,
	CASE des.transaction_isolation_level
		WHEN 0 THEN 'Unspecified'
		WHEN 1 THEN 'ReadUncommitted'
		WHEN 2 THEN 'ReadCommitted'
		WHEN 3 THEN 'Repeatable'
		WHEN 4 THEN 'Serializable'
		WHEN 5 THEN 'Snapshot'
	END AS transaction_isolation_level,
	OBJECT_NAME(dest.objectid, der.database_id) AS OBJECT_NAME,
	SUBSTRING(dest.text, der.statement_start_offset / 2,
			(CASE WHEN der.statement_end_offset = -1
				THEN DATALENGTH(dest.text)
				ELSE der.statement_end_offset
				END - der.statement_start_offset) / 2) AS statement_executing,
	deqp.query_plan
FROM sys.dm_exec_sessions des
	LEFT JOIN sys.dm_exec_requests der
		ON des.session_id = der.session_id
	LEFT JOIN sys.dm_exec_connections dec
		ON des.session_id = dec.session_id
	CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) dest
	CROSS APPLY sys.dm_exec_query_plan(der.plan_handle) deqp
WHERE des.session_id <> @@SPID
ORDER BY des.session_id;
GO

select * from sys.databases

EXEC dbo.sp_WhoIsActive @get_transaction_info = 1,
                        @get_outer_command = 1,
                        @get_plans = 1,
						@show_sleeping_spids = 0,
						@get_locks = 1
--EXEC master.dbo.xp_readerrorlog 0, 1, N'error', NULL, N'2019-03-19', N'2019-03-20', N'desc'

--Retrieving an XML deadlock graph in SQL Server 2008
SELECT  CAST(target_data AS XML) AS TargetData
FROM    sys.dm_xe_session_targets st
        JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
WHERE   name = 'system_health' 

SELECT  CAST(event_data.value('(event/data/value)[1]',
                               'varchar(max)') AS XML) AS DeadlockGraph
FROM    ( SELECT    XEvent.query('.') AS event_data
          FROM      (    -- Cast the target_data to XML 
                      SELECT    CAST(target_data AS XML) AS TargetData
                      FROM      sys.dm_xe_session_targets st
                                JOIN sys.dm_xe_sessions s
                                 ON s.address = st.event_session_address
                      WHERE     name = 'system_health'
                                AND target_name = 'ring_buffer'
                    ) AS Data -- Split out the Event Nodes 
                    CROSS APPLY TargetData.nodes('RingBufferTarget/
                                     event[@name="xml_deadlock_report"]')
                    AS XEventData ( XEvent )
        ) AS tab ( event_data ) 
GO

--Transaction log impact of active transactions
SELECT dtst.session_id,
		des.login_name,
		DB_NAME(dtdt.database_id) AS [Database],
		dtdt.database_transaction_begin_time,
		DATEDIFF(ms, dtdt.database_transaction_begin_time, GETDATE()) AS [Duration ms],
		CASE dtat.transaction_type
			WHEN 1 THEN 'Read/Write'
			WHEN 2 THEN 'Read-only'
			WHEN 3 THEN 'System'
			WHEN 1 THEN 'Distributed'
		END AS [Transaction Type],
		CASE dtat.transaction_type
			WHEN 0 THEN 'Not fully initialized'
			WHEN 1 THEN 'Initialized, not started'
			WHEN 2 THEN 'Active'
			WHEN 3 THEN 'Ended'
			WHEN 4 THEN 'Commit initiated'
			WHEN 5 THEN 'Prepared, awaiting resolution'
			WHEN 6 THEN 'Commited'
			WHEN 7 THEN 'Rolling back'
			WHEN 8 THEN 'Rolled back'
		END AS [Transaction State],
		dtdt.database_transaction_log_record_count,
		dtdt.database_transaction_log_bytes_used,
		dtdt.database_transaction_log_bytes_reserved,
		dest.text,
		deqp.query_plan
FROM sys.dm_tran_database_transactions dtdt
	INNER JOIN sys.dm_tran_session_transactions dtst
		ON dtdt.transaction_id = dtst.transaction_id
	INNER JOIN sys.dm_tran_active_transactions dtat
		ON dtst.transaction_id = dtat.transaction_id
	INNER JOIN sys.dm_exec_sessions des
		ON des.session_id = dtst.session_id
	INNER JOIN sys.dm_exec_connections dec
		ON dec.session_id = dtst.session_id
	INNER JOIN sys.dm_exec_requests der
		ON der.session_id = dtst.session_id
	CROSS APPLY sys.dm_exec_sql_text(dec.most_recent_sql_handle) AS dest
	OUTER APPLY sys.dm_exec_query_plan(der.plan_handle) AS deqp
ORDER BY dtdt.database_transaction_log_bytes_used DESC;
--ORDER BY [Duration ms] DESC;
GO