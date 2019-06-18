--04.Transactions
--Investigating Locking and Blocking
SELECT * FROM sys.dm_tran_locks;
GO

SELECT dtl.request_session_id,
		dtl.resource_type,
		DB_NAME(dtl.resource_database_id) AS [Database Name],
		CASE WHEN dtl.resource_type IN ('DATADASE', 'FILE', 'METADATA')
			THEN dtl.resource_type
			WHEN dtl.resource_type = 'OBJECT'
			THEN OBJECT_NAME(dtl.resource_associated_entity_id, dtl.resource_database_id)
			WHEN dtl.resource_type IN ('KEY', 'PAGE', 'RID')
			THEN (SELECT OBJECT_NAME(object_id) FROM sys.partitions
					WHERE sys.partitions.hobt_id = dtl.resource_associated_entity_id)
			ELSE 'Unidentified'
		END AS [Parent Object],
		dtl.request_mode AS [Lock Type],
		dtl.resource_description,
		dtl.request_status,
		der.blocking_session_id,
		des.login_name,
		CASE dtl.request_lifetime
			WHEN 0 THEN dest_r.text
			ELSE dest_c.text
		END AS [Statement]
FROM sys.dm_tran_locks AS dtl
	LEFT JOIN sys.dm_exec_requests AS der
		ON dtl.request_session_id = der.session_id
	INNER JOIN sys.dm_exec_sessions AS des
		ON dtl.request_session_id = des.session_id
	INNER JOIN sys.dm_exec_connections AS dec
		ON  dtl.request_session_id = dec.most_recent_session_id
	OUTER APPLY sys.dm_exec_sql_text(dec.most_recent_sql_handle) AS dest_c
	OUTER APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest_r
WHERE dtl.resource_database_id = DB_ID()
	AND dtl.resource_type NOT IN ('DATABASE', 'METADATA')
ORDER BY dtl.request_session_id;
GO

--Blocking analysis using sys.dm_tran_locks and sys.dm_os_waiting_tasks
SELECT * FROM sys.dm_os_waiting_tasks;
GO

--Investigating locking and blocking based on waiting tasks
SELECT DB_NAME(der.database_id) AS database_name, dtl.resource_type AS [resource type],
		CASE WHEN dtl.resource_type IN ('DATABASE', 'FILE', 'METADATA')
			THEN dtl.resource_type
			WHEN dtl.resource_type = 'OBJECT'
			THEN OBJECT_NAME(dtl.resource_associated_entity_id)
			WHEN dtl.resource_type IN ('KEY', 'PAGE', 'RID')
			THEN (SELECT OBJECT_NAME(object_id)
					FROM sys.partitions
					WHERE sys.partitions.hobt_id = dtl.resource_associated_entity_id)
			ELSE 'Unidentified'
		END AS [Parent Object],
		dtl.request_mode AS [Lock Type],
		dtl.request_status,
		dowt.wait_duration_ms,
		dowt.wait_type,
		dowt.session_id AS [blocked_session_id],
		des_blocked.login_name AS [blocked_user],
		SUBSTRING(dest_blocked.text, der.statement_start_offset / 2,
					(CASE WHEN der.statement_end_offset = -1
						THEN DATALENGTH(dest_blocked.text)
						ELSE der.statement_end_offset
					END -der.statement_start_offset) / 2) AS [blocked_command],
		dowt.blocking_session_id,
		des_blocking.login_name AS [blocking_user],
		dest_blocking.text AS [blocking_command],
		dowt.resource_description AS [blocking resource detail]
FROM sys.dm_tran_locks dtl
	INNER JOIN sys.dm_os_waiting_tasks dowt
		ON dtl.lock_owner_address = dowt.resource_address
	INNER JOIN sys.dm_exec_requests der
		ON dowt.session_id = der.session_id
	INNER JOIN sys.dm_exec_sessions des_blocked
		ON dowt.session_id = des_blocked.session_id
	INNER JOIN sys.dm_exec_sessions des_blocking
		ON dowt.session_id = des_blocking.session_id
	INNER JOIN sys.dm_exec_connections dec
		ON dtl.request_session_id = dec.most_recent_session_id
	CROSS APPLY sys.dm_exec_sql_text(dec.most_recent_sql_handle) AS dest_blocking
	CROSS APPLY sys.dm_exec_sql_text(der.sql_handle) AS dest_blocked
--WHERE dtl.resource_database_id = DB_ID();
GO

--Analyzing Transactional activity
SELECT * FROM sys.dm_tran_session_transactions;
GO
SELECT * FROM sys.dm_tran_active_transactions;
GO
SELECT * FROM sys.dm_tran_database_transactions;
GO

--Querying sys.dm_db_tran_active_transactions
SELECT dtat.transaction_id,
		dtat.name,
		dtat.transaction_begin_time,
		CASE dtat.transaction_type
			WHEN 1 THEN 'Read/Wite'
			WHEN 2 THEN 'Read-only'
			WHEN 3 THEN 'System'
			WHEN 4 THEN 'Distributed'
		END AS transaction_type,
		CASE dtat.transaction_state
			WHEN 0 THEN 'Not fully initialized'
			WHEN 1 THEN 'Initialized, not started'
			WHEN 2 THEN 'Active'
			WHEN 3 THEN 'Ended' --only applies to read-only transactions
			WHEN 4 THEN 'Commit initialized' --distributed transactions only
			WHEN 5 THEN 'Prepared, awaiting resolution'
			WHEN 6 THEN 'Commited'
			WHEN 7 THEN 'Rolling back'
			WHEN 8 THEN 'Rolled back'
		END AS transaction_state,
		CASE dtat.dtc_state
			WHEN 1 THEN 'Active'
			WHEN 2 THEN 'Prepared'
			WHEN 3 THEN 'Commited'
			WHEN 4 THEN 'Aborted'
			WHEN 5 THEN 'Recovered'
		END AS dtc_state
FROM sys.dm_tran_active_transactions dtat
	INNER JOIN sys.dm_tran_session_transactions dtst
		ON dtat.transaction_id = dtst.transaction_id
WHERE dtst.is_user_transaction = 1
ORDER BY dtat.transaction_begin_time;
GO

--SELECT * FROM sys.dm_tran_database_transactions
--Assessing transaction log impact
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

--Snapshot isolation level and the tempdb Version store
--Investigating snapshot isolation
--DMV for current snapshot activity
SELECT * FROM sys.dm_tran_active_snapshot_database_transactions;
GO
SELECT * FROM sys.dm_tran_current_snapshot;
GO
SELECT * FROM sys.dm_tran_transactions_snapshot;
GO

--Current snapshot activity
--Count of currently active snapshots transactions
SELECT COUNT(transaction_sequence_num) AS [snapshot transaction count]
FROM sys.dm_tran_transactions_snapshot;
GO

--Interrogating the active_snapshot_database_transactions
SELECT dtasdt.transaction_id,
		dtasdt.session_id,
		dtasdt.first_snapshot_sequence_num,
		dtasdt.commit_sequence_num,
		dtasdt.is_snapshot,
		dtasdt.elapsed_time_seconds,
		dest.text 
FROM sys.dm_tran_active_snapshot_database_transactions dtasdt
	INNER JOIN sys.dm_exec_connections dec
		ON dtasdt.session_id = dec.most_recent_session_id
	INNER JOIN sys.dm_tran_database_transactions dtdt
		ON dtasdt.transaction_id = dtdt.transaction_id
	CROSS APPLY sys.dm_exec_sql_text(dec.most_recent_sql_handle) AS dest
WHERE dtdt.database_id = DB_ID();
GO

--Correlating the activity of the varios transactions that are using the version store
SELECT dtts.[transaction_sequence_num] ,
		trx_current.[session_id] AS current_session_id ,
		des_current.[login_name] AS [current session login] ,
		DEST_current.text AS [current session command] ,
		dtts.[snapshot_sequence_num] ,
		trx_existing.[session_id] AS existing_session_id ,
		DES_existing.[login_name] AS [existing session login] ,
		DEST_existing.text AS [existing session command]
FROM sys.dm_tran_transactions_snapshot dtts
	INNER JOIN sys.dm_tran_active_snapshot_database_transactions trx_current
		ON dtts.transaction_sequence_num = trx_current.transaction_sequence_num
	INNER JOIN sys.dm_exec_connections dec_current
		ON trx_current.session_id = dec_current.most_recent_session_id
	INNER JOIN sys.dm_exec_sessions des_current
		ON dec_current.most_recent_session_id = des_current.session_id
	INNER JOIN sys.dm_tran_active_snapshot_database_transactions trx_existing
		ON dtts.snapshot_sequence_num = trx_existing.transaction_sequence_num
	INNER JOIN sys.[dm_exec_connections] DEC_existing
		ON trx_existing.[session_id] = DEC_existing.[most_recent_session_id]
	INNER JOIN sys.[dm_exec_sessions] DES_existing
		ON DEC_existing.[most_recent_session_id] = DES_existing.[session_id]
	CROSS APPLY sys.[dm_exec_sql_text](dec_current.[most_recent_sql_handle]) DEST_current
	CROSS APPLY sys.[dm_exec_sql_text](DEC_existing.[most_recent_sql_handle]) DEST_existing
ORDER BY dtts.[transaction_sequence_num], dtts.[snapshot_sequence_num];
GO

--Version Store usage
SELECT * FROM sys.dm_tran_version_store;
GO
SELECT * FROM sys.dm_tran_top_version_generators;
GO

--Returning raw data from sys.dm_tran_version_store
SELECT DB_NAME(dtvs.database_id) AS [Database Name],
		dtvs.transaction_sequence_num,
		dtvs.version_sequence_num,
		CASE dtvs.status
			WHEN 0 THEN '1'
			WHEN 1 THEN '2'
		END AS pages,
		dtvs.record_length_first_part_in_bytes + dtvs.record_length_second_part_in_bytes AS [record length bytes]
FROM sys.dm_tran_version_store dtvs
ORDER BY DB_NAME(dtvs.database_id), dtvs.transaction_sequence_num, dtvs.version_sequence_num;
GO

--Storage requirements for the version store in the db
SELECT DB_NAME(DTVS.[database_id]) AS [Database Name],
		SUM(DTVS.[record_length_first_part_in_bytes]
		+ DTVS.[record_length_second_part_in_bytes]) AS [total store bytes consumed]
FROM sys.dm_tran_version_store DTVS
GROUP BY DB_NAME(DTVS.[database_id]);
GO

--Finding the highest-consuming version store record within tempdb
WITH version_store ( [rowset_id], [bytes consumed] )
AS ( SELECT TOP 1
		[rowset_id] ,
		SUM([record_length_first_part_in_bytes] + [record_length_second_part_in_bytes])
			AS [bytes consumed]
		FROM sys.dm_tran_version_store
		GROUP BY [rowset_id]
		ORDER BY SUM([record_length_first_part_in_bytes] + [record_length_second_part_in_bytes])
)
SELECT VS.[rowset_id] ,
		VS.[bytes consumed] ,
		DB_NAME(DTVS.[database_id]) AS [database name] ,
		DTASDT.[session_id] AS session_id ,
		DES.[login_name] AS [session login] ,
		DEST.text AS [session command]
FROM version_store VS
	INNER JOIN sys.[dm_tran_version_store] DTVS
		ON VS.rowset_id = DTVS.[rowset_id]
	INNER JOIN sys.[dm_tran_active_snapshot_database_transactions] DTASDT
		ON DTVS.[transaction_sequence_num] = DTASDT.[transaction_sequence_num]
	INNER JOIN sys.dm_exec_connections DEC
		ON DTASDT.[session_id] = DEC.[most_recent_session_id]
	INNER JOIN sys.[dm_exec_sessions] DES
		ON DEC.[most_recent_session_id] = DES.[session_id]
	CROSS APPLY sys.[dm_exec_sql_text](DEC.[most_recent_sql_handle])AS DEST;
GO

