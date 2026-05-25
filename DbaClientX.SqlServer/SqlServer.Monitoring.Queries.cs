namespace DBAClientX;

public partial class SqlServer
{
    private const string ConnectionDiagnosticsQuery = @"
SELECT
    MachineName = CONVERT(nvarchar(128), SERVERPROPERTY('MachineName')),
    ServerName = CONVERT(nvarchar(256), SERVERPROPERTY('ServerName')),
    InstanceName = CONVERT(nvarchar(128), SERVERPROPERTY('InstanceName')),
    ProductVersion = CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')),
    ProductLevel = CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')),
    Edition = CONVERT(nvarchar(256), SERVERPROPERTY('Edition')),
    EngineEdition = TRY_CONVERT(int, SERVERPROPERTY('EngineEdition')),
    OriginalLogin = ORIGINAL_LOGIN(),
    EffectiveLogin = SUSER_SNAME(),
    SessionId = @@SPID,
    LocalNetAddress = c.local_net_address,
    LocalTcpPort = c.local_tcp_port,
    ClientNetAddress = c.client_net_address,
    AuthScheme = c.auth_scheme,
    EncryptOption = c.encrypt_option,
    ProtocolType = c.protocol_type
FROM sys.dm_exec_connections AS c
WHERE c.session_id = @@SPID;";

    private const string DatabaseStateQuery = @"
SELECT
    DatabaseName = d.name,
    Status = d.state_desc,
    AccessMode = d.user_access_desc,
    ReadWrite = CASE WHEN d.is_read_only = 1 THEN N'READ_ONLY' ELSE N'READ_WRITE' END,
    RecoveryModel = d.recovery_model_desc,
    IsSystemDatabase = CASE WHEN d.name IN (N'master', N'model', N'msdb', N'tempdb', N'distribution') THEN 1 ELSE 0 END,
    CreateDate = d.create_date
FROM sys.databases AS d
WHERE @includeFilteredRows = 1
   OR d.name NOT IN (N'master', N'model', N'msdb', N'tempdb', N'distribution')
ORDER BY d.name;";

    private const string BackupFreshnessQuery = @"
WITH last_backup AS
(
    SELECT
        bs.database_name,
        LastFullBackup = MAX(CASE WHEN bs.type = 'D' THEN bs.backup_finish_date END),
        LastDifferentialBackup = MAX(CASE WHEN bs.type = 'I' THEN bs.backup_finish_date END),
        LastLogBackup = MAX(CASE WHEN bs.type = 'L' THEN bs.backup_finish_date END)
    FROM msdb.dbo.backupset AS bs
    GROUP BY bs.database_name
)
SELECT
    DatabaseName = d.name,
    RecoveryModel = d.recovery_model_desc,
    DatabaseCreated = d.create_date,
    lb.LastFullBackup,
    lb.LastDifferentialBackup,
    lb.LastLogBackup
FROM sys.databases AS d
LEFT JOIN last_backup AS lb ON lb.database_name = d.name
WHERE @includeFilteredRows = 1
   OR d.name NOT IN (N'master', N'model', N'msdb', N'tempdb', N'distribution')
ORDER BY d.name;";

    private const string AgentJobsQuery = @"
WITH last_history AS
(
    SELECT
        h.job_id,
        h.run_status,
        h.run_date,
        h.run_time,
        h.run_duration,
        rn = ROW_NUMBER() OVER (PARTITION BY h.job_id ORDER BY h.instance_id DESC)
    FROM msdb.dbo.sysjobhistory AS h
    WHERE h.step_id = 0
),
active_jobs AS
(
    SELECT
        ja.job_id,
        ja.start_execution_date,
        ja.stop_execution_date,
        rn = ROW_NUMBER() OVER (PARTITION BY ja.job_id ORDER BY ja.start_execution_date DESC)
    FROM msdb.dbo.sysjobactivity AS ja
    WHERE ja.start_execution_date IS NOT NULL
)
SELECT
    JobId = j.job_id,
    Name = j.name,
    Category = c.name,
    OwnerLoginName = SUSER_SNAME(j.owner_sid),
    Enabled = CONVERT(bit, j.enabled),
    IsRunning = CONVERT(bit, CASE WHEN a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL THEN 1 ELSE 0 END),
    CurrentStartDate = CASE WHEN a.start_execution_date IS NOT NULL AND a.stop_execution_date IS NULL THEN a.start_execution_date ELSE NULL END,
    LastRunDate = msdb.dbo.agent_datetime(h.run_date, h.run_time),
    LastRunOutcome = CASE h.run_status
        WHEN 0 THEN N'Failed'
        WHEN 1 THEN N'Succeeded'
        WHEN 2 THEN N'Retry'
        WHEN 3 THEN N'Cancelled'
        WHEN 4 THEN N'InProgress'
        ELSE NULL
    END,
    LastRunDuration = h.run_duration
FROM msdb.dbo.sysjobs AS j
LEFT JOIN msdb.dbo.syscategories AS c ON c.category_id = j.category_id
LEFT JOIN last_history AS h ON h.job_id = j.job_id AND h.rn = 1
LEFT JOIN active_jobs AS a ON a.job_id = j.job_id AND a.rn = 1
WHERE @includeFilteredRows = 1 OR j.enabled = 1
ORDER BY j.name;";

    private const string WaitStatisticsQuery = @"
WITH waits AS
(
    SELECT
        wait_type,
        wait_time_ms,
        signal_wait_time_ms,
        waiting_tasks_count
    FROM sys.dm_os_wait_stats
    WHERE wait_time_ms > 0
      AND wait_type NOT LIKE N'SLEEP%'
      AND wait_type NOT IN
      (
          N'BROKER_EVENTHANDLER', N'BROKER_RECEIVE_WAITFOR', N'BROKER_TASK_STOP',
          N'BROKER_TO_FLUSH', N'BROKER_TRANSMITTER', N'CHECKPOINT_QUEUE',
          N'CLR_AUTO_EVENT', N'CLR_MANUAL_EVENT', N'CLR_SEMAPHORE',
          N'DBMIRROR_DBM_EVENT', N'DBMIRROR_EVENTS_QUEUE', N'DBMIRROR_WORKER_QUEUE',
          N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
          N'LAZYWRITER_SLEEP', N'LOGMGR_QUEUE', N'ONDEMAND_TASK_QUEUE',
          N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'REQUEST_FOR_DEADLOCK_SEARCH',
          N'RESOURCE_QUEUE', N'SERVER_IDLE_CHECK', N'SP_SERVER_DIAGNOSTICS_SLEEP',
          N'SQLTRACE_BUFFER_FLUSH', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
          N'WAITFOR', N'XE_DISPATCHER_WAIT', N'XE_TIMER_EVENT'
      )
),
ranked AS
(
    SELECT
        wait_type,
        wait_time_ms,
        signal_wait_time_ms,
        waiting_tasks_count,
        pct = CONVERT(decimal(18, 4), 100.0 * wait_time_ms / NULLIF(SUM(wait_time_ms) OVER (), 0)),
        running_pct = CONVERT(decimal(18, 4), 100.0 * SUM(wait_time_ms) OVER (ORDER BY wait_time_ms DESC ROWS UNBOUNDED PRECEDING) / NULLIF(SUM(wait_time_ms) OVER (), 0))
    FROM waits
)
SELECT
    WaitType = wait_type,
    WaitSeconds = CONVERT(decimal(18, 4), wait_time_ms / 1000.0),
    ResourceSeconds = CONVERT(decimal(18, 4), (wait_time_ms - signal_wait_time_ms) / 1000.0),
    SignalSeconds = CONVERT(decimal(18, 4), signal_wait_time_ms / 1000.0),
    WaitCount = waiting_tasks_count,
    Percentage = pct,
    AverageWaitSeconds = CONVERT(decimal(18, 6), wait_time_ms / 1000.0 / NULLIF(waiting_tasks_count, 0))
FROM ranked
WHERE running_pct <= @threshold OR pct >= 1.0
ORDER BY WaitSeconds DESC;";

    private const string AvailabilityGroupsQuery = @"
IF CONVERT(int, SERVERPROPERTY('IsHadrEnabled')) = 1
BEGIN
    SELECT
        AvailabilityGroupName = ag.name,
        ReplicaServerName = ar.replica_server_name,
        Role = ars.role_desc,
        OperationalState = ars.operational_state_desc,
        ConnectedState = ars.connected_state_desc,
        SynchronizationHealth = ars.synchronization_health_desc,
        DatabaseName = DB_NAME(drs.database_id),
        DatabaseSynchronizationState = drs.synchronization_state_desc,
        DatabaseSynchronizationHealth = drs.synchronization_health_desc,
        IsSuspended = CONVERT(bit, ISNULL(drs.is_suspended, 0)),
        SuspendReason = drs.suspend_reason_desc
    FROM sys.availability_groups AS ag
    INNER JOIN sys.availability_replicas AS ar ON ag.group_id = ar.group_id
    LEFT JOIN sys.dm_hadr_availability_replica_states AS ars ON ar.replica_id = ars.replica_id
    LEFT JOIN sys.dm_hadr_database_replica_states AS drs ON ar.replica_id = drs.replica_id
    ORDER BY ag.name, ar.replica_server_name, DatabaseName;
END;";
}
