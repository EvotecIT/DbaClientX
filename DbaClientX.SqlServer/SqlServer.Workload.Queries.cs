namespace DBAClientX;

public partial class SqlServer
{
    private const string QueryStoreStateQuery = @"
SELECT DB_NAME() AS DatabaseName, DB_ID() AS DatabaseId, desired_state_desc AS DesiredState,
       actual_state_desc AS ActualState, query_capture_mode_desc AS CaptureMode,
       current_storage_size_mb AS CurrentStorageSizeMb, max_storage_size_mb AS MaximumStorageSizeMb,
       readonly_reason AS ReadOnlyReason
FROM (VALUES (1)) AS context(id)
LEFT JOIN sys.database_query_store_options ON 1 = 1";

    private const string IndexUsageQuery = @"
SELECT TOP (@maximumRows) s.name AS SchemaName, t.name AS TableName, i.name AS IndexName,
       i.object_id AS ObjectId, i.index_id AS IndexId, i.is_primary_key AS IsPrimaryKey,
       i.is_unique AS IsUnique, i.is_unique_constraint AS IsUniqueConstraint, i.is_disabled AS IsDisabled,
       u.user_seeks AS UserSeeks, u.user_scans AS UserScans, u.user_lookups AS UserLookups,
       u.user_updates AS UserUpdates, u.last_user_seek AS LastUserSeek,
       u.last_user_scan AS LastUserScan, u.last_user_lookup AS LastUserLookup,
       u.last_user_update AS LastUserUpdate, p.last_updated AS StatisticsLastUpdated,
       p.rows AS StatisticsRows, p.rows_sampled AS StatisticsRowsSampled,
       p.modification_counter AS StatisticsModificationCounter, si.sqlserver_start_time AS ServerStartTime
FROM sys.tables AS t
JOIN sys.schemas AS s ON s.schema_id = t.schema_id
JOIN sys.indexes AS i ON i.object_id = t.object_id
LEFT JOIN sys.dm_db_index_usage_stats AS u ON u.database_id = DB_ID()
    AND u.object_id = i.object_id AND u.index_id = i.index_id
OUTER APPLY sys.dm_db_stats_properties(i.object_id, i.index_id) AS p
CROSS JOIN sys.dm_os_sys_info AS si
WHERE t.is_ms_shipped = 0 AND t.is_memory_optimized = 0 AND i.type IN (1, 2) AND i.is_hypothetical = 0
ORDER BY s.name, t.name, i.index_id, i.object_id";
}
