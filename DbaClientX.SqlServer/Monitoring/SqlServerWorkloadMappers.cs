using System;
using System.Data;

namespace DBAClientX.SqlServerMonitoring;

internal static class SqlServerWorkloadMappers
{
    internal static SqlServerQueryStoreState MapQueryStoreState(IDataRecord record) => new()
    {
        DatabaseName = record.GetString(record.GetOrdinal("DatabaseName")),
        DesiredState = record.GetString(record.GetOrdinal("DesiredState")),
        ActualState = record.GetString(record.GetOrdinal("ActualState")),
        CaptureMode = record.GetString(record.GetOrdinal("CaptureMode")),
        CurrentStorageSizeMb = Convert.ToInt64(record["CurrentStorageSizeMb"]),
        MaximumStorageSizeMb = Convert.ToInt64(record["MaximumStorageSizeMb"]),
        ReadOnlyReason = Convert.ToInt64(record["ReadOnlyReason"])
    };

    internal static (SqlServerIndexUsage Index, DateTime? ServerStartTime) MapIndexUsage(IDataRecord record) => (new()
    {
        SchemaName = record.GetString(record.GetOrdinal("SchemaName")),
        TableName = record.GetString(record.GetOrdinal("TableName")),
        IndexName = record.GetString(record.GetOrdinal("IndexName")),
        ObjectId = Convert.ToInt32(record["ObjectId"]),
        IndexId = Convert.ToInt32(record["IndexId"]),
        IsPrimaryKey = Convert.ToBoolean(record["IsPrimaryKey"]),
        IsUnique = Convert.ToBoolean(record["IsUnique"]),
        IsUniqueConstraint = Convert.ToBoolean(record["IsUniqueConstraint"]),
        IsDisabled = Convert.ToBoolean(record["IsDisabled"]),
        UserSeeks = NullableInt64(record, "UserSeeks"),
        UserScans = NullableInt64(record, "UserScans"),
        UserLookups = NullableInt64(record, "UserLookups"),
        UserUpdates = NullableInt64(record, "UserUpdates"),
        LastUserSeek = ServerLocalTime(record, "LastUserSeek"),
        LastUserScan = ServerLocalTime(record, "LastUserScan"),
        LastUserLookup = ServerLocalTime(record, "LastUserLookup"),
        LastUserUpdate = ServerLocalTime(record, "LastUserUpdate"),
        StatisticsLastUpdated = ServerLocalTime(record, "StatisticsLastUpdated"),
        StatisticsRows = NullableInt64(record, "StatisticsRows"),
        StatisticsRowsSampled = NullableInt64(record, "StatisticsRowsSampled"),
        StatisticsModificationCounter = NullableInt64(record, "StatisticsModificationCounter")
    }, ServerLocalTime(record, "ServerStartTime"));

    private static long? NullableInt64(IDataRecord record, string name)
        => record.IsDBNull(record.GetOrdinal(name)) ? null : Convert.ToInt64(record[name]);

    private static DateTime? ServerLocalTime(IDataRecord record, string name)
        => record.IsDBNull(record.GetOrdinal(name)) ? null : DateTime.SpecifyKind(record.GetDateTime(record.GetOrdinal(name)), DateTimeKind.Unspecified);
}
