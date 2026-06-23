namespace DBAClientX;

public partial class SqlServer
{
    private const string SqlServerMaskedColumnsSupportQuery = @"
SELECT CASE WHEN OBJECT_ID(N'sys.masked_columns') IS NULL THEN 0 ELSE 1 END;";

    private const string SqlServerColumnEncryptionSupportQuery = @"
SELECT CASE
    WHEN OBJECT_ID(N'sys.column_encryption_keys') IS NULL OR COL_LENGTH(N'sys.columns', N'encryption_type_desc') IS NULL THEN 0
    ELSE 1
END;";

    private const string SqlServerAvailabilityGroupsSupportQuery = @"
SELECT CASE
    WHEN OBJECT_ID(N'sys.availability_groups') IS NULL
      OR OBJECT_ID(N'sys.availability_replicas') IS NULL
      OR COL_LENGTH(N'sys.availability_replicas', N'replica_metadata_id') IS NULL THEN 0
    ELSE 1
END;";

    private const string SqlServerGraphEdgeConstraintsSupportQuery = @"
SELECT CASE
    WHEN OBJECT_ID(N'sys.edge_constraints') IS NULL OR OBJECT_ID(N'sys.edge_constraint_clauses') IS NULL THEN 0
    ELSE 1
END;";

    private const string SqlServerHashIndexesSupportQuery = @"
SELECT CASE WHEN OBJECT_ID(N'sys.hash_indexes') IS NULL THEN 0 ELSE 1 END;";

    private const string SqlServerFileTablesSupportQuery = @"
SELECT CASE
    WHEN OBJECT_ID(N'sys.filetables') IS NULL THEN 0
    ELSE 1
END;";

    private const string SqlServerServerTriggersSupportQuery = @"
SELECT CASE WHEN OBJECT_ID(N'sys.server_triggers') IS NULL THEN 0 ELSE 1 END;";

    private const string SqlServerServerTriggerModulesSupportQuery = @"
SELECT CASE
    WHEN OBJECT_ID(N'sys.server_triggers') IS NULL THEN 0
    WHEN OBJECT_ID(N'sys.server_sql_modules') IS NULL THEN 0
    WHEN OBJECT_ID(N'sys.server_assembly_modules') IS NULL THEN 0
    ELSE 1
END;";

    private const string SqlServerAgentCatalogSupportQuery = @"
SELECT CASE
    WHEN DB_ID(N'msdb') IS NULL THEN 0
    WHEN OBJECT_ID(N'msdb.dbo.sysjobs') IS NULL THEN 0
    ELSE 1
END;";

    private const string SqlServerModuleScriptsServerTriggerUnionToken = "{ServerTriggerModuleScripts}";

    private const string SqlServerDependenciesServerTriggerUnionToken = "{ServerTriggerDependencies}";

    private const string SqlServerDependenciesServerTriggerUnion = @"
UNION ALL
SELECT
    DependencyType = N'SqlExpression',
    ReferencingSchema = CONVERT(sysname, NULL),
    ReferencingName = trigger_info.name COLLATE DATABASE_DEFAULT,
    ReferencingType = trigger_info.type_desc COLLATE DATABASE_DEFAULT,
    ReferencedServerName = dependency.referenced_server_name COLLATE DATABASE_DEFAULT,
    ReferencedDatabaseName = dependency.referenced_database_name COLLATE DATABASE_DEFAULT,
    ReferencedSchemaName = dependency.referenced_schema_name COLLATE DATABASE_DEFAULT,
    ReferencedEntityName = dependency.referenced_entity_name COLLATE DATABASE_DEFAULT,
    ReferencedClassDescription = dependency.referenced_class_desc COLLATE DATABASE_DEFAULT,
    IsCallerDependent = CONVERT(bit, dependency.is_caller_dependent),
    IsAmbiguous = CONVERT(bit, dependency.is_ambiguous)
FROM sys.sql_expression_dependencies AS dependency
INNER JOIN sys.server_triggers AS trigger_info ON trigger_info.object_id = dependency.referencing_id
WHERE dependency.referencing_class = 13
  AND @schema IS NULL
  AND (@name IS NULL OR trigger_info.name COLLATE DATABASE_DEFAULT = @name)";

    private const string SqlServerModuleScriptsServerTriggerUnion = @"
UNION ALL
SELECT
    ScriptType = N'Module',
    SchemaName = CONVERT(sysname, NULL),
    ObjectName = trigger_info.name COLLATE DATABASE_DEFAULT,
    ObjectType = trigger_info.type_desc COLLATE DATABASE_DEFAULT,
    Script = CONCAT(
        N'SET ANSI_NULLS ', CASE WHEN module_info.uses_ansi_nulls = 1 THEN N'ON' ELSE N'OFF' END, N';', CHAR(13), CHAR(10), N'GO', CHAR(13), CHAR(10),
        N'SET QUOTED_IDENTIFIER ', CASE WHEN module_info.uses_quoted_identifier = 1 THEN N'ON' ELSE N'OFF' END, N';', CHAR(13), CHAR(10), N'GO', CHAR(13), CHAR(10),
        module_info.definition COLLATE DATABASE_DEFAULT,
        CASE WHEN trigger_info.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10) + N'DISABLE TRIGGER ' + QUOTENAME(trigger_info.name COLLATE DATABASE_DEFAULT) + N' ON ALL SERVER;' ELSE N'' END)
FROM sys.server_triggers AS trigger_info
INNER JOIN sys.server_sql_modules AS module_info ON module_info.object_id = trigger_info.object_id
WHERE module_info.definition IS NOT NULL
  AND @schema IS NULL
  AND (@name IS NULL OR trigger_info.name COLLATE DATABASE_DEFAULT = @name)
UNION ALL
SELECT
    ScriptType = N'Module',
    SchemaName = CONVERT(sysname, NULL),
    ObjectName = trigger_info.name COLLATE DATABASE_DEFAULT,
    ObjectType = trigger_info.type_desc COLLATE DATABASE_DEFAULT,
    Script = CONCAT(
        N'CREATE TRIGGER ', QUOTENAME(trigger_info.name COLLATE DATABASE_DEFAULT),
        N' ON ALL SERVER', server_trigger_options.OptionClause, N' FOR ', COALESCE(server_trigger_events.EventList, N'LOGON'),
        CHAR(13), CHAR(10), N'AS EXTERNAL NAME ',
        QUOTENAME(assembly_info.name COLLATE DATABASE_DEFAULT), N'.', QUOTENAME(assembly_module.assembly_class COLLATE DATABASE_DEFAULT), N'.', QUOTENAME(assembly_module.assembly_method COLLATE DATABASE_DEFAULT),
        CASE WHEN trigger_info.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10) + N'DISABLE TRIGGER ' + QUOTENAME(trigger_info.name COLLATE DATABASE_DEFAULT) + N' ON ALL SERVER;' ELSE N'' END)
FROM sys.server_triggers AS trigger_info
INNER JOIN sys.server_assembly_modules AS assembly_module ON assembly_module.object_id = trigger_info.object_id
INNER JOIN master.sys.assemblies AS assembly_info ON assembly_info.assembly_id = assembly_module.assembly_id
LEFT JOIN sys.server_principals AS execute_as_principal ON execute_as_principal.principal_id = assembly_module.execute_as_principal_id
OUTER APPLY (
    SELECT ExecuteAsClause = CASE
        WHEN assembly_module.execute_as_principal_id IS NULL THEN NULL
        WHEN assembly_module.execute_as_principal_id = -2 THEN N'EXECUTE AS OWNER'
        WHEN execute_as_principal.name IS NOT NULL THEN N'EXECUTE AS ''' + REPLACE(execute_as_principal.name COLLATE DATABASE_DEFAULT, N'''', N'''''') + N''''
        ELSE NULL
    END
) AS server_execute_as_info
OUTER APPLY (
    SELECT OptionClause = CASE WHEN server_execute_as_info.ExecuteAsClause IS NULL THEN N'' ELSE N' WITH ' + server_execute_as_info.ExecuteAsClause END
) AS server_trigger_options
OUTER APPLY (
    SELECT EventList = STUFF((
        SELECT N', ' + event_info.type_desc COLLATE DATABASE_DEFAULT
        FROM sys.server_trigger_events AS event_info
        WHERE event_info.object_id = trigger_info.object_id
        ORDER BY event_info.type_desc
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 2, N'')
) AS server_trigger_events
WHERE @schema IS NULL
  AND (@name IS NULL OR trigger_info.name COLLATE DATABASE_DEFAULT = @name)";

    private const string SqlServerTableScriptMaskingFunctionToken = "{MaskingFunction}";

    private const string SqlServerTableScriptMaskingJoinToken = "{MaskingJoin}";

    private const string SqlServerTableScriptEncryptionDefinitionToken = "{EncryptionDefinition}";

    private const string SqlServerTableScriptEncryptionJoinToken = "{EncryptionJoin}";

    private const string SqlServerPermissionsAvailabilityGroupNameToken = "{AvailabilityGroupName}";

    private const string SqlServerPermissionsAvailabilityGroupJoinToken = "{AvailabilityGroupJoin}";

    private const string SqlServerTableScriptGraphEdgeConstraintStatementsToken = "{GraphEdgeConstraintStatements}";

    private const string SqlServerTableScriptPrimaryKeyBucketCountToken = "{PrimaryKeyBucketCount}";

    private const string SqlServerTableScriptPrimaryKeyHashJoinToken = "{PrimaryKeyHashJoin}";

    private const string SqlServerTableScriptUniqueHashJoinToken = "{UniqueHashJoin}";

    private const string SqlServerTableScriptUniqueHashBucketCountToken = "{UniqueHashBucketCount}";

    private const string SqlServerTableScriptMemoryHashJoinToken = "{MemoryHashJoin}";

    private const string SqlServerTableScriptMemoryHashBucketCountToken = "{MemoryHashBucketCount}";

    private const string SqlServerTableScriptGraphHiddenColumnFilterToken = "{GraphHiddenColumnFilter}";

    private const string SqlServerTableScriptGraphTableOnlyRowsToken = "{GraphTableOnlyRows}";

    private const string SqlServerTableScriptFileTableOptionsToken = "{FileTableOptions}";

    private const string SqlServerTableScriptFileTableJoinToken = "{FileTableJoin}";

    private const string SqlServerTableScriptLegacyMaskingFunctionProjection = "CONVERT(nvarchar(4000), NULL)";

    private const string SqlServerTableScriptMaskingFunctionProjection = "masking_info.masking_function";

    private const string SqlServerTableScriptMaskingJoin = "LEFT JOIN sys.masked_columns AS masking_info ON masking_info.object_id = column_info.object_id AND masking_info.column_id = column_info.column_id";

    private const string SqlServerTableScriptLegacyEncryptionDefinitionProjection = "CONVERT(nvarchar(4000), NULL)";

    private const string SqlServerTableScriptEncryptionDefinitionProjection = "CASE WHEN column_info.encryption_type_desc IS NULL THEN NULL ELSE N'ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ' + QUOTENAME(encryption_key.name) + N', ENCRYPTION_TYPE = ' + column_info.encryption_type_desc + N', ALGORITHM = ''' + column_info.encryption_algorithm_name + N''')' END";

    private const string SqlServerTableScriptEncryptionJoin = "LEFT JOIN sys.column_encryption_keys AS encryption_key ON encryption_key.column_encryption_key_id = column_info.column_encryption_key_id";

    private const string SqlServerTableScriptPrimaryKeyBucketCountProjection = "primary_key_hash.bucket_count";

    private const string SqlServerTableScriptLegacyPrimaryKeyBucketCountProjection = "CONVERT(bigint, NULL)";

    private const string SqlServerTableScriptPrimaryKeyHashJoin = "LEFT JOIN sys.hash_indexes AS primary_key_hash ON primary_key_hash.object_id = primary_key.object_id AND primary_key_hash.index_id = primary_key.index_id";

    private const string SqlServerTableScriptUniqueHashJoin = "LEFT JOIN sys.hash_indexes AS unique_hash ON unique_hash.object_id = unique_index.object_id AND unique_hash.index_id = unique_index.index_id";

    private const string SqlServerTableScriptUniqueHashBucketCount = "CASE WHEN unique_index.type_desc LIKE N'%HASH%' AND unique_hash.bucket_count IS NOT NULL THEN N' WITH (BUCKET_COUNT = ' + CONVERT(nvarchar(20), unique_hash.bucket_count) + N')' ELSE N'' END";

    private const string SqlServerTableScriptMemoryHashJoin = "LEFT JOIN sys.hash_indexes AS memory_hash ON memory_hash.object_id = memory_index.object_id AND memory_hash.index_id = memory_index.index_id";

    private const string SqlServerTableScriptMemoryHashBucketCount = "CASE WHEN memory_index.type_desc LIKE N'%HASH%' AND memory_hash.bucket_count IS NOT NULL THEN N' WITH (BUCKET_COUNT = ' + CONVERT(nvarchar(20), memory_hash.bucket_count) + N')' ELSE N'' END";

    private const string SqlServerTableScriptGraphCopyColumnFilter = "  AND NOT (graph_info.graph_kind IN (N'NODE', N'EDGE') AND graph_column_info.graph_type IS NOT NULL AND graph_column_info.graph_type NOT IN (5, 8))";

    private const string SqlServerTableScriptGraphHiddenColumnFilter = "  AND NOT (graph_info.graph_kind IN (N'NODE', N'EDGE') AND graph_column_info.graph_type IS NOT NULL)";

    private const string SqlServerTableScriptLegacyFileTableOptionsProjection = "CONVERT(nvarchar(max), NULL)";

    private const string SqlServerTableScriptFileTableOptionsProjection = "filetable_info.options";

    private const string SqlServerTableScriptFileTableJoin = @"
OUTER APPLY (
    SELECT TOP (1) primary_key_name = key_info.name
    FROM sys.key_constraints AS key_info
    INNER JOIN sys.index_columns AS key_column ON key_column.object_id = key_info.parent_object_id AND key_column.index_id = key_info.unique_index_id
    INNER JOIN sys.columns AS key_column_info ON key_column_info.object_id = key_column.object_id AND key_column_info.column_id = key_column.column_id
    WHERE key_info.parent_object_id = table_info.object_id
      AND key_info.type = N'PK'
      AND key_column_info.name = N'path_locator'
    ORDER BY key_info.name
) AS filetable_primary_key
OUTER APPLY (
    SELECT TOP (1) stream_unique_name = index_info.name
    FROM sys.indexes AS index_info
    INNER JOIN sys.index_columns AS key_column ON key_column.object_id = index_info.object_id AND key_column.index_id = index_info.index_id
    INNER JOIN sys.columns AS key_column_info ON key_column_info.object_id = key_column.object_id AND key_column_info.column_id = key_column.column_id
    WHERE index_info.object_id = table_info.object_id
      AND index_info.is_unique = 1
      AND key_column_info.name = N'stream_id'
    ORDER BY index_info.name
) AS filetable_stream_unique
OUTER APPLY (
    SELECT TOP (1) fullpath_unique_name = index_info.name
    FROM sys.indexes AS index_info
    WHERE index_info.object_id = table_info.object_id
      AND index_info.is_unique = 1
      AND EXISTS (
          SELECT 1
          FROM sys.index_columns AS key_column
          INNER JOIN sys.columns AS key_column_info ON key_column_info.object_id = key_column.object_id AND key_column_info.column_id = key_column.column_id
          WHERE key_column.object_id = index_info.object_id
            AND key_column.index_id = index_info.index_id
            AND key_column_info.name = N'parent_path_locator'
      )
      AND EXISTS (
          SELECT 1
          FROM sys.index_columns AS key_column
          INNER JOIN sys.columns AS key_column_info ON key_column_info.object_id = key_column.object_id AND key_column_info.column_id = key_column.column_id
          WHERE key_column.object_id = index_info.object_id
            AND key_column.index_id = index_info.index_id
            AND key_column_info.name = N'name'
      )
    ORDER BY index_info.name
) AS filetable_fullpath_unique
OUTER APPLY (
    SELECT options = CASE WHEN option_info.Options IS NULL THEN NULL ELSE N'WITH (' + option_info.Options + N')' END
    FROM sys.filetables AS filetable_source
    OUTER APPLY (
        SELECT Options = STUFF((
            SELECT N', ' + option_value
            FROM (VALUES
                (CASE WHEN filetable_source.directory_name IS NOT NULL THEN N'FILETABLE_DIRECTORY = N''' + REPLACE(filetable_source.directory_name, N'''', N'''''') + N'''' ELSE NULL END),
                (CASE WHEN filetable_source.filename_collation_name IS NOT NULL THEN N'FILETABLE_COLLATE_FILENAME = ' + filetable_source.filename_collation_name ELSE NULL END),
                (CASE WHEN filetable_primary_key.primary_key_name IS NOT NULL THEN N'FILETABLE_PRIMARY_KEY_CONSTRAINT_NAME = ' + QUOTENAME(filetable_primary_key.primary_key_name) ELSE NULL END),
                (CASE WHEN filetable_stream_unique.stream_unique_name IS NOT NULL THEN N'FILETABLE_STREAMID_UNIQUE_CONSTRAINT_NAME = ' + QUOTENAME(filetable_stream_unique.stream_unique_name) ELSE NULL END),
                (CASE WHEN filetable_fullpath_unique.fullpath_unique_name IS NOT NULL THEN N'FILETABLE_FULLPATH_UNIQUE_CONSTRAINT_NAME = ' + QUOTENAME(filetable_fullpath_unique.fullpath_unique_name) ELSE NULL END)
            ) AS filetable_options(option_value)
            WHERE option_value IS NOT NULL
            FOR XML PATH(N''), TYPE
        ).value(N'.', N'nvarchar(max)'), 1, 2, N'')
    ) AS option_info
    WHERE filetable_source.object_id = table_info.object_id
) AS filetable_info";

    private const string SqlServerPermissionsLegacyAvailabilityGroupNameProjection = "CONVERT(nvarchar(128), permission.major_id) COLLATE DATABASE_DEFAULT";

    private const string SqlServerPermissionsAvailabilityGroupNameProjection = "COALESCE(availability_group.name, CONVERT(nvarchar(128), permission.major_id)) COLLATE DATABASE_DEFAULT";

    private const string SqlServerPermissionsAvailabilityGroupJoin = @"
LEFT JOIN (
    SELECT
        ReplicaMetadataId = availability_replica.replica_metadata_id,
        Name = availability_group.name
    FROM sys.availability_groups AS availability_group
    INNER JOIN sys.availability_replicas AS availability_replica ON availability_replica.group_id = availability_group.group_id
) AS availability_group ON availability_group.ReplicaMetadataId = permission.major_id AND permission.class_desc = N'AVAILABILITY GROUP'";

    private const string SqlServerTableScriptGraphEdgeConstraintStatements = @"
            UNION ALL
            SELECT statement =
                N'ALTER TABLE ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(table_info.name) +
                N' ADD CONSTRAINT ' + QUOTENAME(edge_constraint.name) + N' CONNECTION (' +
                STUFF((
                    SELECT N', ' + QUOTENAME(from_schema.name) + N'.' + QUOTENAME(from_table.name) + N' TO ' + QUOTENAME(to_schema.name) + N'.' + QUOTENAME(to_table.name)
                    FROM sys.edge_constraint_clauses AS edge_clause
                    INNER JOIN sys.tables AS from_table ON from_table.object_id = edge_clause.from_object_id
                    INNER JOIN sys.schemas AS from_schema ON from_schema.schema_id = from_table.schema_id
                    INNER JOIN sys.tables AS to_table ON to_table.object_id = edge_clause.to_object_id
                    INNER JOIN sys.schemas AS to_schema ON to_schema.schema_id = to_table.schema_id
                    WHERE edge_clause.object_id = edge_constraint.object_id
                    ORDER BY edge_clause.clause_number
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'nvarchar(max)'), 1, 2, N'') + N');' +
                CASE WHEN edge_constraint.is_disabled = 1 THEN
                    CHAR(30) + N'ALTER TABLE ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(table_info.name) +
                    N' NOCHECK CONSTRAINT ' + QUOTENAME(edge_constraint.name) + N';'
                ELSE N'' END
            FROM sys.edge_constraints AS edge_constraint
            WHERE edge_constraint.parent_object_id = table_info.object_id";

    private const string SqlServerTableScriptGraphTableOnlyRows = @"
UNION ALL
SELECT
    SchemaName = schema_info.name,
    TableName = table_info.name,
    ColumnName = CONVERT(sysname, N''),
    Ordinal = CONVERT(int, 0),
    DataType = CONVERT(nvarchar(4000), N''),
    IsNullable = CONVERT(bit, 1),
    IsIdentity = CONVERT(bit, 0),
    IdentitySeed = CONVERT(nvarchar(40), NULL),
    IdentityIncrement = CONVERT(nvarchar(40), NULL),
    IdentityNotForReplication = CONVERT(bit, 0),
    IsRowGuidColumn = CONVERT(bit, 0),
    DefaultConstraintName = CONVERT(sysname, NULL),
    DefaultDefinition = CONVERT(nvarchar(max), NULL),
    ComputedDefinition = CONVERT(nvarchar(max), NULL),
    IsPersisted = CONVERT(bit, 0),
    GeneratedAlwaysTypeDescription = CONVERT(nvarchar(60), NULL),
    IsHidden = CONVERT(bit, 0),
    IsSparse = CONVERT(bit, 0),
    IsColumnSet = CONVERT(bit, 0),
    GraphColumnRole = CONVERT(nvarchar(60), NULL),
    MaskingFunction = CONVERT(nvarchar(4000), NULL),
    EncryptionDefinition = CONVERT(nvarchar(4000), NULL),
    TemporalType = CONVERT(int, 0),
    LedgerType = CONVERT(int, 0),
    IsMemoryOptimized = CONVERT(bit, 0),
    DurabilityDescription = CONVERT(nvarchar(60), NULL),
    HistoryTableSchema = CONVERT(sysname, NULL),
    HistoryTableName = CONVERT(sysname, NULL),
    PrimaryKeyName = CONVERT(sysname, NULL),
    PrimaryKeyOrdinal = CONVERT(int, NULL),
    PrimaryKeyIndexType = CONVERT(nvarchar(60), NULL),
    PrimaryKeyIsDescending = CONVERT(bit, NULL),
    PrimaryKeyBucketCount = CONVERT(bigint, NULL),
    UniqueConstraintName = CONVERT(sysname, NULL),
    UniqueConstraintOrdinal = CONVERT(int, NULL),
    UniqueConstraintIndexType = CONVERT(nvarchar(60), NULL),
    UniqueConstraintIsDescending = CONVERT(bit, NULL),
    UniqueConstraintBucketCount = CONVERT(bigint, NULL),
    GraphTableKind = graph_info.graph_kind,
    FileTableOptions = CONVERT(nvarchar(max), NULL),
    AdditionalConstraintDefinitions = CONVERT(nvarchar(max), NULL),
    PostCreateStatements = graph_only_post_create_info.statements
FROM sys.tables AS table_info
INNER JOIN sys.schemas AS schema_info ON schema_info.schema_id = table_info.schema_id
OUTER APPLY (
    SELECT graph_kind = CASE
        WHEN table_metadata.data.exist(N'/table/is_node') = 1 AND table_metadata.data.value(N'(/table/is_node/text())[1]', N'bit') = 1 THEN N'NODE'
        WHEN table_metadata.data.exist(N'/table/is_edge') = 1 AND table_metadata.data.value(N'(/table/is_edge/text())[1]', N'bit') = 1 THEN N'EDGE'
        ELSE NULL
    END
    FROM (SELECT data = (SELECT table_info.* FOR XML PATH(N'table'), TYPE)) AS table_metadata
) AS graph_info
OUTER APPLY (
    SELECT statements = STUFF((
        SELECT CHAR(30) + statement
        FROM (
            SELECT statement = CONVERT(nvarchar(max), NULL)
            WHERE 1 = 0
{GraphEdgeConstraintStatements}
        ) AS table_statement
        WHERE statement IS NOT NULL
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 1, N'')
) AS graph_only_post_create_info
WHERE graph_info.graph_kind IN (N'NODE', N'EDGE')
  AND (@schema IS NULL OR schema_info.name = @schema)
  AND (@name IS NULL OR table_info.name = @name)
  AND NOT EXISTS (
      SELECT 1
      FROM sys.columns AS visible_column
      OUTER APPLY (
          SELECT graph_type = CASE
              WHEN visible_column_metadata.data.exist(N'/column/graph_type') = 1 THEN visible_column_metadata.data.value(N'(/column/graph_type/text())[1]', N'int')
              ELSE NULL
          END
          FROM (SELECT data = (SELECT visible_column.* FOR XML PATH(N'column'), TYPE)) AS visible_column_metadata
      ) AS visible_graph_column_info
      WHERE visible_column.object_id = table_info.object_id
        AND visible_graph_column_info.graph_type IS NULL
  )";

    private const string SqlServerAgentJobsManagementQuery = @"
SELECT
    JobId = j.job_id,
    Name = j.name,
    Category = c.name,
    OwnerLoginName = SUSER_SNAME(j.owner_sid),
    Description = j.description,
    Enabled = CONVERT(bit, j.enabled),
    StartStepId = j.start_step_id,
    Created = j.date_created,
    Modified = j.date_modified
FROM msdb.dbo.sysjobs AS j
LEFT JOIN msdb.dbo.syscategories AS c ON c.category_id = j.category_id
WHERE (@includeDisabled = 1 OR j.enabled = 1)
  AND (@jobName IS NULL OR j.name = @jobName)
ORDER BY j.name;";

    private const string SqlServerAgentJobStepsManagementQuery = @"
SELECT
    JobId = j.job_id,
    JobName = j.name,
    StepId = s.step_id,
    StepName = s.step_name,
    Subsystem = s.subsystem,
    Command = s.command,
    DatabaseName = s.database_name,
    OnSuccessAction = CASE s.on_success_action
        WHEN 1 THEN N'QuitWithSuccess'
        WHEN 2 THEN N'QuitWithFailure'
        WHEN 3 THEN N'GoToNextStep'
        WHEN 4 THEN N'GoToStep'
        ELSE CONVERT(nvarchar(32), s.on_success_action)
    END,
    OnSuccessStepId = s.on_success_step_id,
    OnFailAction = CASE s.on_fail_action
        WHEN 1 THEN N'QuitWithSuccess'
        WHEN 2 THEN N'QuitWithFailure'
        WHEN 3 THEN N'GoToNextStep'
        WHEN 4 THEN N'GoToStep'
        ELSE CONVERT(nvarchar(32), s.on_fail_action)
    END,
    OnFailStepId = s.on_fail_step_id,
    RetryAttempts = s.retry_attempts,
    RetryInterval = s.retry_interval
FROM msdb.dbo.sysjobsteps AS s
INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = s.job_id
WHERE (@jobName IS NULL OR j.name = @jobName)
ORDER BY j.name, s.step_id;";

    private const string SqlServerAgentSchedulesManagementQuery = @"
SELECT
    JobId = j.job_id,
    JobName = j.name,
    ScheduleId = s.schedule_id,
    Name = s.name,
    Enabled = CONVERT(bit, s.enabled),
    FrequencyType = s.freq_type,
    FrequencyInterval = s.freq_interval,
    FrequencyRelativeInterval = s.freq_relative_interval,
    FrequencySubdayType = s.freq_subday_type,
    FrequencySubdayInterval = s.freq_subday_interval,
    FrequencyRecurrenceFactor = s.freq_recurrence_factor,
    ActiveStartDate = s.active_start_date,
    ActiveEndDate = s.active_end_date,
    ActiveStartTime = s.active_start_time,
    ActiveEndTime = s.active_end_time
FROM msdb.dbo.sysschedules AS s
LEFT JOIN msdb.dbo.sysjobschedules AS js ON js.schedule_id = s.schedule_id
LEFT JOIN msdb.dbo.sysjobs AS j ON j.job_id = js.job_id
WHERE (@includeDisabled = 1 OR s.enabled = 1)
  AND (@jobName IS NULL OR j.name = @jobName)
ORDER BY ISNULL(j.name, N''), s.name;";

    private const string SqlServerServerPrincipalsManagementQuery = @"
SELECT
    Scope = N'Server',
    DatabaseName = CONVERT(nvarchar(128), NULL),
    Name = sp.name,
    Type = sp.type,
    TypeDescription = sp.type_desc,
    Sid = CONVERT(varchar(514), sp.sid, 1),
    DefaultDatabaseName = sp.default_database_name,
    DefaultSchemaName = CONVERT(nvarchar(128), NULL),
    AuthenticationType = CONVERT(nvarchar(60), NULL),
    IsDisabled = CONVERT(bit, sl.is_disabled),
    IsFixedRole = CONVERT(bit, sp.is_fixed_role),
    Created = sp.create_date,
    Modified = sp.modify_date
FROM sys.server_principals AS sp
LEFT JOIN sys.sql_logins AS sl ON sl.principal_id = sp.principal_id
WHERE (@includeSystem = 1 OR sp.name NOT LIKE N'##MS_%##')
  AND (@includeSystem = 1 OR ISNULL(sp.is_fixed_role, 0) = 0)
  AND (@name IS NULL OR sp.name COLLATE DATABASE_DEFAULT = @name)
ORDER BY sp.type_desc, sp.name;";

    private const string SqlServerDatabasePrincipalsManagementQuery = @"
SELECT
    Scope = N'Database',
    DatabaseName = DB_NAME(),
    Name = dp.name,
    Type = dp.type,
    TypeDescription = dp.type_desc,
    Sid = CONVERT(varchar(514), dp.sid, 1),
    DefaultDatabaseName = CONVERT(nvarchar(128), NULL),
    DefaultSchemaName = dp.default_schema_name,
    AuthenticationType = dp.authentication_type_desc,
    IsDisabled = CONVERT(bit, NULL),
    IsFixedRole = CONVERT(bit, dp.is_fixed_role),
    Created = dp.create_date,
    Modified = dp.modify_date
FROM sys.database_principals AS dp
WHERE (@includeSystem = 1 OR dp.name NOT IN (N'dbo', N'guest', N'INFORMATION_SCHEMA', N'sys'))
  AND (@includeSystem = 1 OR ISNULL(dp.is_fixed_role, 0) = 0)
  AND (@name IS NULL OR dp.name = @name)
ORDER BY dp.type_desc, dp.name;";

    private const string SqlServerRoleMembershipsManagementQuery = @"
SELECT
    Scope = N'Server' COLLATE DATABASE_DEFAULT,
    DatabaseName = CONVERT(nvarchar(128), NULL),
    RoleName = role_principal.name COLLATE DATABASE_DEFAULT,
    RoleTypeDescription = role_principal.type_desc COLLATE DATABASE_DEFAULT,
    MemberName = member_principal.name COLLATE DATABASE_DEFAULT,
    MemberTypeDescription = member_principal.type_desc COLLATE DATABASE_DEFAULT
FROM sys.server_role_members AS membership
INNER JOIN sys.server_principals AS role_principal ON role_principal.principal_id = membership.role_principal_id
INNER JOIN sys.server_principals AS member_principal ON member_principal.principal_id = membership.member_principal_id
WHERE (@roleName IS NULL OR role_principal.name COLLATE DATABASE_DEFAULT = @roleName)
  AND (@memberName IS NULL OR member_principal.name COLLATE DATABASE_DEFAULT = @memberName)
UNION ALL
SELECT
    Scope = N'Database' COLLATE DATABASE_DEFAULT,
    DatabaseName = DB_NAME() COLLATE DATABASE_DEFAULT,
    RoleName = role_principal.name COLLATE DATABASE_DEFAULT,
    RoleTypeDescription = role_principal.type_desc COLLATE DATABASE_DEFAULT,
    MemberName = member_principal.name COLLATE DATABASE_DEFAULT,
    MemberTypeDescription = member_principal.type_desc COLLATE DATABASE_DEFAULT
FROM sys.database_role_members AS membership
INNER JOIN sys.database_principals AS role_principal ON role_principal.principal_id = membership.role_principal_id
INNER JOIN sys.database_principals AS member_principal ON member_principal.principal_id = membership.member_principal_id
WHERE (@roleName IS NULL OR role_principal.name = @roleName)
  AND (@memberName IS NULL OR member_principal.name = @memberName)
ORDER BY Scope, DatabaseName, RoleName, MemberName;";

    private const string SqlServerPermissionsManagementQuery = @"
SELECT
    Scope = N'Server' COLLATE DATABASE_DEFAULT,
    DatabaseName = CONVERT(nvarchar(128), NULL),
    State = permission.state COLLATE DATABASE_DEFAULT,
    StateDescription = permission.state_desc COLLATE DATABASE_DEFAULT,
    PermissionName = permission.permission_name COLLATE DATABASE_DEFAULT,
    ClassDescription = permission.class_desc COLLATE DATABASE_DEFAULT,
    SecurableSchema = CONVERT(nvarchar(128), NULL),
    SecurableName = CASE permission.class_desc
        WHEN N'SERVER' THEN CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName')) COLLATE DATABASE_DEFAULT
        WHEN N'ENDPOINT' THEN endpoint.name COLLATE DATABASE_DEFAULT
        WHEN N'SERVER_PRINCIPAL' THEN target_principal.name COLLATE DATABASE_DEFAULT
        WHEN N'AVAILABILITY GROUP' THEN {AvailabilityGroupName}
        ELSE CONVERT(nvarchar(256), permission.major_id)
    END,
    SecurableColumn = CONVERT(nvarchar(128), NULL),
    GranteeName = grantee.name COLLATE DATABASE_DEFAULT,
    GrantorName = grantor.name COLLATE DATABASE_DEFAULT
FROM sys.server_permissions AS permission
INNER JOIN sys.server_principals AS grantee ON grantee.principal_id = permission.grantee_principal_id
INNER JOIN sys.server_principals AS grantor ON grantor.principal_id = permission.grantor_principal_id
LEFT JOIN sys.endpoints AS endpoint ON endpoint.endpoint_id = permission.major_id AND permission.class_desc = N'ENDPOINT'
LEFT JOIN sys.server_principals AS target_principal ON target_principal.principal_id = permission.major_id AND permission.class_desc = N'SERVER_PRINCIPAL'
{AvailabilityGroupJoin}
WHERE (@principalName IS NULL OR grantee.name COLLATE DATABASE_DEFAULT = @principalName)
UNION ALL
SELECT
    Scope = N'Database' COLLATE DATABASE_DEFAULT,
    DatabaseName = DB_NAME() COLLATE DATABASE_DEFAULT,
    State = permission.state COLLATE DATABASE_DEFAULT,
    StateDescription = permission.state_desc COLLATE DATABASE_DEFAULT,
    PermissionName = permission.permission_name COLLATE DATABASE_DEFAULT,
    ClassDescription = permission.class_desc COLLATE DATABASE_DEFAULT,
    SecurableSchema = CASE
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' THEN OBJECT_SCHEMA_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'SCHEMA' THEN SCHEMA_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'TYPE' THEN target_type_schema.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'XML_SCHEMA_COLLECTION' THEN target_xml_schema.name COLLATE DATABASE_DEFAULT
        ELSE NULL
    END,
    SecurableName = CASE
        WHEN permission.class_desc = N'DATABASE' THEN DB_NAME() COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' THEN OBJECT_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'SCHEMA' THEN SCHEMA_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'DATABASE_PRINCIPAL' THEN target_database_principal.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'TYPE' THEN target_type.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'CERTIFICATE' THEN target_certificate.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'ASYMMETRIC_KEY' THEN target_asymmetric_key.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'SYMMETRIC_KEYS' THEN target_symmetric_key.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'ASSEMBLY' THEN target_assembly.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'XML_SCHEMA_COLLECTION' THEN target_xml_collection.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'MESSAGE_TYPE' THEN target_message_type.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'SERVICE_CONTRACT' THEN target_service_contract.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'SERVICE' THEN target_service.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'REMOTE_SERVICE_BINDING' THEN target_remote_service_binding.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'ROUTE' THEN target_route.name COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'FULLTEXT_CATALOG' THEN target_fulltext_catalog.name COLLATE DATABASE_DEFAULT
        ELSE CONVERT(nvarchar(256), permission.major_id)
    END,
    SecurableColumn = CASE
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' AND permission.minor_id > 0 THEN COL_NAME(permission.major_id, permission.minor_id) COLLATE DATABASE_DEFAULT
        ELSE NULL
    END,
    GranteeName = grantee.name COLLATE DATABASE_DEFAULT,
    GrantorName = grantor.name COLLATE DATABASE_DEFAULT
FROM sys.database_permissions AS permission
INNER JOIN sys.database_principals AS grantee ON grantee.principal_id = permission.grantee_principal_id
INNER JOIN sys.database_principals AS grantor ON grantor.principal_id = permission.grantor_principal_id
LEFT JOIN sys.database_principals AS target_database_principal ON target_database_principal.principal_id = permission.major_id AND permission.class_desc = N'DATABASE_PRINCIPAL'
LEFT JOIN sys.types AS target_type ON target_type.user_type_id = permission.major_id AND permission.class_desc = N'TYPE'
LEFT JOIN sys.schemas AS target_type_schema ON target_type_schema.schema_id = target_type.schema_id
LEFT JOIN sys.certificates AS target_certificate ON target_certificate.certificate_id = permission.major_id AND permission.class_desc = N'CERTIFICATE'
LEFT JOIN sys.asymmetric_keys AS target_asymmetric_key ON target_asymmetric_key.asymmetric_key_id = permission.major_id AND permission.class_desc = N'ASYMMETRIC_KEY'
LEFT JOIN sys.symmetric_keys AS target_symmetric_key ON target_symmetric_key.symmetric_key_id = permission.major_id AND permission.class_desc = N'SYMMETRIC_KEYS'
LEFT JOIN sys.assemblies AS target_assembly ON target_assembly.assembly_id = permission.major_id AND permission.class_desc = N'ASSEMBLY'
LEFT JOIN sys.xml_schema_collections AS target_xml_collection ON target_xml_collection.xml_collection_id = permission.major_id AND permission.class_desc = N'XML_SCHEMA_COLLECTION'
LEFT JOIN sys.schemas AS target_xml_schema ON target_xml_schema.schema_id = target_xml_collection.schema_id
LEFT JOIN sys.service_message_types AS target_message_type ON target_message_type.message_type_id = permission.major_id AND permission.class_desc = N'MESSAGE_TYPE'
LEFT JOIN sys.service_contracts AS target_service_contract ON target_service_contract.service_contract_id = permission.major_id AND permission.class_desc = N'SERVICE_CONTRACT'
LEFT JOIN sys.services AS target_service ON target_service.service_id = permission.major_id AND permission.class_desc = N'SERVICE'
LEFT JOIN sys.remote_service_bindings AS target_remote_service_binding ON target_remote_service_binding.remote_service_binding_id = permission.major_id AND permission.class_desc = N'REMOTE_SERVICE_BINDING'
LEFT JOIN sys.routes AS target_route ON target_route.route_id = permission.major_id AND permission.class_desc = N'ROUTE'
LEFT JOIN sys.fulltext_catalogs AS target_fulltext_catalog ON target_fulltext_catalog.fulltext_catalog_id = permission.major_id AND permission.class_desc = N'FULLTEXT_CATALOG'
WHERE (@principalName IS NULL OR grantee.name COLLATE DATABASE_DEFAULT = @principalName)
ORDER BY Scope, DatabaseName, GranteeName, PermissionName, ClassDescription, SecurableName, SecurableColumn;";

    private const string SqlServerInstancePropertiesManagementQuery = @"
SELECT Name, Value
FROM
(
    VALUES
        (N'MachineName', CONVERT(nvarchar(4000), SERVERPROPERTY(N'MachineName'))),
        (N'ServerName', CONVERT(nvarchar(4000), SERVERPROPERTY(N'ServerName'))),
        (N'InstanceName', CONVERT(nvarchar(4000), SERVERPROPERTY(N'InstanceName'))),
        (N'ProductVersion', CONVERT(nvarchar(4000), SERVERPROPERTY(N'ProductVersion'))),
        (N'ProductLevel', CONVERT(nvarchar(4000), SERVERPROPERTY(N'ProductLevel'))),
        (N'Edition', CONVERT(nvarchar(4000), SERVERPROPERTY(N'Edition'))),
        (N'EngineEdition', CONVERT(nvarchar(4000), SERVERPROPERTY(N'EngineEdition'))),
        (N'Collation', CONVERT(nvarchar(4000), SERVERPROPERTY(N'Collation'))),
        (N'IsClustered', CONVERT(nvarchar(4000), SERVERPROPERTY(N'IsClustered'))),
        (N'IsHadrEnabled', CONVERT(nvarchar(4000), SERVERPROPERTY(N'IsHadrEnabled')))
) AS properties(Name, Value)
WHERE (@name IS NULL OR Name = @name)
ORDER BY Name;";

    private const string SqlServerConfigurationsManagementQuery = @"
SELECT
    Name = name,
    Value = CONVERT(int, value),
    ValueInUse = CONVERT(int, value_in_use),
    Minimum = CONVERT(int, minimum),
    Maximum = CONVERT(int, maximum),
    IsDynamic = CONVERT(bit, is_dynamic),
    IsAdvanced = CONVERT(bit, is_advanced),
    Description = description
FROM sys.configurations
WHERE (@includeAdvanced = 1 OR is_advanced = 0)
  AND (@name IS NULL OR name = @name)
ORDER BY name;";

    private const string SqlServerDependenciesManagementQuery = @"
SELECT
    DependencyType = N'SqlExpression',
    ReferencingSchema = referencing_schema.name,
    ReferencingName = referencing_object.name,
    ReferencingType = referencing_object.type_desc,
    ReferencedServerName = dependency.referenced_server_name,
    ReferencedDatabaseName = dependency.referenced_database_name,
    ReferencedSchemaName = dependency.referenced_schema_name,
    ReferencedEntityName = dependency.referenced_entity_name,
    ReferencedClassDescription = dependency.referenced_class_desc,
    IsCallerDependent = CONVERT(bit, dependency.is_caller_dependent),
    IsAmbiguous = CONVERT(bit, dependency.is_ambiguous)
FROM sys.sql_expression_dependencies AS dependency
INNER JOIN sys.objects AS referencing_object ON referencing_object.object_id = dependency.referencing_id
INNER JOIN sys.schemas AS referencing_schema ON referencing_schema.schema_id = referencing_object.schema_id
WHERE dependency.referencing_class = 1
  AND (@schema IS NULL OR referencing_schema.name = @schema)
  AND (@name IS NULL OR referencing_object.name = @name)
UNION ALL
SELECT
    DependencyType = N'SqlExpression',
    ReferencingSchema = CONVERT(sysname, NULL),
    ReferencingName = trigger_info.name,
    ReferencingType = trigger_info.type_desc,
    ReferencedServerName = dependency.referenced_server_name,
    ReferencedDatabaseName = dependency.referenced_database_name,
    ReferencedSchemaName = dependency.referenced_schema_name,
    ReferencedEntityName = dependency.referenced_entity_name,
    ReferencedClassDescription = dependency.referenced_class_desc,
    IsCallerDependent = CONVERT(bit, dependency.is_caller_dependent),
    IsAmbiguous = CONVERT(bit, dependency.is_ambiguous)
FROM sys.sql_expression_dependencies AS dependency
INNER JOIN sys.triggers AS trigger_info ON trigger_info.object_id = dependency.referencing_id
WHERE dependency.referencing_class = 12
  AND trigger_info.parent_class = 0
  AND @schema IS NULL
  AND (@name IS NULL OR trigger_info.name COLLATE DATABASE_DEFAULT = @name)
{ServerTriggerDependencies}
UNION ALL
SELECT
    DependencyType = N'ForeignKey',
    ReferencingSchema = parent_schema.name,
    ReferencingName = parent_table.name,
    ReferencingType = parent_table.type_desc,
    ReferencedServerName = CONVERT(nvarchar(128), NULL),
    ReferencedDatabaseName = CONVERT(nvarchar(128), NULL),
    ReferencedSchemaName = referenced_schema.name,
    ReferencedEntityName = referenced_table.name,
    ReferencedClassDescription = N'OBJECT_OR_COLUMN',
    IsCallerDependent = CONVERT(bit, 0),
    IsAmbiguous = CONVERT(bit, 0)
FROM sys.foreign_keys AS foreign_key
INNER JOIN sys.tables AS parent_table ON parent_table.object_id = foreign_key.parent_object_id
INNER JOIN sys.schemas AS parent_schema ON parent_schema.schema_id = parent_table.schema_id
INNER JOIN sys.tables AS referenced_table ON referenced_table.object_id = foreign_key.referenced_object_id
INNER JOIN sys.schemas AS referenced_schema ON referenced_schema.schema_id = referenced_table.schema_id
WHERE (@schema IS NULL OR parent_schema.name = @schema)
  AND (@name IS NULL OR parent_table.name = @name)
ORDER BY ReferencingSchema, ReferencingName, DependencyType, ReferencedSchemaName, ReferencedEntityName;";

    private const string SqlServerModuleScriptsManagementQuery = @"
SELECT
    ScriptType = N'Module',
    SchemaName = schema_info.name,
    ObjectName = object_info.name,
    ObjectType = object_info.type_desc,
    Script = CONCAT(
        N'SET ANSI_NULLS ', CASE WHEN module_info.uses_ansi_nulls = 1 THEN N'ON' ELSE N'OFF' END, N';', CHAR(13), CHAR(10), N'GO', CHAR(13), CHAR(10),
        N'SET QUOTED_IDENTIFIER ', CASE WHEN module_info.uses_quoted_identifier = 1 THEN N'ON' ELSE N'OFF' END, N';', CHAR(13), CHAR(10), N'GO', CHAR(13), CHAR(10),
        module_info.definition,
        CASE WHEN trigger_info.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10) + N'DISABLE TRIGGER ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name) + N' ON ' + QUOTENAME(parent_schema.name) + N'.' + QUOTENAME(parent_object.name) + N';' ELSE N'' END)
FROM sys.objects AS object_info
INNER JOIN sys.schemas AS schema_info ON schema_info.schema_id = object_info.schema_id
INNER JOIN sys.sql_modules AS module_info ON module_info.object_id = object_info.object_id
LEFT JOIN sys.triggers AS trigger_info ON trigger_info.object_id = object_info.object_id AND trigger_info.parent_class = 1
LEFT JOIN sys.objects AS parent_object ON parent_object.object_id = trigger_info.parent_id
LEFT JOIN sys.schemas AS parent_schema ON parent_schema.schema_id = parent_object.schema_id
WHERE object_info.type IN ('P', 'X', 'V', 'TR', 'FN', 'IF', 'TF')
  AND module_info.definition IS NOT NULL
  AND (@schema IS NULL OR schema_info.name = @schema)
  AND (@name IS NULL OR object_info.name = @name)
UNION ALL
SELECT
    ScriptType = N'Module',
    SchemaName = CONVERT(sysname, NULL),
    ObjectName = trigger_info.name,
    ObjectType = trigger_info.type_desc,
    Script = CONCAT(
        N'SET ANSI_NULLS ', CASE WHEN module_info.uses_ansi_nulls = 1 THEN N'ON' ELSE N'OFF' END, N';', CHAR(13), CHAR(10), N'GO', CHAR(13), CHAR(10),
        N'SET QUOTED_IDENTIFIER ', CASE WHEN module_info.uses_quoted_identifier = 1 THEN N'ON' ELSE N'OFF' END, N';', CHAR(13), CHAR(10), N'GO', CHAR(13), CHAR(10),
        module_info.definition,
        CASE WHEN trigger_info.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10) + N'DISABLE TRIGGER ' + QUOTENAME(trigger_info.name) + N' ON DATABASE;' ELSE N'' END)
FROM sys.triggers AS trigger_info
INNER JOIN sys.sql_modules AS module_info ON module_info.object_id = trigger_info.object_id
WHERE trigger_info.parent_class = 0
  AND module_info.definition IS NOT NULL
  AND @schema IS NULL
  AND (@name IS NULL OR trigger_info.name COLLATE DATABASE_DEFAULT = @name)
UNION ALL
SELECT
    ScriptType = N'Module',
    SchemaName = CONVERT(sysname, NULL),
    ObjectName = trigger_info.name,
    ObjectType = trigger_info.type_desc,
    Script = CONCAT(
        N'CREATE TRIGGER ', QUOTENAME(trigger_info.name),
        N' ON DATABASE', database_trigger_options.OptionClause,
        N' FOR ', COALESCE(database_trigger_events.EventList, N'DDL_DATABASE_LEVEL_EVENTS'),
        CHAR(13), CHAR(10), N'AS EXTERNAL NAME ',
        QUOTENAME(assembly_info.name), N'.', QUOTENAME(assembly_module.assembly_class), N'.', QUOTENAME(assembly_module.assembly_method),
        CASE WHEN trigger_info.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10) + N'DISABLE TRIGGER ' + QUOTENAME(trigger_info.name) + N' ON DATABASE;' ELSE N'' END)
FROM sys.triggers AS trigger_info
INNER JOIN sys.assembly_modules AS assembly_module ON assembly_module.object_id = trigger_info.object_id
INNER JOIN sys.assemblies AS assembly_info ON assembly_info.assembly_id = assembly_module.assembly_id
LEFT JOIN sys.database_principals AS execute_as_principal ON execute_as_principal.principal_id = assembly_module.execute_as_principal_id
OUTER APPLY (
    SELECT ExecuteAsClause = CASE
        WHEN assembly_module.execute_as_principal_id IS NULL THEN NULL
        WHEN assembly_module.execute_as_principal_id = -2 THEN N'EXECUTE AS OWNER'
        WHEN execute_as_principal.name IS NOT NULL THEN N'EXECUTE AS N''' + REPLACE(execute_as_principal.name, N'''', N'''''') + N''''
        ELSE NULL
    END
) AS database_execute_as_info
OUTER APPLY (
    SELECT OptionClause = CASE WHEN database_execute_as_info.ExecuteAsClause IS NULL THEN N'' ELSE N' WITH ' + database_execute_as_info.ExecuteAsClause END
) AS database_trigger_options
OUTER APPLY (
    SELECT EventList = STUFF((
        SELECT N', ' + event_info.type_desc
        FROM sys.trigger_events AS event_info
        WHERE event_info.object_id = trigger_info.object_id
        ORDER BY event_info.type_desc
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 2, N'')
) AS database_trigger_events
WHERE trigger_info.parent_class = 0
  AND @schema IS NULL
  AND (@name IS NULL OR trigger_info.name COLLATE DATABASE_DEFAULT = @name)
UNION ALL
SELECT
    ScriptType = N'Module',
    SchemaName = schema_info.name,
    ObjectName = object_info.name,
    ObjectType = object_info.type_desc,
    Script = CONCAT(
        CASE object_info.type
            WHEN N'PC' THEN N'CREATE PROCEDURE ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name) + CASE WHEN parameter_info.ParameterList IS NULL THEN N'' ELSE N' ' + parameter_info.ParameterList END + clr_options.OptionClause
            WHEN N'FS' THEN N'CREATE FUNCTION ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name) + N'(' + COALESCE(parameter_info.ParameterList, N'') + N')' + CHAR(13) + CHAR(10) + N'RETURNS ' + COALESCE(return_type_info.DataType, N'sql_variant') + clr_options.OptionClause
            WHEN N'FT' THEN N'CREATE FUNCTION ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name) + N'(' + COALESCE(parameter_info.ParameterList, N'') + N')' + CHAR(13) + CHAR(10) + N'RETURNS TABLE (' + COALESCE(table_return_info.TableDefinition, N'') + N')' + clr_options.OptionClause
            WHEN N'TA' THEN N'CREATE TRIGGER ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name) + N' ON ' + QUOTENAME(clr_parent_schema.name) + N'.' + QUOTENAME(clr_parent_object.name) + clr_options.OptionClause + CASE WHEN clr_trigger.is_instead_of_trigger = 1 THEN N' INSTEAD OF ' ELSE N' FOR ' END + COALESCE(clr_trigger_events.EventList, N'INSERT') + CASE WHEN clr_trigger.is_not_for_replication = 1 THEN N' NOT FOR REPLICATION' ELSE N'' END
            ELSE N'CREATE ' + object_info.type_desc + N' ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name)
        END,
        CHAR(13), CHAR(10), N'AS EXTERNAL NAME ',
        QUOTENAME(assembly_info.name), N'.', QUOTENAME(assembly_module.assembly_class), N'.', QUOTENAME(assembly_module.assembly_method),
        CASE WHEN object_info.type = N'TA' AND clr_trigger.is_disabled = 1 THEN CHAR(13) + CHAR(10) + N'GO' + CHAR(13) + CHAR(10) + N'DISABLE TRIGGER ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name) + N' ON ' + QUOTENAME(clr_parent_schema.name) + N'.' + QUOTENAME(clr_parent_object.name) + N';' ELSE N'' END)
FROM sys.objects AS object_info
INNER JOIN sys.schemas AS schema_info ON schema_info.schema_id = object_info.schema_id
INNER JOIN sys.assembly_modules AS assembly_module ON assembly_module.object_id = object_info.object_id
INNER JOIN sys.assemblies AS assembly_info ON assembly_info.assembly_id = assembly_module.assembly_id
LEFT JOIN sys.triggers AS clr_trigger ON clr_trigger.object_id = object_info.object_id AND clr_trigger.parent_class = 1
LEFT JOIN sys.objects AS clr_parent_object ON clr_parent_object.object_id = clr_trigger.parent_id
LEFT JOIN sys.schemas AS clr_parent_schema ON clr_parent_schema.schema_id = clr_parent_object.schema_id
LEFT JOIN sys.database_principals AS execute_as_principal ON execute_as_principal.principal_id = assembly_module.execute_as_principal_id
OUTER APPLY (
    SELECT ExecuteAsClause = CASE
        WHEN assembly_module.execute_as_principal_id IS NULL THEN NULL
        WHEN assembly_module.execute_as_principal_id = -2 THEN N'EXECUTE AS OWNER'
        WHEN execute_as_principal.name IS NOT NULL THEN N'EXECUTE AS N''' + REPLACE(execute_as_principal.name, N'''', N'''''') + N''''
        ELSE NULL
    END
) AS execute_as_info
OUTER APPLY (
    SELECT OptionClause = CASE WHEN option_info.Options IS NULL THEN N'' ELSE N' WITH ' + option_info.Options END
    FROM (
        SELECT Options = STUFF((
            SELECT N', ' + option_value
            FROM (VALUES
                (execute_as_info.ExecuteAsClause),
                (CASE WHEN object_info.type = N'FS' AND assembly_module.null_on_null_input = 1 THEN N'RETURNS NULL ON NULL INPUT' ELSE NULL END)
            ) AS options(option_value)
            WHERE option_value IS NOT NULL
            FOR XML PATH(N''), TYPE
        ).value(N'.', N'nvarchar(max)'), 1, 2, N'')
    ) AS option_info
) AS clr_options
OUTER APPLY (
    SELECT EventList = STUFF((
        SELECT N', ' + event_info.type_desc
        FROM sys.trigger_events AS event_info
        WHERE event_info.object_id = object_info.object_id
        ORDER BY event_info.type_desc
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 2, N'')
) AS clr_trigger_events
OUTER APPLY (
    SELECT ParameterList = STUFF((
        SELECT N', ' + parameter_item.name + N' ' + CASE
            WHEN parameter_type.is_user_defined = 1 THEN QUOTENAME(parameter_type_schema.name) + N'.' + QUOTENAME(parameter_type.name)
            WHEN parameter_type.name IN (N'varchar', N'char', N'varbinary', N'binary') THEN parameter_type.name + N'(' + CASE WHEN parameter_item.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), parameter_item.max_length) END + N')'
            WHEN parameter_type.name IN (N'nvarchar', N'nchar') THEN parameter_type.name + N'(' + CASE WHEN parameter_item.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), parameter_item.max_length / 2) END + N')'
            WHEN parameter_type.name IN (N'decimal', N'numeric') THEN parameter_type.name + N'(' + CONVERT(nvarchar(12), parameter_item.precision) + N',' + CONVERT(nvarchar(12), parameter_item.scale) + N')'
            WHEN parameter_type.name IN (N'datetime2', N'datetimeoffset', N'time') THEN parameter_type.name + N'(' + CONVERT(nvarchar(12), parameter_item.scale) + N')'
            ELSE parameter_type.name
        END + CASE WHEN parameter_item.has_default_value = 1 THEN N' = ' + CASE
            WHEN parameter_item.default_value IS NULL THEN N'NULL'
            WHEN CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(parameter_item.default_value, N'BaseType')) IN (N'varchar', N'nvarchar', N'char', N'nchar', N'xml', N'uniqueidentifier', N'date', N'datetime', N'datetime2', N'datetimeoffset', N'time', N'smalldatetime') THEN N'N''' + REPLACE(CONVERT(nvarchar(max), parameter_item.default_value), N'''', N'''''') + N''''
            WHEN CONVERT(nvarchar(128), SQL_VARIANT_PROPERTY(parameter_item.default_value, N'BaseType')) = N'bit' THEN CASE WHEN CONVERT(bit, parameter_item.default_value) = 1 THEN N'1' ELSE N'0' END
            ELSE CONVERT(nvarchar(max), parameter_item.default_value)
        END ELSE N'' END + CASE WHEN parameter_item.is_output = 1 THEN N' OUTPUT' ELSE N'' END
        FROM sys.parameters AS parameter_item
        INNER JOIN sys.types AS parameter_type ON parameter_type.user_type_id = parameter_item.user_type_id
        LEFT JOIN sys.schemas AS parameter_type_schema ON parameter_type_schema.schema_id = parameter_type.schema_id
        WHERE parameter_item.object_id = object_info.object_id
          AND parameter_item.parameter_id > 0
        ORDER BY parameter_item.parameter_id
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 2, N'')
) AS parameter_info
LEFT JOIN sys.parameters AS return_parameter ON return_parameter.object_id = object_info.object_id AND return_parameter.parameter_id = 0
LEFT JOIN sys.types AS return_type ON return_type.user_type_id = return_parameter.user_type_id
LEFT JOIN sys.schemas AS return_type_schema ON return_type_schema.schema_id = return_type.schema_id
OUTER APPLY (
    SELECT DataType = CASE
        WHEN return_type.is_user_defined = 1 THEN QUOTENAME(return_type_schema.name) + N'.' + QUOTENAME(return_type.name)
        WHEN return_type.name IN (N'varchar', N'char', N'varbinary', N'binary') THEN return_type.name + N'(' + CASE WHEN return_parameter.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), return_parameter.max_length) END + N')'
        WHEN return_type.name IN (N'nvarchar', N'nchar') THEN return_type.name + N'(' + CASE WHEN return_parameter.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), return_parameter.max_length / 2) END + N')'
        WHEN return_type.name IN (N'decimal', N'numeric') THEN return_type.name + N'(' + CONVERT(nvarchar(12), return_parameter.precision) + N',' + CONVERT(nvarchar(12), return_parameter.scale) + N')'
        WHEN return_type.name IN (N'datetime2', N'datetimeoffset', N'time') THEN return_type.name + N'(' + CONVERT(nvarchar(12), return_parameter.scale) + N')'
        ELSE return_type.name
    END
) AS return_type_info
OUTER APPLY (
    SELECT TableDefinition = STUFF((
        SELECT N', ' + QUOTENAME(column_item.name) + N' ' + CASE
            WHEN column_type.is_user_defined = 1 THEN QUOTENAME(column_type_schema.name) + N'.' + QUOTENAME(column_type.name)
            WHEN column_type.name IN (N'varchar', N'char', N'varbinary', N'binary') THEN column_type.name + N'(' + CASE WHEN column_item.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), column_item.max_length) END + N')'
            WHEN column_type.name IN (N'nvarchar', N'nchar') THEN column_type.name + N'(' + CASE WHEN column_item.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), column_item.max_length / 2) END + N')'
            WHEN column_type.name IN (N'decimal', N'numeric') THEN column_type.name + N'(' + CONVERT(nvarchar(12), column_item.precision) + N',' + CONVERT(nvarchar(12), column_item.scale) + N')'
            WHEN column_type.name IN (N'datetime2', N'datetimeoffset', N'time') THEN column_type.name + N'(' + CONVERT(nvarchar(12), column_item.scale) + N')'
            ELSE column_type.name
        END
        FROM sys.columns AS column_item
        INNER JOIN sys.types AS column_type ON column_type.user_type_id = column_item.user_type_id
        LEFT JOIN sys.schemas AS column_type_schema ON column_type_schema.schema_id = column_type.schema_id
        WHERE column_item.object_id = object_info.object_id
        ORDER BY column_item.column_id
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 2, N'')
) AS table_return_info
WHERE object_info.type IN ('PC', 'FS', 'FT', 'TA')
  AND (@schema IS NULL OR schema_info.name = @schema)
  AND (@name IS NULL OR object_info.name = @name)
{ServerTriggerModuleScripts}
ORDER BY SchemaName, ObjectName;";

    private const string SqlServerTableScriptColumnsManagementQuery = @"
SELECT
    SchemaName = schema_info.name,
    TableName = table_info.name,
    ColumnName = CASE
        WHEN graph_column_info.graph_column_role IN (N'$from_id', N'$to_id') THEN graph_column_info.graph_column_role
        ELSE column_info.name
    END,
    Ordinal = column_info.column_id,
    DataType = CASE
        WHEN type_info.name = N'vector' AND vector_info.vector_dimensions IS NOT NULL THEN N'vector(' + CONVERT(nvarchar(12), vector_info.vector_dimensions) + CASE WHEN vector_info.vector_base_type_desc IS NOT NULL AND vector_info.vector_base_type_desc <> N'FLOAT32' THEN N', ' + LOWER(vector_info.vector_base_type_desc) ELSE N'' END + N')'
        WHEN type_info.is_user_defined = 1 THEN QUOTENAME(type_schema.name) + N'.' + QUOTENAME(type_info.name)
        WHEN type_info.name = N'xml' AND column_info.xml_collection_id <> 0 THEN N'xml(' + CASE WHEN column_info.is_xml_document = 1 THEN N'DOCUMENT ' ELSE N'CONTENT ' END + QUOTENAME(xml_schema.name) + N'.' + QUOTENAME(xml_collection.name) + N')'
        WHEN type_info.name = N'xml' THEN N'xml'
        WHEN type_info.name IN (N'varchar', N'char', N'varbinary', N'binary') THEN type_info.name + N'(' + CASE WHEN column_info.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), column_info.max_length) END + N')'
        WHEN type_info.name IN (N'nvarchar', N'nchar') THEN type_info.name + N'(' + CASE WHEN column_info.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), column_info.max_length / 2) END + N')'
        WHEN type_info.name IN (N'decimal', N'numeric') THEN type_info.name + N'(' + CONVERT(nvarchar(12), column_info.precision) + N',' + CONVERT(nvarchar(12), column_info.scale) + N')'
        WHEN type_info.name = N'float' THEN type_info.name + N'(' + CONVERT(nvarchar(12), column_info.precision) + N')'
        WHEN type_info.name IN (N'datetime2', N'datetimeoffset', N'time') THEN type_info.name + N'(' + CONVERT(nvarchar(12), column_info.scale) + N')'
        ELSE type_info.name
    END + CASE
        WHEN type_info.name IN (N'varchar', N'char', N'nvarchar', N'nchar') AND column_info.collation_name IS NOT NULL THEN N' COLLATE ' + column_info.collation_name
        ELSE N''
    END + CASE
        WHEN column_info.is_filestream = 1 THEN N' FILESTREAM'
        ELSE N''
    END,
    IsNullable = CONVERT(bit, column_info.is_nullable),
    IsIdentity = CONVERT(bit, column_info.is_identity),
    IdentitySeed = CONVERT(nvarchar(40), identity_info.seed_value),
    IdentityIncrement = CONVERT(nvarchar(40), identity_info.increment_value),
    IdentityNotForReplication = CONVERT(bit, ISNULL(identity_info.is_not_for_replication, 0)),
    IsRowGuidColumn = CONVERT(bit, column_info.is_rowguidcol),
    DefaultConstraintName = default_info.name,
    DefaultDefinition = default_info.definition,
    ComputedDefinition = computed_info.definition,
    IsPersisted = CONVERT(bit, ISNULL(computed_info.is_persisted, 0)),
    GeneratedAlwaysTypeDescription = CASE CONVERT(int, ISNULL(COLUMNPROPERTY(column_info.object_id, column_info.name, N'GeneratedAlwaysType'), 0))
        WHEN 1 THEN N'AS_ROW_START'
        WHEN 2 THEN N'AS_ROW_END'
        WHEN 5 THEN N'AS_TRANSACTION_ID_START'
        WHEN 6 THEN N'AS_TRANSACTION_ID_END'
        WHEN 7 THEN N'AS_SEQUENCE_NUMBER_START'
        WHEN 8 THEN N'AS_SEQUENCE_NUMBER_END'
        ELSE NULL
    END,
    IsHidden = CONVERT(bit, ISNULL(COLUMNPROPERTY(column_info.object_id, column_info.name, N'IsHidden'), 0)),
    IsSparse = CONVERT(bit, column_info.is_sparse),
    IsColumnSet = CONVERT(bit, column_info.is_column_set),
    GraphColumnRole = graph_column_info.graph_column_role,
    MaskingFunction = {MaskingFunction},
    EncryptionDefinition = {EncryptionDefinition},
    TemporalType = temporal_info.temporal_type,
    LedgerType = ledger_info.ledger_type,
    IsMemoryOptimized = memory_info.is_memory_optimized,
    DurabilityDescription = memory_info.durability_desc,
    HistoryTableSchema = history_schema.name,
    HistoryTableName = history_table.name,
    PrimaryKeyName = primary_key.name,
    PrimaryKeyOrdinal = primary_key_column.key_ordinal,
    PrimaryKeyIndexType = primary_key.type_desc,
    PrimaryKeyIsDescending = CONVERT(bit, primary_key_column.is_descending_key),
    PrimaryKeyBucketCount = {PrimaryKeyBucketCount},
    UniqueConstraintName = CONVERT(sysname, NULL),
    UniqueConstraintOrdinal = CONVERT(int, NULL),
    UniqueConstraintIndexType = CONVERT(nvarchar(60), NULL),
    UniqueConstraintIsDescending = CONVERT(bit, NULL),
    UniqueConstraintBucketCount = CONVERT(bigint, NULL),
    GraphTableKind = graph_info.graph_kind,
    FileTableOptions = {FileTableOptions},
    AdditionalConstraintDefinitions = constraint_info.definitions,
    PostCreateStatements = post_create_info.statements
FROM sys.tables AS table_info
INNER JOIN sys.schemas AS schema_info ON schema_info.schema_id = table_info.schema_id
INNER JOIN sys.columns AS column_info ON column_info.object_id = table_info.object_id
INNER JOIN sys.types AS type_info ON type_info.user_type_id = column_info.user_type_id
INNER JOIN sys.schemas AS type_schema ON type_schema.schema_id = type_info.schema_id
LEFT JOIN sys.xml_schema_collections AS xml_collection ON xml_collection.xml_collection_id = column_info.xml_collection_id AND column_info.xml_collection_id <> 0
LEFT JOIN sys.schemas AS xml_schema ON xml_schema.schema_id = xml_collection.schema_id
LEFT JOIN sys.identity_columns AS identity_info ON identity_info.object_id = column_info.object_id AND identity_info.column_id = column_info.column_id
LEFT JOIN sys.default_constraints AS default_info ON default_info.object_id = column_info.default_object_id
LEFT JOIN sys.computed_columns AS computed_info ON computed_info.object_id = column_info.object_id AND computed_info.column_id = column_info.column_id
OUTER APPLY (
    SELECT
        vector_dimensions = CASE
            WHEN column_metadata.data.exist(N'/column/vector_dimensions') = 1 THEN column_metadata.data.value(N'(/column/vector_dimensions/text())[1]', N'int')
            ELSE NULL
        END,
        vector_base_type_desc = CASE
            WHEN column_metadata.data.exist(N'/column/vector_base_type_desc') = 1 THEN column_metadata.data.value(N'(/column/vector_base_type_desc/text())[1]', N'nvarchar(60)')
            ELSE NULL
        END
    FROM (SELECT data = (SELECT column_info.* FOR XML PATH(N'column'), TYPE)) AS column_metadata
) AS vector_info
{MaskingJoin}
{EncryptionJoin}
LEFT JOIN sys.indexes AS primary_key ON primary_key.object_id = table_info.object_id AND primary_key.is_primary_key = 1
LEFT JOIN sys.index_columns AS primary_key_column ON primary_key_column.object_id = primary_key.object_id AND primary_key_column.index_id = primary_key.index_id AND primary_key_column.column_id = column_info.column_id
{PrimaryKeyHashJoin}
OUTER APPLY (
    SELECT
        temporal_type = CASE
            WHEN table_metadata.data.exist(N'/table/temporal_type') = 1 THEN table_metadata.data.value(N'(/table/temporal_type/text())[1]', N'int')
            ELSE 0
        END,
        history_table_id = CASE
            WHEN table_metadata.data.exist(N'/table/history_table_id') = 1 THEN table_metadata.data.value(N'(/table/history_table_id/text())[1]', N'int')
            ELSE NULL
        END
    FROM (SELECT data = (SELECT table_info.* FOR XML PATH(N'table'), TYPE)) AS table_metadata
) AS temporal_info
LEFT JOIN sys.tables AS history_table ON history_table.object_id = temporal_info.history_table_id
LEFT JOIN sys.schemas AS history_schema ON history_schema.schema_id = history_table.schema_id
OUTER APPLY (
    SELECT ledger_type = CASE
        WHEN table_metadata.data.exist(N'/table/ledger_type') = 1 THEN table_metadata.data.value(N'(/table/ledger_type/text())[1]', N'int')
        ELSE 0
    END
    FROM (SELECT data = (SELECT table_info.* FOR XML PATH(N'table'), TYPE)) AS table_metadata
) AS ledger_info
OUTER APPLY (
    SELECT
        is_memory_optimized = CASE
            WHEN table_metadata.data.exist(N'/table/is_memory_optimized') = 1 THEN table_metadata.data.value(N'(/table/is_memory_optimized/text())[1]', N'bit')
            ELSE CONVERT(bit, 0)
        END,
        durability_desc = CASE
            WHEN table_metadata.data.exist(N'/table/durability_desc') = 1 THEN table_metadata.data.value(N'(/table/durability_desc/text())[1]', N'nvarchar(60)')
            ELSE NULL
        END
    FROM (SELECT data = (SELECT table_info.* FOR XML PATH(N'table'), TYPE)) AS table_metadata
) AS memory_info
OUTER APPLY (
    SELECT graph_kind = CASE
        WHEN table_metadata.data.exist(N'/table/is_filetable') = 1 AND table_metadata.data.value(N'(/table/is_filetable/text())[1]', N'bit') = 1 THEN N'FILETABLE'
        WHEN table_metadata.data.exist(N'/table/is_node') = 1 AND table_metadata.data.value(N'(/table/is_node/text())[1]', N'bit') = 1 THEN N'NODE'
        WHEN table_metadata.data.exist(N'/table/is_edge') = 1 AND table_metadata.data.value(N'(/table/is_edge/text())[1]', N'bit') = 1 THEN N'EDGE'
        ELSE NULL
    END
    FROM (SELECT data = (SELECT table_info.* FOR XML PATH(N'table'), TYPE)) AS table_metadata
) AS graph_info
OUTER APPLY (
    SELECT
        graph_type = CASE
            WHEN column_metadata.data.exist(N'/column/graph_type') = 1 THEN column_metadata.data.value(N'(/column/graph_type/text())[1]', N'int')
            ELSE NULL
        END,
        graph_column_role = CASE
            WHEN column_metadata.data.exist(N'/column/graph_type') = 0 THEN NULL
            WHEN column_metadata.data.value(N'(/column/graph_type/text())[1]', N'int') = 5 THEN N'$from_id'
            WHEN column_metadata.data.value(N'(/column/graph_type/text())[1]', N'int') = 8 THEN N'$to_id'
            WHEN column_metadata.data.value(N'(/column/graph_type/text())[1]', N'int') = 2 AND graph_info.graph_kind = N'NODE' THEN N'$node_id'
            WHEN column_metadata.data.value(N'(/column/graph_type/text())[1]', N'int') = 2 AND graph_info.graph_kind = N'EDGE' THEN N'$edge_id'
            ELSE NULL
        END
    FROM (SELECT data = (SELECT column_info.* FOR XML PATH(N'column'), TYPE)) AS column_metadata
) AS graph_column_info
{FileTableJoin}
OUTER APPLY (
    SELECT is_external = CASE
        WHEN table_metadata.data.exist(N'/table/is_external') = 1 THEN table_metadata.data.value(N'(/table/is_external/text())[1]', N'bit')
        ELSE CONVERT(bit, 0)
    END
    FROM (SELECT data = (SELECT table_info.* FOR XML PATH(N'table'), TYPE)) AS table_metadata
) AS external_info
OUTER APPLY (
    SELECT definitions = STUFF((
        SELECT CHAR(30) + definition
        FROM (
            SELECT definition =
                N'CONSTRAINT ' + QUOTENAME(unique_index.name) + N' UNIQUE ' +
                CASE WHEN unique_index.type_desc LIKE N'%HASH%' THEN N'NONCLUSTERED HASH' WHEN unique_index.type_desc = N'NONCLUSTERED' THEN N'NONCLUSTERED' ELSE N'CLUSTERED' END +
                N' (' +
                STUFF((
                    SELECT N', ' + QUOTENAME(unique_column.name) + CASE WHEN unique_index.type_desc LIKE N'%HASH%' THEN N'' WHEN unique_index_column.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END
                    FROM sys.index_columns AS unique_index_column
                    INNER JOIN sys.columns AS unique_column ON unique_column.object_id = unique_index_column.object_id AND unique_column.column_id = unique_index_column.column_id
                    WHERE unique_index_column.object_id = unique_index.object_id
                      AND unique_index_column.index_id = unique_index.index_id
                      AND unique_index_column.key_ordinal > 0
                    ORDER BY unique_index_column.key_ordinal
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'nvarchar(max)'), 1, 2, N'') + N')' +
                {UniqueHashBucketCount}
            FROM sys.indexes AS unique_index
            {UniqueHashJoin}
            WHERE unique_index.object_id = table_info.object_id
              AND unique_index.is_unique_constraint = 1
            UNION ALL
            SELECT definition =
                N'INDEX ' + QUOTENAME(memory_index.name) + N' ' +
                CASE WHEN memory_index.is_unique = 1 THEN N'UNIQUE ' ELSE N'' END +
                CASE WHEN memory_index.type_desc LIKE N'%HASH%' THEN N'NONCLUSTERED HASH' WHEN memory_index.type_desc = N'NONCLUSTERED' THEN N'NONCLUSTERED' ELSE memory_index.type_desc END +
                N' (' +
                STUFF((
                    SELECT N', ' + QUOTENAME(memory_column.name) + CASE WHEN memory_index.type_desc LIKE N'%HASH%' THEN N'' WHEN memory_index_column.is_descending_key = 1 THEN N' DESC' ELSE N' ASC' END
                    FROM sys.index_columns AS memory_index_column
                    INNER JOIN sys.columns AS memory_column ON memory_column.object_id = memory_index_column.object_id AND memory_column.column_id = memory_index_column.column_id
                    WHERE memory_index_column.object_id = memory_index.object_id
                      AND memory_index_column.index_id = memory_index.index_id
                      AND memory_index_column.key_ordinal > 0
                    ORDER BY memory_index_column.key_ordinal
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'nvarchar(max)'), 1, 2, N'') + N')' +
                {MemoryHashBucketCount}
            FROM sys.indexes AS memory_index
            {MemoryHashJoin}
            WHERE memory_index.object_id = table_info.object_id
              AND memory_info.is_memory_optimized = 1
              AND memory_index.index_id > 0
              AND memory_index.is_primary_key = 0
              AND memory_index.is_unique_constraint = 0
              AND memory_index.is_hypothetical = 0
              AND memory_index.is_disabled = 0
            UNION ALL
            SELECT definition =
                N'CONSTRAINT ' + QUOTENAME(check_info.name) + N' CHECK ' +
                CASE WHEN check_info.is_not_for_replication = 1 THEN N'NOT FOR REPLICATION ' ELSE N'' END +
                check_info.definition
            FROM sys.check_constraints AS check_info
            WHERE check_info.parent_object_id = table_info.object_id
              AND check_info.is_disabled = 0
              AND check_info.is_not_trusted = 0
        ) AS table_constraint
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 1, N'')
) AS constraint_info
OUTER APPLY (
    SELECT statements = STUFF((
        SELECT CHAR(30) + statement
        FROM (
            SELECT statement =
                N'ALTER TABLE ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(table_info.name) +
                CASE WHEN check_info.is_not_trusted = 1 OR check_info.is_disabled = 1 THEN N' WITH NOCHECK' ELSE N' WITH CHECK' END +
                N' ADD CONSTRAINT ' + QUOTENAME(check_info.name) + N' CHECK ' +
                CASE WHEN check_info.is_not_for_replication = 1 THEN N'NOT FOR REPLICATION ' ELSE N'' END +
                check_info.definition + N';' +
                CASE WHEN check_info.is_disabled = 1 THEN
                    CHAR(30) + N'ALTER TABLE ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(table_info.name) +
                    N' NOCHECK CONSTRAINT ' + QUOTENAME(check_info.name) + N';'
                ELSE N'' END
            FROM sys.check_constraints AS check_info
            WHERE check_info.parent_object_id = table_info.object_id
              AND (check_info.is_disabled = 1 OR check_info.is_not_trusted = 1)
            UNION ALL
            SELECT statement =
                N'ALTER TABLE ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(table_info.name) +
                CASE WHEN foreign_key.is_not_trusted = 1 OR foreign_key.is_disabled = 1 THEN N' WITH NOCHECK' ELSE N' WITH CHECK' END +
                N' ADD CONSTRAINT ' + QUOTENAME(foreign_key.name) + N' FOREIGN KEY (' +
                STUFF((
                    SELECT N', ' + QUOTENAME(parent_column.name)
                    FROM sys.foreign_key_columns AS foreign_key_column
                    INNER JOIN sys.columns AS parent_column ON parent_column.object_id = foreign_key_column.parent_object_id AND parent_column.column_id = foreign_key_column.parent_column_id
                    WHERE foreign_key_column.constraint_object_id = foreign_key.object_id
                    ORDER BY foreign_key_column.constraint_column_id
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'nvarchar(max)'), 1, 2, N'') + N') REFERENCES ' +
                QUOTENAME(referenced_schema.name) + N'.' + QUOTENAME(referenced_table.name) + N' (' +
                STUFF((
                    SELECT N', ' + QUOTENAME(referenced_column.name)
                    FROM sys.foreign_key_columns AS foreign_key_column
                    INNER JOIN sys.columns AS referenced_column ON referenced_column.object_id = foreign_key_column.referenced_object_id AND referenced_column.column_id = foreign_key_column.referenced_column_id
                    WHERE foreign_key_column.constraint_object_id = foreign_key.object_id
                    ORDER BY foreign_key_column.constraint_column_id
                    FOR XML PATH(N''), TYPE
                ).value(N'.', N'nvarchar(max)'), 1, 2, N'') + N')' +
                CASE WHEN foreign_key.delete_referential_action_desc <> N'NO_ACTION' THEN N' ON DELETE ' + REPLACE(foreign_key.delete_referential_action_desc, N'_', N' ') ELSE N'' END +
                CASE WHEN foreign_key.update_referential_action_desc <> N'NO_ACTION' THEN N' ON UPDATE ' + REPLACE(foreign_key.update_referential_action_desc, N'_', N' ') ELSE N'' END +
                CASE WHEN foreign_key.is_not_for_replication = 1 THEN N' NOT FOR REPLICATION' ELSE N'' END +
                N';' +
                CASE WHEN foreign_key.is_disabled = 1 THEN
                    CHAR(30) + N'ALTER TABLE ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(table_info.name) +
                    N' NOCHECK CONSTRAINT ' + QUOTENAME(foreign_key.name) + N';'
                ELSE N'' END
            FROM sys.foreign_keys AS foreign_key
            INNER JOIN sys.tables AS referenced_table ON referenced_table.object_id = foreign_key.referenced_object_id
            INNER JOIN sys.schemas AS referenced_schema ON referenced_schema.schema_id = referenced_table.schema_id
            WHERE foreign_key.parent_object_id = table_info.object_id
{GraphEdgeConstraintStatements}
        ) AS table_statement
        FOR XML PATH(N''), TYPE
    ).value(N'.', N'nvarchar(max)'), 1, 1, N'')
) AS post_create_info
WHERE (@schema IS NULL OR schema_info.name = @schema)
  AND (@name IS NULL OR table_info.name = @name)
  AND temporal_info.temporal_type <> 1
  AND ledger_info.ledger_type <> 1
  AND external_info.is_external = 0
{GraphHiddenColumnFilter}
{GraphTableOnlyRows}
ORDER BY SchemaName, TableName, Ordinal;";
}
