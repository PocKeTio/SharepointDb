using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Core;

namespace SharePointDb.Sqlite
{
    public sealed class SqliteLocalStore : ILocalStore, ILocalEntityStore
    {
        private readonly string _connectionString;

        public SqliteLocalStore(string sqliteFilePath)
        {
            if (string.IsNullOrWhiteSpace(sqliteFilePath))
            {
                throw new ArgumentException("SQLite file path is required.", nameof(sqliteFilePath));
            }

            var builder = new SQLiteConnectionStringBuilder
            {
                DataSource = sqliteFilePath,
                Pooling = true
            };

            _connectionString = builder.ToString();
        }

        public Task InitializeAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS LocalConfig (
    AppId TEXT NOT NULL PRIMARY KEY,
    ConfigVersion INTEGER NOT NULL,
    TablesJson TEXT NOT NULL,
    UpdatedUtc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS SyncState (
    EntityName TEXT NOT NULL PRIMARY KEY,
    LastSyncModifiedUtc TEXT NULL,
    LastSyncSpId INTEGER NULL,
    LastSuccessfulSyncUtc TEXT NULL,
    LastConfigVersionApplied INTEGER NULL,
    LastError TEXT NULL
);

CREATE TABLE IF NOT EXISTS ChangeLog (
    Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    EntityName TEXT NOT NULL,
    AppPK TEXT NOT NULL,
    Operation TEXT NOT NULL,
    PayloadJson TEXT NULL,
    CreatedUtc TEXT NOT NULL,
    Status TEXT NOT NULL,
    AttemptCount INTEGER NOT NULL DEFAULT 0,
    AppliedUtc TEXT NULL,
    LastError TEXT NULL
);

CREATE INDEX IF NOT EXISTS IX_ChangeLog_Status_CreatedUtc ON ChangeLog(Status, CreatedUtc);
CREATE INDEX IF NOT EXISTS IX_ChangeLog_Entity_AppPK ON ChangeLog(EntityName, AppPK);

CREATE TABLE IF NOT EXISTS Attachments (
    EntityName TEXT NOT NULL,
    AppPK TEXT NOT NULL,
    FileName TEXT NOT NULL,
    LocalPath TEXT NOT NULL,
    Size INTEGER NULL,
    Hash TEXT NULL,
    LastSyncUtc TEXT NULL,
    IsDeleted INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (EntityName, AppPK, FileName)
);

CREATE INDEX IF NOT EXISTS IX_Attachments_Entity_AppPK ON Attachments(EntityName, AppPK);

CREATE TABLE IF NOT EXISTS ConflictLog (
    Id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    OccurredUtc TEXT NOT NULL,
    EntityName TEXT NOT NULL,
    AppPK TEXT NOT NULL,
    ChangeId INTEGER NULL,
    Operation TEXT NOT NULL,
    Policy TEXT NOT NULL,
    SharePointId INTEGER NULL,
    LocalETag TEXT NULL,
    ServerETag TEXT NULL,
    LocalPayloadJson TEXT NULL,
    ServerFieldsJson TEXT NULL,
    Message TEXT NULL
);

CREATE INDEX IF NOT EXISTS IX_ConflictLog_OccurredUtc ON ConflictLog(OccurredUtc);
CREATE INDEX IF NOT EXISTS IX_ConflictLog_Entity_AppPK ON ConflictLog(EntityName, AppPK);
";

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task MarkChangeConflictedAsync(long changeId, string error, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
UPDATE ChangeLog
SET Status = @status,
    AttemptCount = AttemptCount + 1,
    LastError = @error
WHERE Id = @id;";

                    cmd.Parameters.AddWithValue("@status", ChangeStatus.Conflict.ToString());
                    cmd.Parameters.AddWithValue("@error", (object)error ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@id", changeId);

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task LogConflictAsync(ConflictLogEntry entry, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (entry == null)
                {
                    throw new ArgumentNullException(nameof(entry));
                }

                if (string.IsNullOrWhiteSpace(entry.EntityName))
                {
                    throw new ArgumentException("EntityName is required.", nameof(entry));
                }

                if (string.IsNullOrWhiteSpace(entry.AppPK))
                {
                    throw new ArgumentException("AppPK is required.", nameof(entry));
                }

                var occurred = entry.OccurredUtc == default(DateTime) ? DateTime.UtcNow : entry.OccurredUtc;

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
INSERT INTO ConflictLog (
    OccurredUtc,
    EntityName,
    AppPK,
    ChangeId,
    Operation,
    Policy,
    SharePointId,
    LocalETag,
    ServerETag,
    LocalPayloadJson,
    ServerFieldsJson,
    Message
)
VALUES (
    @occurredUtc,
    @entityName,
    @appPk,
    @changeId,
    @operation,
    @policy,
    @spId,
    @localETag,
    @serverETag,
    @localPayloadJson,
    @serverFieldsJson,
    @message
);";

                    cmd.Parameters.AddWithValue("@occurredUtc", FormatUtc(occurred));
                    cmd.Parameters.AddWithValue("@entityName", entry.EntityName);
                    cmd.Parameters.AddWithValue("@appPk", entry.AppPK);
                    cmd.Parameters.AddWithValue("@changeId", (object)entry.ChangeId ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@operation", entry.Operation.ToString());
                    cmd.Parameters.AddWithValue("@policy", entry.Policy.ToString());
                    cmd.Parameters.AddWithValue("@spId", (object)entry.SharePointId ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@localETag", (object)entry.LocalETag ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@serverETag", (object)entry.ServerETag ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@localPayloadJson", (object)entry.LocalPayloadJson ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@serverFieldsJson", (object)entry.ServerFieldsJson ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@message", (object)entry.Message ?? DBNull.Value);

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task<IReadOnlyList<ConflictLogEntry>> GetRecentConflictsAsync(int maxCount, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run<IReadOnlyList<ConflictLogEntry>>(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var list = new List<ConflictLogEntry>();

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
SELECT Id, OccurredUtc, EntityName, AppPK, ChangeId, Operation, Policy, SharePointId, LocalETag, ServerETag, LocalPayloadJson, ServerFieldsJson, Message
FROM ConflictLog
ORDER BY OccurredUtc DESC, Id DESC
LIMIT @limit;";

                    cmd.Parameters.AddWithValue("@limit", maxCount <= 0 ? 100 : maxCount);

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            list.Add(new ConflictLogEntry
                            {
                                Id = reader.GetInt64(0),
                                OccurredUtc = ParseUtc(reader.GetString(1)) ?? DateTime.UtcNow,
                                EntityName = reader.GetString(2),
                                AppPK = reader.GetString(3),
                                ChangeId = reader.IsDBNull(4) ? (long?)null : reader.GetInt64(4),
                                Operation = ParseEnum<ChangeOperation>(reader.GetString(5)),
                                Policy = ParseEnum<ConflictResolutionPolicy>(reader.GetString(6)),
                                SharePointId = reader.IsDBNull(7) ? (int?)null : reader.GetInt32(7),
                                LocalETag = reader.IsDBNull(8) ? null : reader.GetString(8),
                                ServerETag = reader.IsDBNull(9) ? null : reader.GetString(9),
                                LocalPayloadJson = reader.IsDBNull(10) ? null : reader.GetString(10),
                                ServerFieldsJson = reader.IsDBNull(11) ? null : reader.GetString(11),
                                Message = reader.IsDBNull(12) ? null : reader.GetString(12)
                            });
                        }
                    }
                }

                return list;
            }, cancellationToken);
        }

        public Task<LocalConfig> GetLocalConfigAsync(string appId, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(appId))
                {
                    throw new ArgumentException("AppId is required.", nameof(appId));
                }

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = "SELECT AppId, ConfigVersion, TablesJson, UpdatedUtc FROM LocalConfig WHERE AppId = @appId;";
                    cmd.Parameters.AddWithValue("@appId", appId);

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                        {
                            return new LocalConfig
                            {
                                AppId = appId,
                                ConfigVersion = 0,
                                Tables = Array.Empty<AppTableConfig>(),
                                UpdatedUtc = DateTime.UtcNow
                            };
                        }

                        var tablesJson = reader.GetString(2);
                        var tables = Json.Deserialize<List<AppTableConfig>>(tablesJson) ?? new List<AppTableConfig>();

                        return new LocalConfig
                        {
                            AppId = reader.GetString(0),
                            ConfigVersion = reader.GetInt32(1),
                            Tables = tables,
                            UpdatedUtc = ParseUtc(reader.GetString(3)) ?? DateTime.UtcNow
                        };
                    }
                }
            }, cancellationToken);
        }

        public Task UpsertLocalConfigAsync(LocalConfig config, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (config == null)
                {
                    throw new ArgumentNullException(nameof(config));
                }

                if (string.IsNullOrWhiteSpace(config.AppId))
                {
                    throw new ArgumentException("AppId is required.", nameof(config));
                }

                var tablesJson = Json.Serialize(config.Tables ?? Array.Empty<AppTableConfig>());
                var updatedUtc = FormatUtc(config.UpdatedUtc == default(DateTime) ? DateTime.UtcNow : config.UpdatedUtc);

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
INSERT OR REPLACE INTO LocalConfig (AppId, ConfigVersion, TablesJson, UpdatedUtc)
VALUES (@appId, @configVersion, @tablesJson, @updatedUtc);";

                    cmd.Parameters.AddWithValue("@appId", config.AppId);
                    cmd.Parameters.AddWithValue("@configVersion", config.ConfigVersion);
                    cmd.Parameters.AddWithValue("@tablesJson", tablesJson);
                    cmd.Parameters.AddWithValue("@updatedUtc", updatedUtc);

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task<SyncState> GetSyncStateAsync(string entityName, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(entityName))
                {
                    throw new ArgumentException("EntityName is required.", nameof(entityName));
                }

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"SELECT EntityName, LastSyncModifiedUtc, LastSyncSpId, LastSuccessfulSyncUtc, LastConfigVersionApplied, LastError
FROM SyncState WHERE EntityName = @entityName;";
                    cmd.Parameters.AddWithValue("@entityName", entityName);

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                        {
                            return new SyncState
                            {
                                EntityName = entityName,
                                LastSyncModifiedUtc = null,
                                LastSyncSpId = null,
                                LastSuccessfulSyncUtc = null,
                                LastConfigVersionApplied = null,
                                LastError = null
                            };
                        }

                        return new SyncState
                        {
                            EntityName = reader.GetString(0),
                            LastSyncModifiedUtc = reader.IsDBNull(1) ? (DateTime?)null : ParseUtc(reader.GetString(1)),
                            LastSyncSpId = reader.IsDBNull(2) ? (int?)null : reader.GetInt32(2),
                            LastSuccessfulSyncUtc = reader.IsDBNull(3) ? (DateTime?)null : ParseUtc(reader.GetString(3)),
                            LastConfigVersionApplied = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4),
                            LastError = reader.IsDBNull(5) ? null : reader.GetString(5)
                        };
                    }
                }
            }, cancellationToken);
        }

        public Task UpsertSyncStateAsync(SyncState state, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (state == null)
                {
                    throw new ArgumentNullException(nameof(state));
                }

                if (string.IsNullOrWhiteSpace(state.EntityName))
                {
                    throw new ArgumentException("EntityName is required.", nameof(state));
                }

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
INSERT OR REPLACE INTO SyncState (EntityName, LastSyncModifiedUtc, LastSyncSpId, LastSuccessfulSyncUtc, LastConfigVersionApplied, LastError)
VALUES (@entityName, @lastSyncModifiedUtc, @lastSyncSpId, @lastSuccessfulSyncUtc, @lastConfigVersionApplied, @lastError);";

                    cmd.Parameters.AddWithValue("@entityName", state.EntityName);
                    cmd.Parameters.AddWithValue("@lastSyncModifiedUtc", (object)FormatUtcOrNull(state.LastSyncModifiedUtc) ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@lastSyncSpId", (object)state.LastSyncSpId ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@lastSuccessfulSyncUtc", (object)FormatUtcOrNull(state.LastSuccessfulSyncUtc) ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@lastConfigVersionApplied", (object)state.LastConfigVersionApplied ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@lastError", (object)state.LastError ?? DBNull.Value);

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task EnqueueChangeAsync(ChangeLogEntry entry, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (entry == null)
                {
                    throw new ArgumentNullException(nameof(entry));
                }

                if (string.IsNullOrWhiteSpace(entry.EntityName))
                {
                    throw new ArgumentException("EntityName is required.", nameof(entry));
                }

                if (string.IsNullOrWhiteSpace(entry.AppPK))
                {
                    throw new ArgumentException("AppPK is required.", nameof(entry));
                }

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
INSERT INTO ChangeLog (EntityName, AppPK, Operation, PayloadJson, CreatedUtc, Status, AttemptCount, AppliedUtc, LastError)
VALUES (@entityName, @appPk, @operation, @payloadJson, @createdUtc, @status, @attemptCount, @appliedUtc, @lastError);";

                    cmd.Parameters.AddWithValue("@entityName", entry.EntityName);
                    cmd.Parameters.AddWithValue("@appPk", entry.AppPK);
                    cmd.Parameters.AddWithValue("@operation", entry.Operation.ToString());
                    cmd.Parameters.AddWithValue("@payloadJson", (object)entry.PayloadJson ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@createdUtc", FormatUtc(entry.CreatedUtc == default(DateTime) ? DateTime.UtcNow : entry.CreatedUtc));
                    cmd.Parameters.AddWithValue("@status", entry.Status.ToString());
                    cmd.Parameters.AddWithValue("@attemptCount", entry.AttemptCount);
                    cmd.Parameters.AddWithValue("@appliedUtc", (object)FormatUtcOrNull(entry.AppliedUtc) ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@lastError", (object)entry.LastError ?? DBNull.Value);

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task<IReadOnlyList<ChangeLogEntry>> GetPendingChangesAsync(int maxCount, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run<IReadOnlyList<ChangeLogEntry>>(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var list = new List<ChangeLogEntry>();

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
SELECT Id, EntityName, AppPK, Operation, PayloadJson, CreatedUtc, Status, AttemptCount, AppliedUtc, LastError
FROM ChangeLog
WHERE Status = @status
ORDER BY CreatedUtc
LIMIT @limit;";

                    cmd.Parameters.AddWithValue("@status", ChangeStatus.Pending.ToString());
                    cmd.Parameters.AddWithValue("@limit", maxCount <= 0 ? 100 : maxCount);

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            list.Add(new ChangeLogEntry
                            {
                                Id = reader.GetInt64(0),
                                EntityName = reader.GetString(1),
                                AppPK = reader.GetString(2),
                                Operation = ParseEnum<ChangeOperation>(reader.GetString(3)),
                                PayloadJson = reader.IsDBNull(4) ? null : reader.GetString(4),
                                CreatedUtc = ParseUtc(reader.GetString(5)) ?? DateTime.UtcNow,
                                Status = ParseEnum<ChangeStatus>(reader.GetString(6)),
                                AttemptCount = reader.GetInt32(7),
                                AppliedUtc = reader.IsDBNull(8) ? (DateTime?)null : ParseUtc(reader.GetString(8)),
                                LastError = reader.IsDBNull(9) ? null : reader.GetString(9)
                            });
                        }
                    }
                }

                return list;
            }, cancellationToken);
        }

        public Task MarkChangeAppliedAsync(long changeId, DateTime appliedUtc, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
UPDATE ChangeLog
SET Status = @status,
    AppliedUtc = @appliedUtc,
    LastError = NULL
WHERE Id = @id;";

                    cmd.Parameters.AddWithValue("@status", ChangeStatus.Applied.ToString());
                    cmd.Parameters.AddWithValue("@appliedUtc", FormatUtc(appliedUtc == default(DateTime) ? DateTime.UtcNow : appliedUtc));
                    cmd.Parameters.AddWithValue("@id", changeId);

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task MarkChangeFailedAsync(long changeId, string error, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = @"
UPDATE ChangeLog
SET AttemptCount = AttemptCount + 1,
    LastError = @error
WHERE Id = @id;";

                    cmd.Parameters.AddWithValue("@error", (object)error ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@id", changeId);

                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task EnsureEntitySchemaAsync(AppTableConfig tableConfig, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (tableConfig == null)
                {
                    throw new ArgumentNullException(nameof(tableConfig));
                }

                if (string.IsNullOrWhiteSpace(tableConfig.EntityName))
                {
                    throw new ArgumentException("EntityName is required.", nameof(tableConfig));
                }

                var tableName = QuoteIdentifier(tableConfig.EntityName);

                using (var connection = OpenConnection())
                {
                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = $@"
CREATE TABLE IF NOT EXISTS {tableName} (
    {QuoteIdentifier("AppPK")} TEXT NOT NULL PRIMARY KEY,
    {QuoteIdentifier("__sp_id")} INTEGER NULL,
    {QuoteIdentifier("__sp_modified_utc")} TEXT NULL,
    {QuoteIdentifier("__sp_etag")} TEXT NULL,
    {QuoteIdentifier("IsDeleted")} INTEGER NOT NULL DEFAULT 0,
    {QuoteIdentifier("DeletedAtUtc")} TEXT NULL
);";

                        cmd.ExecuteNonQuery();
                    }

                    var existing = GetExistingColumns(connection, tableConfig.EntityName);

                    var pkInternal = tableConfig.PkInternalName ?? "AppPK";
                    var reserved = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    {
                        "AppPK",
                        pkInternal,
                        "__sp_id",
                        "__sp_modified_utc",
                        "__sp_etag",
                        "IsDeleted",
                        "DeletedAtUtc"
                    };

                    if (tableConfig.SelectFields != null)
                    {
                        foreach (var fieldName in tableConfig.SelectFields.Where(f => !string.IsNullOrWhiteSpace(f)))
                        {
                            if (reserved.Contains(fieldName))
                            {
                                continue;
                            }

                            if (existing.Contains(fieldName))
                            {
                                continue;
                            }

                            using (var cmd = connection.CreateCommand())
                            {
                                cmd.CommandText = $"ALTER TABLE {tableName} ADD COLUMN {QuoteIdentifier(fieldName)} NUMERIC NULL;";
                                cmd.ExecuteNonQuery();
                            }
                        }
                    }

                    EnsureIndex(connection, tableConfig.EntityName, "IsDeleted");
                    EnsureIndex(connection, tableConfig.EntityName, "__sp_modified_utc");
                    EnsureIndex(connection, tableConfig.EntityName, "DeletedAtUtc");
                }
            }, cancellationToken);
        }

        public Task UpsertEntityAsync(
            string entityName,
            string appPk,
            IReadOnlyDictionary<string, object> fields,
            LocalEntitySystemFields system,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(entityName))
                {
                    throw new ArgumentException("EntityName is required.", nameof(entityName));
                }

                if (string.IsNullOrWhiteSpace(appPk))
                {
                    throw new ArgumentException("AppPK is required.", nameof(appPk));
                }

                var tableName = QuoteIdentifier(entityName);
                var fieldDict = fields ?? new Dictionary<string, object>();
                var sys = system ?? new LocalEntitySystemFields();

                var reserved = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    "AppPK",
                    "__sp_id",
                    "__sp_modified_utc",
                    "__sp_etag",
                    "IsDeleted",
                    "DeletedAtUtc"
                };

                var columns = new List<string>();
                var values = new List<string>();
                var parameters = new List<SQLiteParameter>();
                var pIndex = 0;

                void Add(string column, object value)
                {
                    var paramName = "@p" + pIndex.ToString(CultureInfo.InvariantCulture);
                    pIndex++;
                    columns.Add(QuoteIdentifier(column));
                    values.Add(paramName);
                    parameters.Add(new SQLiteParameter(paramName, value ?? DBNull.Value));
                }

                Add("AppPK", appPk);

                foreach (var kvp in fieldDict)
                {
                    if (string.IsNullOrWhiteSpace(kvp.Key))
                    {
                        continue;
                    }

                    if (reserved.Contains(kvp.Key))
                    {
                        continue;
                    }

                    Add(kvp.Key, ToDbValue(kvp.Value));
                }

                Add("__sp_id", sys.SharePointId.HasValue ? (object)sys.SharePointId.Value : DBNull.Value);
                Add("__sp_modified_utc", (object)FormatUtcOrNull(sys.SharePointModifiedUtc) ?? DBNull.Value);
                Add("__sp_etag", (object)sys.SharePointETag ?? DBNull.Value);
                Add("IsDeleted", sys.IsDeleted ? 1 : 0);
                Add("DeletedAtUtc", (object)FormatUtcOrNull(sys.DeletedAtUtc) ?? DBNull.Value);

                var sql = $"INSERT OR REPLACE INTO {tableName} ({string.Join(",", columns)}) VALUES ({string.Join(",", values)});";

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    cmd.Parameters.AddRange(parameters.ToArray());
                    cmd.ExecuteNonQuery();
                }
            }, cancellationToken);
        }

        public Task<LocalEntityRow> GetEntityAsync(string entityName, string appPk, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (string.IsNullOrWhiteSpace(entityName))
                {
                    throw new ArgumentException("EntityName is required.", nameof(entityName));
                }

                if (string.IsNullOrWhiteSpace(appPk))
                {
                    throw new ArgumentException("AppPK is required.", nameof(appPk));
                }

                var tableName = QuoteIdentifier(entityName);

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = $"SELECT * FROM {tableName} WHERE {QuoteIdentifier("AppPK")} = @appPk LIMIT 1;";
                    cmd.Parameters.AddWithValue("@appPk", appPk);

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                        {
                            return null;
                        }

                        var fields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                        var system = new LocalEntitySystemFields();

                        for (var i = 0; i < reader.FieldCount; i++)
                        {
                            var name = reader.GetName(i);
                            var value = reader.IsDBNull(i) ? null : reader.GetValue(i);

                            if (string.Equals(name, "AppPK", StringComparison.OrdinalIgnoreCase))
                            {
                                continue;
                            }

                            if (string.Equals(name, "__sp_id", StringComparison.OrdinalIgnoreCase))
                            {
                                if (value != null)
                                {
                                    system.SharePointId = Convert.ToInt32(value, CultureInfo.InvariantCulture);
                                }

                                continue;
                            }

                            if (string.Equals(name, "__sp_modified_utc", StringComparison.OrdinalIgnoreCase))
                            {
                                system.SharePointModifiedUtc = value == null ? (DateTime?)null : ParseUtc(Convert.ToString(value, CultureInfo.InvariantCulture));
                                continue;
                            }

                            if (string.Equals(name, "__sp_etag", StringComparison.OrdinalIgnoreCase))
                            {
                                system.SharePointETag = value == null ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
                                continue;
                            }

                            if (string.Equals(name, "IsDeleted", StringComparison.OrdinalIgnoreCase))
                            {
                                if (value != null)
                                {
                                    system.IsDeleted = Convert.ToInt32(value, CultureInfo.InvariantCulture) != 0;
                                }

                                continue;
                            }

                            if (string.Equals(name, "DeletedAtUtc", StringComparison.OrdinalIgnoreCase))
                            {
                                system.DeletedAtUtc = value == null ? (DateTime?)null : ParseUtc(Convert.ToString(value, CultureInfo.InvariantCulture));
                                continue;
                            }

                            fields[name] = value;
                        }

                        return new LocalEntityRow
                        {
                            AppPK = appPk,
                            Fields = fields,
                            System = system
                        };
                    }
                }
            }, cancellationToken);
        }

        public void Dispose()
        {
        }

        private SQLiteConnection OpenConnection()
        {
            var connection = new SQLiteConnection(_connectionString);
            connection.Open();

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"PRAGMA busy_timeout = 5000; PRAGMA foreign_keys = ON; PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;";
                cmd.ExecuteNonQuery();
            }

            return connection;
        }

        private static string FormatUtc(DateTime dateTimeUtc)
        {
            return dateTimeUtc.ToUniversalTime().ToString("o", CultureInfo.InvariantCulture);
        }

        private static string FormatUtcOrNull(DateTime? dateTimeUtc)
        {
            if (!dateTimeUtc.HasValue)
            {
                return null;
            }

            return FormatUtc(dateTimeUtc.Value);
        }

        private static DateTime? ParseUtc(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                return null;
            }

            DateTime dt;
            if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dt))
            {
                return DateTime.SpecifyKind(dt, DateTimeKind.Utc).ToUniversalTime();
            }

            return null;
        }

        private static T ParseEnum<T>(string value) where T : struct
        {
            T parsed;
            if (Enum.TryParse(value, true, out parsed))
            {
                return parsed;
            }

            return default(T);
        }

        private static string QuoteIdentifier(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("Identifier is required.", nameof(name));
            }

            return "\"" + name.Replace("\"", "\"\"") + "\"";
        }

        private static HashSet<string> GetExistingColumns(SQLiteConnection connection, string tableName)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"PRAGMA table_info({QuoteIdentifier(tableName)});";

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var name = reader.GetString(1);
                        if (!string.IsNullOrWhiteSpace(name))
                        {
                            set.Add(name);
                        }
                    }
                }
            }

            return set;
        }

        private static void EnsureIndex(SQLiteConnection connection, string tableName, string columnName)
        {
            var sanitizedTable = SanitizeForIndexName(tableName);
            var sanitizedColumn = SanitizeForIndexName(columnName);
            var indexName = $"IX_{sanitizedTable}_{sanitizedColumn}";

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"CREATE INDEX IF NOT EXISTS {QuoteIdentifier(indexName)} ON {QuoteIdentifier(tableName)} ({QuoteIdentifier(columnName)});";
                cmd.ExecuteNonQuery();
            }
        }

        private static string SanitizeForIndexName(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return "_";
            }

            var sb = new StringBuilder(value.Length);
            foreach (var ch in value)
            {
                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_')
                {
                    sb.Append(ch);
                }
                else
                {
                    sb.Append('_');
                }
            }

            return sb.ToString();
        }

        private static object ToDbValue(object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is DateTime)
            {
                return FormatUtc(((DateTime)value));
            }

            if (value is DateTimeOffset)
            {
                return ((DateTimeOffset)value).UtcDateTime.ToString("o", CultureInfo.InvariantCulture);
            }

            if (value is bool)
            {
                return ((bool)value) ? 1 : 0;
            }

            var type = value.GetType();
            if (type.IsPrimitive || value is decimal || value is string)
            {
                return value;
            }

            return Json.Serialize(value);
        }
    }
}
