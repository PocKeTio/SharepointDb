using System;
 using System.Collections.Generic;
 using System.Data;
 using System.Data.OleDb;
 using System.Globalization;
 using System.IO;
 using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Core;

namespace SharePointDb.Access
{
    public sealed class AccessLocalStore : ILocalStore, ILocalEntityStore
    {
        private readonly string _accessFilePath;
        private readonly string _connectionString;
        private bool _disposed;

        public AccessLocalStore(string accessFilePath)
        {
            if (string.IsNullOrWhiteSpace(accessFilePath))
            {
                throw new ArgumentException("Access file path is required.", nameof(accessFilePath));
            }

            _accessFilePath = accessFilePath;

            if (!File.Exists(_accessFilePath))
            {
                throw new FileNotFoundException("Access database file not found.", _accessFilePath);
            }

            _connectionString = BuildConnectionString(_accessFilePath);
        }

        public Task InitializeAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                using (var connection = OpenConnection())
                {
                    EnsureCoreSchema(connection);
                }
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
                    EnsureCoreSchema(connection);

                    cmd.CommandText = "SELECT AppId, ConfigVersion, TablesJson, UpdatedUtc FROM LocalConfig WHERE AppId = ?";
                    cmd.Parameters.AddWithValue("@p1", appId);

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

                        var tablesJson = reader.IsDBNull(2) ? null : reader.GetString(2);
                        var tables = Json.Deserialize<List<AppTableConfig>>(tablesJson) ?? new List<AppTableConfig>();

                        return new LocalConfig
                        {
                            AppId = reader.GetString(0),
                            ConfigVersion = reader.GetInt32(1),
                            Tables = tables,
                            UpdatedUtc = ParseUtc(reader.IsDBNull(3) ? null : reader.GetString(3)) ?? DateTime.UtcNow
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
                {
                    EnsureCoreSchema(connection);

                    using (var update = connection.CreateCommand())
                    {
                        update.CommandText = "UPDATE LocalConfig SET ConfigVersion = ?, TablesJson = ?, UpdatedUtc = ? WHERE AppId = ?";
                        update.Parameters.AddWithValue("@p1", config.ConfigVersion);
                        update.Parameters.AddWithValue("@p2", tablesJson);
                        update.Parameters.AddWithValue("@p3", updatedUtc);
                        update.Parameters.AddWithValue("@p4", config.AppId);

                        var affected = update.ExecuteNonQuery();
                        if (affected > 0)
                        {
                            return;
                        }
                    }

                    using (var insert = connection.CreateCommand())
                    {
                        insert.CommandText = "INSERT INTO LocalConfig (AppId, ConfigVersion, TablesJson, UpdatedUtc) VALUES (?, ?, ?, ?)";
                        insert.Parameters.AddWithValue("@p1", config.AppId);
                        insert.Parameters.AddWithValue("@p2", config.ConfigVersion);
                        insert.Parameters.AddWithValue("@p3", tablesJson);
                        insert.Parameters.AddWithValue("@p4", updatedUtc);
                        insert.ExecuteNonQuery();
                    }
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
                    EnsureCoreSchema(connection);

                    cmd.CommandText = "SELECT EntityName, LastSyncModifiedUtc, LastSyncSpId, LastSuccessfulSyncUtc, LastConfigVersionApplied, LastError FROM SyncState WHERE EntityName = ?";
                    cmd.Parameters.AddWithValue("@p1", entityName);

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
                            LastSyncSpId = reader.IsDBNull(2) ? (int?)null : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                            LastSuccessfulSyncUtc = reader.IsDBNull(3) ? (DateTime?)null : ParseUtc(reader.GetString(3)),
                            LastConfigVersionApplied = reader.IsDBNull(4) ? (int?)null : Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture),
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
                {
                    EnsureCoreSchema(connection);

                    using (var update = connection.CreateCommand())
                    {
                        update.CommandText = "UPDATE SyncState SET LastSyncModifiedUtc = ?, LastSyncSpId = ?, LastSuccessfulSyncUtc = ?, LastConfigVersionApplied = ?, LastError = ? WHERE EntityName = ?";
                        update.Parameters.AddWithValue("@p1", (object)FormatUtcOrNull(state.LastSyncModifiedUtc) ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p2", (object)state.LastSyncSpId ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p3", (object)FormatUtcOrNull(state.LastSuccessfulSyncUtc) ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p4", (object)state.LastConfigVersionApplied ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p5", (object)state.LastError ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p6", state.EntityName);

                        var affected = update.ExecuteNonQuery();
                        if (affected > 0)
                        {
                            return;
                        }
                    }

                    using (var insert = connection.CreateCommand())
                    {
                        insert.CommandText = "INSERT INTO SyncState (EntityName, LastSyncModifiedUtc, LastSyncSpId, LastSuccessfulSyncUtc, LastConfigVersionApplied, LastError) VALUES (?, ?, ?, ?, ?, ?)";
                        insert.Parameters.AddWithValue("@p1", state.EntityName);
                        insert.Parameters.AddWithValue("@p2", (object)FormatUtcOrNull(state.LastSyncModifiedUtc) ?? DBNull.Value);
                        insert.Parameters.AddWithValue("@p3", (object)state.LastSyncSpId ?? DBNull.Value);
                        insert.Parameters.AddWithValue("@p4", (object)FormatUtcOrNull(state.LastSuccessfulSyncUtc) ?? DBNull.Value);
                        insert.Parameters.AddWithValue("@p5", (object)state.LastConfigVersionApplied ?? DBNull.Value);
                        insert.Parameters.AddWithValue("@p6", (object)state.LastError ?? DBNull.Value);
                        insert.ExecuteNonQuery();
                    }
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

                var createdUtc = FormatUtc(entry.CreatedUtc == default(DateTime) ? DateTime.UtcNow : entry.CreatedUtc);

                using (var connection = OpenConnection())
                {
                    EnsureCoreSchema(connection);

                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO ChangeLog (EntityName, AppPK, Operation, PayloadJson, CreatedUtc, Status, AttemptCount, AppliedUtc, LastError) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        cmd.Parameters.AddWithValue("@p1", entry.EntityName);
                        cmd.Parameters.AddWithValue("@p2", entry.AppPK);
                        cmd.Parameters.AddWithValue("@p3", entry.Operation.ToString());
                        cmd.Parameters.AddWithValue("@p4", (object)entry.PayloadJson ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p5", createdUtc);
                        cmd.Parameters.AddWithValue("@p6", entry.Status.ToString());
                        cmd.Parameters.AddWithValue("@p7", entry.AttemptCount);
                        cmd.Parameters.AddWithValue("@p8", (object)FormatUtcOrNull(entry.AppliedUtc) ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p9", (object)entry.LastError ?? DBNull.Value);
                        cmd.ExecuteNonQuery();
                    }
                }
            }, cancellationToken);
        }

        public Task<IReadOnlyList<ChangeLogEntry>> GetPendingChangesAsync(int maxCount, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run<IReadOnlyList<ChangeLogEntry>>(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var list = new List<ChangeLogEntry>();
                var limit = maxCount <= 0 ? 100 : maxCount;

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    EnsureCoreSchema(connection);

                    cmd.CommandText = $"SELECT TOP {limit.ToString(CultureInfo.InvariantCulture)} Id, EntityName, AppPK, Operation, PayloadJson, CreatedUtc, Status, AttemptCount, AppliedUtc, LastError FROM ChangeLog WHERE Status = ? ORDER BY CreatedUtc";
                    cmd.Parameters.AddWithValue("@p1", ChangeStatus.Pending.ToString());

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            list.Add(new ChangeLogEntry
                            {
                                Id = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
                                EntityName = reader.GetString(1),
                                AppPK = reader.GetString(2),
                                Operation = ParseEnum<ChangeOperation>(reader.GetString(3)),
                                PayloadJson = reader.IsDBNull(4) ? null : reader.GetString(4),
                                CreatedUtc = ParseUtc(reader.IsDBNull(5) ? null : reader.GetString(5)) ?? DateTime.UtcNow,
                                Status = ParseEnum<ChangeStatus>(reader.GetString(6)),
                                AttemptCount = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
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
                    EnsureCoreSchema(connection);

                    cmd.CommandText = "UPDATE ChangeLog SET Status = ?, AppliedUtc = ?, LastError = NULL WHERE Id = ?";
                    cmd.Parameters.AddWithValue("@p1", ChangeStatus.Applied.ToString());
                    cmd.Parameters.AddWithValue("@p2", FormatUtc(appliedUtc == default(DateTime) ? DateTime.UtcNow : appliedUtc));
                    cmd.Parameters.AddWithValue("@p3", changeId);
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
                    EnsureCoreSchema(connection);

                    cmd.CommandText = "UPDATE ChangeLog SET AttemptCount = AttemptCount + 1, LastError = ? WHERE Id = ?";
                    cmd.Parameters.AddWithValue("@p1", (object)error ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@p2", changeId);
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
                    EnsureCoreSchema(connection);

                    cmd.CommandText = "UPDATE ChangeLog SET Status = ?, AttemptCount = AttemptCount + 1, LastError = ? WHERE Id = ?";
                    cmd.Parameters.AddWithValue("@p1", ChangeStatus.Conflict.ToString());
                    cmd.Parameters.AddWithValue("@p2", (object)error ?? DBNull.Value);
                    cmd.Parameters.AddWithValue("@p3", changeId);
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
                {
                    EnsureCoreSchema(connection);

                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = "INSERT INTO ConflictLog (OccurredUtc, EntityName, AppPK, ChangeId, Operation, Policy, SharePointId, LocalETag, ServerETag, LocalPayloadJson, ServerFieldsJson, Message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        cmd.Parameters.AddWithValue("@p1", FormatUtc(occurred));
                        cmd.Parameters.AddWithValue("@p2", entry.EntityName);
                        cmd.Parameters.AddWithValue("@p3", entry.AppPK);
                        cmd.Parameters.AddWithValue("@p4", (object)entry.ChangeId ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p5", entry.Operation.ToString());
                        cmd.Parameters.AddWithValue("@p6", entry.Policy.ToString());
                        cmd.Parameters.AddWithValue("@p7", (object)entry.SharePointId ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p8", (object)entry.LocalETag ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p9", (object)entry.ServerETag ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p10", (object)entry.LocalPayloadJson ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p11", (object)entry.ServerFieldsJson ?? DBNull.Value);
                        cmd.Parameters.AddWithValue("@p12", (object)entry.Message ?? DBNull.Value);
                        cmd.ExecuteNonQuery();
                    }
                }
            }, cancellationToken);
        }

        public Task<IReadOnlyList<ConflictLogEntry>> GetRecentConflictsAsync(int maxCount, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.Run<IReadOnlyList<ConflictLogEntry>>(() =>
            {
                cancellationToken.ThrowIfCancellationRequested();

                var list = new List<ConflictLogEntry>();
                var limit = maxCount <= 0 ? 100 : maxCount;

                using (var connection = OpenConnection())
                using (var cmd = connection.CreateCommand())
                {
                    EnsureCoreSchema(connection);

                    cmd.CommandText = $"SELECT TOP {limit.ToString(CultureInfo.InvariantCulture)} Id, OccurredUtc, EntityName, AppPK, ChangeId, Operation, Policy, SharePointId, LocalETag, ServerETag, LocalPayloadJson, ServerFieldsJson, Message FROM ConflictLog ORDER BY OccurredUtc DESC, Id DESC";

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            list.Add(new ConflictLogEntry
                            {
                                Id = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
                                OccurredUtc = ParseUtc(reader.IsDBNull(1) ? null : reader.GetString(1)) ?? DateTime.UtcNow,
                                EntityName = reader.GetString(2),
                                AppPK = reader.GetString(3),
                                ChangeId = reader.IsDBNull(4) ? (long?)null : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                                Operation = ParseEnum<ChangeOperation>(reader.GetString(5)),
                                Policy = ParseEnum<ConflictResolutionPolicy>(reader.GetString(6)),
                                SharePointId = reader.IsDBNull(7) ? (int?)null : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
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

                using (var connection = OpenConnection())
                {
                    EnsureEntityTable(connection, tableConfig.EntityName);
                }
            }, cancellationToken);
        }

        public Task UpsertEntityAsync(string entityName, string appPk, IReadOnlyDictionary<string, object> fields, LocalEntitySystemFields system, CancellationToken cancellationToken = default(CancellationToken))
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

                var sys = system ?? new LocalEntitySystemFields();
                var payload = Json.Serialize(fields ?? new Dictionary<string, object>());

                using (var connection = OpenConnection())
                {
                    EnsureEntityTable(connection, entityName);

                    using (var update = connection.CreateCommand())
                    {
                        update.CommandText = $"UPDATE {QuoteIdentifier(entityName)} SET FieldsJson = ?, __sp_id = ?, __sp_modified_utc = ?, __sp_etag = ?, IsDeleted = ?, DeletedAtUtc = ? WHERE AppPK = ?";
                        update.Parameters.AddWithValue("@p1", payload);
                        update.Parameters.AddWithValue("@p2", (object)sys.SharePointId ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p3", (object)FormatUtcOrNull(sys.SharePointModifiedUtc) ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p4", (object)sys.SharePointETag ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p5", sys.IsDeleted);
                        update.Parameters.AddWithValue("@p6", (object)FormatUtcOrNull(sys.DeletedAtUtc) ?? DBNull.Value);
                        update.Parameters.AddWithValue("@p7", appPk);

                        var affected = update.ExecuteNonQuery();
                        if (affected > 0)
                        {
                            return;
                        }
                    }

                    using (var insert = connection.CreateCommand())
                    {
                        insert.CommandText = $"INSERT INTO {QuoteIdentifier(entityName)} (AppPK, FieldsJson, __sp_id, __sp_modified_utc, __sp_etag, IsDeleted, DeletedAtUtc) VALUES (?, ?, ?, ?, ?, ?, ?)";
                        insert.Parameters.AddWithValue("@p1", appPk);
                        insert.Parameters.AddWithValue("@p2", payload);
                        insert.Parameters.AddWithValue("@p3", (object)sys.SharePointId ?? DBNull.Value);
                        insert.Parameters.AddWithValue("@p4", (object)FormatUtcOrNull(sys.SharePointModifiedUtc) ?? DBNull.Value);
                        insert.Parameters.AddWithValue("@p5", (object)sys.SharePointETag ?? DBNull.Value);
                        insert.Parameters.AddWithValue("@p6", sys.IsDeleted);
                        insert.Parameters.AddWithValue("@p7", (object)FormatUtcOrNull(sys.DeletedAtUtc) ?? DBNull.Value);
                        insert.ExecuteNonQuery();
                    }
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

                using (var connection = OpenConnection())
                {
                    if (!TableExists(connection, entityName))
                    {
                        return null;
                    }

                    using (var cmd = connection.CreateCommand())
                    {
                        cmd.CommandText = $"SELECT AppPK, FieldsJson, __sp_id, __sp_modified_utc, __sp_etag, IsDeleted, DeletedAtUtc FROM {QuoteIdentifier(entityName)} WHERE AppPK = ?";
                        cmd.Parameters.AddWithValue("@p1", appPk);

                        using (var reader = cmd.ExecuteReader())
                        {
                            if (!reader.Read())
                            {
                                return null;
                            }

                            var fieldsJson = reader.IsDBNull(1) ? null : reader.GetString(1);
                            var fields = Json.Deserialize<Dictionary<string, object>>(fieldsJson) ?? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                            var system = new LocalEntitySystemFields
                            {
                                SharePointId = reader.IsDBNull(2) ? (int?)null : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                                SharePointModifiedUtc = reader.IsDBNull(3) ? (DateTime?)null : ParseUtc(reader.GetString(3)),
                                SharePointETag = reader.IsDBNull(4) ? null : reader.GetString(4),
                                IsDeleted = !reader.IsDBNull(5) && Convert.ToBoolean(reader.GetValue(5), CultureInfo.InvariantCulture),
                                DeletedAtUtc = reader.IsDBNull(6) ? (DateTime?)null : ParseUtc(reader.GetString(6))
                            };

                            return new LocalEntityRow
                            {
                                AppPK = reader.GetString(0),
                                Fields = fields,
                                System = system
                            };
                        }
                    }
                }
            }, cancellationToken);
        }

        public void Dispose()
        {
            _disposed = true;
        }

        private OleDbConnection OpenConnection()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(AccessLocalStore));
            }

            var connection = new OleDbConnection(_connectionString);
            connection.Open();
            return connection;
        }

        private static string BuildConnectionString(string filePath)
        {
            return $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={filePath};Persist Security Info=False;";
        }

        private static void EnsureCoreSchema(OleDbConnection connection)
        {
            EnsureTable(connection, "LocalConfig", @"CREATE TABLE LocalConfig (AppId TEXT(64) NOT NULL, ConfigVersion LONG NOT NULL, TablesJson LONGTEXT NOT NULL, UpdatedUtc TEXT(50) NOT NULL, CONSTRAINT PK_LocalConfig PRIMARY KEY (AppId))");
            EnsureTable(connection, "SyncState", @"CREATE TABLE SyncState (EntityName TEXT(255) NOT NULL, LastSyncModifiedUtc TEXT(50) NULL, LastSyncSpId LONG NULL, LastSuccessfulSyncUtc TEXT(50) NULL, LastConfigVersionApplied LONG NULL, LastError LONGTEXT NULL, CONSTRAINT PK_SyncState PRIMARY KEY (EntityName))");
            EnsureTable(connection, "ChangeLog", @"CREATE TABLE ChangeLog (Id COUNTER NOT NULL, EntityName TEXT(255) NOT NULL, AppPK TEXT(255) NOT NULL, Operation TEXT(32) NOT NULL, PayloadJson LONGTEXT NULL, CreatedUtc TEXT(50) NOT NULL, Status TEXT(32) NOT NULL, AttemptCount LONG NOT NULL, AppliedUtc TEXT(50) NULL, LastError LONGTEXT NULL, CONSTRAINT PK_ChangeLog PRIMARY KEY (Id))");
            EnsureIndex(connection, "ChangeLog", "IX_ChangeLog_Status_CreatedUtc", new[] { "Status", "CreatedUtc" });
            EnsureIndex(connection, "ChangeLog", "IX_ChangeLog_Entity_AppPK", new[] { "EntityName", "AppPK" });
            EnsureTable(connection, "ConflictLog", @"CREATE TABLE ConflictLog (Id COUNTER NOT NULL, OccurredUtc TEXT(50) NOT NULL, EntityName TEXT(255) NOT NULL, AppPK TEXT(255) NOT NULL, ChangeId LONG NULL, Operation TEXT(32) NOT NULL, Policy TEXT(32) NOT NULL, SharePointId LONG NULL, LocalETag TEXT(255) NULL, ServerETag TEXT(255) NULL, LocalPayloadJson LONGTEXT NULL, ServerFieldsJson LONGTEXT NULL, Message LONGTEXT NULL, CONSTRAINT PK_ConflictLog PRIMARY KEY (Id))");
            EnsureIndex(connection, "ConflictLog", "IX_ConflictLog_OccurredUtc", new[] { "OccurredUtc" });
            EnsureIndex(connection, "ConflictLog", "IX_ConflictLog_Entity_AppPK", new[] { "EntityName", "AppPK" });
        }

        private static void EnsureEntityTable(OleDbConnection connection, string entityName)
        {
            EnsureTable(connection, entityName, $"CREATE TABLE {QuoteIdentifier(entityName)} (AppPK TEXT(255) NOT NULL, FieldsJson LONGTEXT NULL, __sp_id LONG NULL, __sp_modified_utc TEXT(50) NULL, __sp_etag TEXT(255) NULL, IsDeleted YESNO NOT NULL, DeletedAtUtc TEXT(50) NULL, CONSTRAINT PK_{SanitizeForIndexName(entityName)} PRIMARY KEY (AppPK))");
            EnsureIndex(connection, entityName, "IX_" + SanitizeForIndexName(entityName) + "_IsDeleted", new[] { "IsDeleted" });
            EnsureIndex(connection, entityName, "IX_" + SanitizeForIndexName(entityName) + "___sp_modified_utc", new[] { "__sp_modified_utc" });
            EnsureIndex(connection, entityName, "IX_" + SanitizeForIndexName(entityName) + "_DeletedAtUtc", new[] { "DeletedAtUtc" });
        }

        private static void EnsureTable(OleDbConnection connection, string tableName, string createSql)
        {
            if (TableExists(connection, tableName))
            {
                return;
            }

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = createSql;
                cmd.ExecuteNonQuery();
            }
        }

        private static bool TableExists(OleDbConnection connection, string tableName)
        {
            var schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, new object[] { null, null, tableName, "TABLE" });
            return schema != null && schema.Rows.Count > 0;
        }

        private static void EnsureIndex(OleDbConnection connection, string tableName, string indexName, IReadOnlyList<string> columns)
        {
            if (string.IsNullOrWhiteSpace(indexName) || columns == null || columns.Count == 0)
            {
                return;
            }

            if (IndexExists(connection, tableName, indexName))
            {
                return;
            }

            var cols = string.Join(",", columns.Where(c => !string.IsNullOrWhiteSpace(c)).Select(QuoteIdentifier));
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = $"CREATE INDEX {QuoteIdentifier(indexName)} ON {QuoteIdentifier(tableName)} ({cols})";
                cmd.ExecuteNonQuery();
            }
        }

        private static bool IndexExists(OleDbConnection connection, string tableName, string indexName)
        {
            if (connection == null)
            {
                return false;
            }

            if (string.IsNullOrWhiteSpace(tableName) || string.IsNullOrWhiteSpace(indexName))
            {
                return false;
            }

            DataTable schema;
            try
            {
                schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Indexes, new object[] { null, null, null, null, tableName });
            }
            catch
            {
                schema = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Indexes, null);
            }

            if (schema == null || schema.Rows.Count == 0)
            {
                return false;
            }

            var idxCol = schema.Columns.Contains("INDEX_NAME") ? "INDEX_NAME" : (schema.Columns.Contains("INDEXNAME") ? "INDEXNAME" : null);
            var tblCol = schema.Columns.Contains("TABLE_NAME") ? "TABLE_NAME" : (schema.Columns.Contains("TABLENAME") ? "TABLENAME" : null);

            foreach (DataRow row in schema.Rows)
            {
                var idx = idxCol == null || row.IsNull(idxCol) ? null : Convert.ToString(row[idxCol], CultureInfo.InvariantCulture);
                if (!string.Equals(idx, indexName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (tblCol != null)
                {
                    var tbl = row.IsNull(tblCol) ? null : Convert.ToString(row[tblCol], CultureInfo.InvariantCulture);
                    if (!string.Equals(tbl, tableName, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }
                }

                return true;
            }

            return false;
        }

        private static string QuoteIdentifier(string identifier)
        {
            var trimmed = (identifier ?? string.Empty).Trim();
            if (trimmed.StartsWith("[", StringComparison.Ordinal) && trimmed.EndsWith("]", StringComparison.Ordinal))
            {
                return trimmed;
            }

            return "[" + trimmed.Replace("]", "]]" ) + "]";
        }

        private static string FormatUtc(DateTime dt)
        {
            var utc = dt.Kind == DateTimeKind.Utc ? dt : dt.ToUniversalTime();
            return utc.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);
        }

        private static string FormatUtcOrNull(DateTime? dt)
        {
            if (!dt.HasValue)
            {
                return null;
            }

            return FormatUtc(dt.Value);
        }

        private static DateTime? ParseUtc(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            if (DateTime.TryParseExact(value, "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dt))
            {
                return dt;
            }

            if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out dt))
            {
                return dt;
            }

            return null;
        }

        private static T ParseEnum<T>(string value)
            where T : struct
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return default(T);
            }

            if (Enum.TryParse<T>(value, ignoreCase: true, result: out var parsed))
            {
                return parsed;
            }

            return default(T);
        }

        private static string SanitizeForIndexName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return "IDX";
            }

            var sb = new System.Text.StringBuilder();
            foreach (var ch in name)
            {
                if (char.IsLetterOrDigit(ch) || ch == '_')
                {
                    sb.Append(ch);
                }
                else
                {
                    sb.Append('_');
                }
            }

            var s = sb.ToString();
            if (s.Length > 40)
            {
                s = s.Substring(0, 40);
            }

            return s;
        }
    }
}
