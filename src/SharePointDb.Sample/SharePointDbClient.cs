using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Core;
using SharePointDb.SharePoint;
using SharePointDb.Sqlite;
using SharePointDb.Sync;

namespace SharePointDb.Sample
{
    public sealed class SharePointDbClient : IDisposable
    {
        private readonly SharePointDbClientOptions _options;
        private readonly SqliteLocalStore _localStore;
        private readonly SharePointRestConnector _connector;
        private readonly SharePointConfigurationManager _configurationManager;
        private readonly SharePointSyncEngine _syncEngine;

        private readonly object _tableLocksGate = new object();
        private readonly Dictionary<string, SemaphoreSlim> _tableLocks = new Dictionary<string, SemaphoreSlim>(StringComparer.OrdinalIgnoreCase);

        private LocalConfig _config;

        public SharePointDbClient(SharePointDbClientOptions options, ISharePointCookieProvider cookieProvider)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));

            if (cookieProvider == null)
            {
                throw new ArgumentNullException(nameof(cookieProvider));
            }

            EnsureDirectoryExists(_options.SqliteFilePath);

            _localStore = new SqliteLocalStore(_options.SqliteFilePath);
            _connector = new SharePointRestConnector(new SharePointRestConnectorOptions(_options.SiteUri), cookieProvider);
            _configurationManager = new SharePointConfigurationManager(_connector, _localStore);
            _syncEngine = new SharePointSyncEngine(_connector, _localStore, _localStore);
        }

        public async Task InitializeAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            await _localStore.InitializeAsync(cancellationToken).ConfigureAwait(false);
            await EnsureConfigAsync(cancellationToken).ConfigureAwait(false);
        }

        public Task<LocalConfig> EnsureConfigAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return EnsureConfigCoreAsync(cancellationToken);
        }

        public async Task SyncOnOpenAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var config = await EnsureConfigCoreAsync(cancellationToken).ConfigureAwait(false);

            await _syncEngine.SyncUpAsync(config, cancellationToken: cancellationToken).ConfigureAwait(false);
            await _syncEngine.SyncDownOnOpenAsync(config, cancellationToken).ConfigureAwait(false);
        }

        public async Task SyncAllAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var config = await EnsureConfigCoreAsync(cancellationToken).ConfigureAwait(false);

            await _syncEngine.SyncUpAsync(config, cancellationToken: cancellationToken).ConfigureAwait(false);

            var tables = (config.Tables ?? Array.Empty<AppTableConfig>())
                .Where(t => t != null && t.Enabled)
                .OrderBy(t => t.Priority);

            foreach (var table in tables)
            {
                await _syncEngine.SyncDownAsync(table, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task SyncTableAsync(string entityName, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(entityName))
            {
                throw new ArgumentException("EntityName is required.", nameof(entityName));
            }

            var config = await EnsureConfigCoreAsync(cancellationToken).ConfigureAwait(false);
            var table = FindTableOrThrow(config, entityName);

            var gate = GetTableLock(entityName);
            await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await _syncEngine.SyncUpAsync(config, cancellationToken: cancellationToken).ConfigureAwait(false);
                await _syncEngine.SyncDownAsync(table, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                gate.Release();
            }
        }

        public Task<LocalEntityRow> GetLocalAsync(string entityName, string appPk, CancellationToken cancellationToken = default(CancellationToken))
        {
            return _localStore.GetEntityAsync(entityName, appPk, cancellationToken);
        }

        public async Task UpsertLocalAndEnqueueInsertAsync(
            string entityName,
            string appPk,
            IDictionary<string, object> fields,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var table = await EnsureTableSchemaAsync(entityName, cancellationToken).ConfigureAwait(false);
            var payload = SanitizePayload(table, fields);
            var localPayload = FilterPayloadForLocalMirror(table, payload);

            var existing = await _localStore.GetEntityAsync(entityName, appPk, cancellationToken).ConfigureAwait(false);

            var mergedFields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            if (existing?.Fields != null)
            {
                foreach (var kvp in existing.Fields)
                {
                    mergedFields[kvp.Key] = kvp.Value;
                }
            }

            foreach (var kvp in localPayload)
            {
                mergedFields[kvp.Key] = kvp.Value;
            }

            await _localStore.UpsertEntityAsync(entityName, appPk, mergedFields, existing?.System ?? new LocalEntitySystemFields(), cancellationToken).ConfigureAwait(false);

            await _localStore.EnqueueChangeAsync(new ChangeLogEntry
            {
                EntityName = entityName,
                AppPK = appPk,
                Operation = ChangeOperation.Insert,
                PayloadJson = Json.Serialize(payload),
                CreatedUtc = DateTime.UtcNow,
                Status = ChangeStatus.Pending,
                AttemptCount = 0
            }, cancellationToken).ConfigureAwait(false);
        }

        public async Task UpsertLocalAndEnqueueUpdateAsync(
            string entityName,
            string appPk,
            IDictionary<string, object> fields,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var table = await EnsureTableSchemaAsync(entityName, cancellationToken).ConfigureAwait(false);
            var payload = SanitizePayload(table, fields);
            var localPayload = FilterPayloadForLocalMirror(table, payload);

            var existing = await _localStore.GetEntityAsync(entityName, appPk, cancellationToken).ConfigureAwait(false);

            var mergedFields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            if (existing?.Fields != null)
            {
                foreach (var kvp in existing.Fields)
                {
                    mergedFields[kvp.Key] = kvp.Value;
                }
            }

            foreach (var kvp in localPayload)
            {
                mergedFields[kvp.Key] = kvp.Value;
            }

            await _localStore.UpsertEntityAsync(entityName, appPk, mergedFields, existing?.System ?? new LocalEntitySystemFields(), cancellationToken).ConfigureAwait(false);

            await _localStore.EnqueueChangeAsync(new ChangeLogEntry
            {
                EntityName = entityName,
                AppPK = appPk,
                Operation = ChangeOperation.Update,
                PayloadJson = Json.Serialize(payload),
                CreatedUtc = DateTime.UtcNow,
                Status = ChangeStatus.Pending,
                AttemptCount = 0
            }, cancellationToken).ConfigureAwait(false);
        }

        public async Task MarkLocalDeletedAndEnqueueSoftDeleteAsync(
            string entityName,
            string appPk,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            await EnsureTableSchemaAsync(entityName, cancellationToken).ConfigureAwait(false);

            var existing = await _localStore.GetEntityAsync(entityName, appPk, cancellationToken).ConfigureAwait(false);
            var fields = existing?.Fields ?? new Dictionary<string, object>();
            var system = existing?.System ?? new LocalEntitySystemFields();

            system.IsDeleted = true;
            system.DeletedAtUtc = DateTime.UtcNow;

            await _localStore.UpsertEntityAsync(entityName, appPk, fields, system, cancellationToken).ConfigureAwait(false);

            await _localStore.EnqueueChangeAsync(new ChangeLogEntry
            {
                EntityName = entityName,
                AppPK = appPk,
                Operation = ChangeOperation.SoftDelete,
                PayloadJson = null,
                CreatedUtc = DateTime.UtcNow,
                Status = ChangeStatus.Pending,
                AttemptCount = 0
            }, cancellationToken).ConfigureAwait(false);
        }

        public Task<IReadOnlyList<ConflictLogEntry>> GetRecentConflictsAsync(int maxCount, CancellationToken cancellationToken = default(CancellationToken))
        {
            return _localStore.GetRecentConflictsAsync(maxCount, cancellationToken);
        }

        public void Dispose()
        {
            _connector.Dispose();
            _localStore.Dispose();
        }

        private async Task<LocalConfig> EnsureConfigCoreAsync(CancellationToken cancellationToken)
        {
            _config = await _configurationManager.EnsureLocalConfigUpToDateAsync(_options.AppId, cancellationToken).ConfigureAwait(false);
            return _config;
        }

        private async Task<AppTableConfig> EnsureTableSchemaAsync(string entityName, CancellationToken cancellationToken)
        {
            if (string.IsNullOrWhiteSpace(entityName))
            {
                throw new ArgumentException("EntityName is required.", nameof(entityName));
            }

            var config = await EnsureConfigCoreAsync(cancellationToken).ConfigureAwait(false);
            var table = FindTableOrThrow(config, entityName);

            await _localStore.EnsureEntitySchemaAsync(table, cancellationToken).ConfigureAwait(false);
            return table;
        }

        private static AppTableConfig FindTableOrThrow(LocalConfig config, string entityName)
        {
            if (config?.Tables == null)
            {
                throw new InvalidOperationException("No configuration has been loaded (APP_Tables). Run migration and seed config first.");
            }

            foreach (var t in config.Tables)
            {
                if (t != null && string.Equals(t.EntityName, entityName, StringComparison.OrdinalIgnoreCase))
                {
                    return t;
                }
            }

            throw new InvalidOperationException("Unknown entity/table: " + entityName);
        }

        private SemaphoreSlim GetTableLock(string entityName)
        {
            lock (_tableLocksGate)
            {
                SemaphoreSlim existing;
                if (_tableLocks.TryGetValue(entityName, out existing))
                {
                    return existing;
                }

                var created = new SemaphoreSlim(1, 1);
                _tableLocks[entityName] = created;
                return created;
            }
        }

        private static void EnsureDirectoryExists(string sqliteFilePath)
        {
            var full = Path.GetFullPath(sqliteFilePath);
            var dir = Path.GetDirectoryName(full);

            if (!string.IsNullOrWhiteSpace(dir) && !Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }
        }

        private static IDictionary<string, object> SanitizePayload(AppTableConfig table, IDictionary<string, object> fields)
        {
            var dict = fields == null
                ? new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object>(fields, StringComparer.OrdinalIgnoreCase);

            dict.Remove("AppPK");
            dict.Remove("__sp_id");
            dict.Remove("__sp_modified_utc");
            dict.Remove("__sp_etag");
            dict.Remove("IsDeleted");
            dict.Remove("DeletedAtUtc");

            if (!string.IsNullOrWhiteSpace(table?.PkInternalName) && !string.Equals(table.PkInternalName, "AppPK", StringComparison.OrdinalIgnoreCase))
            {
                dict.Remove(table.PkInternalName);
            }

            return dict;
        }

        private static IDictionary<string, object> FilterPayloadForLocalMirror(AppTableConfig table, IDictionary<string, object> payload)
        {
            if (payload == null || payload.Count == 0)
            {
                return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            }

            var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (table?.SelectFields != null)
            {
                foreach (var f in table.SelectFields)
                {
                    if (!string.IsNullOrWhiteSpace(f))
                    {
                        allowed.Add(f);
                    }
                }
            }

            allowed.Remove("AppPK");
            allowed.Remove("__sp_id");
            allowed.Remove("__sp_modified_utc");
            allowed.Remove("__sp_etag");
            allowed.Remove("IsDeleted");
            allowed.Remove("DeletedAtUtc");

            if (!string.IsNullOrWhiteSpace(table?.PkInternalName))
            {
                allowed.Remove(table.PkInternalName);
            }

            var filtered = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in payload)
            {
                if (!string.IsNullOrWhiteSpace(kvp.Key) && allowed.Contains(kvp.Key))
                {
                    filtered[kvp.Key] = kvp.Value;
                }
            }

            return filtered;
        }
    }
}
