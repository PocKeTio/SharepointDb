using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Core;

namespace SharePointDb.Sync
{
    public sealed class SharePointSyncEngine
    {
        private static readonly TimeSpan DefaultWatermarkOverlap = TimeSpan.FromMinutes(5);

        private readonly ISharePointConnector _sharePoint;
        private readonly ILocalStore _localStore;
        private readonly ILocalEntityStore _entityStore;

        public SharePointSyncEngine(ISharePointConnector sharePoint, ILocalStore localStore, ILocalEntityStore entityStore)
        {
            _sharePoint = sharePoint ?? throw new ArgumentNullException(nameof(sharePoint));
            _localStore = localStore ?? throw new ArgumentNullException(nameof(localStore));
            _entityStore = entityStore ?? throw new ArgumentNullException(nameof(entityStore));
        }

        public async Task SyncDownOnOpenAsync(LocalConfig config, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var tables = (config.Tables ?? Array.Empty<AppTableConfig>())
                .Where(t => t != null && t.Enabled && t.SyncPolicy == SyncPolicy.OnOpen)
                .OrderBy(t => t.Priority);

            foreach (var table in tables)
            {
                await SyncDownAsync(table, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task SyncDownAsync(AppTableConfig tableConfig, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (tableConfig == null)
            {
                throw new ArgumentNullException(nameof(tableConfig));
            }

            if (string.IsNullOrWhiteSpace(tableConfig.EntityName))
            {
                throw new ArgumentException("EntityName is required.", nameof(tableConfig));
            }

            await _entityStore.EnsureEntitySchemaAsync(tableConfig, cancellationToken).ConfigureAwait(false);

            var state = await _localStore.GetSyncStateAsync(tableConfig.EntityName, cancellationToken).ConfigureAwait(false);

            var select = BuildSyncSelectFields(tableConfig);
            var filter = BuildIncrementalFilter(state, DefaultWatermarkOverlap);

            var query = new SharePointListQuery
            {
                SelectFields = select,
                Filter = filter,
                OrderBy = "Modified asc, Id asc",
                Top = 200
            };

            DateTime? maxModified = state.LastSyncModifiedUtc;
            int? maxId = state.LastSyncSpId;

            string next = null;
            do
            {
                query.NextPageUrl = next;
                var page = await _sharePoint.QueryListItemsAsync(tableConfig.ListId, query, cancellationToken).ConfigureAwait(false);

                if (page.Items != null)
                {
                    foreach (var item in page.Items)
                    {
                        var appPk = GetFieldAsString(item.Fields, tableConfig.PkInternalName);
                        if (string.IsNullOrWhiteSpace(appPk))
                        {
                            continue;
                        }

                        var localFields = ExtractLocalFields(tableConfig, item.Fields);

                        var system = new LocalEntitySystemFields
                        {
                            SharePointId = item.Id,
                            SharePointModifiedUtc = item.ModifiedUtc,
                            SharePointETag = item.ETag,
                            IsDeleted = GetFieldAsBool(item.Fields, "IsDeleted"),
                            DeletedAtUtc = GetFieldAsDateTimeUtc(item.Fields, "DeletedAtUtc")
                        };

                        await _entityStore.UpsertEntityAsync(tableConfig.EntityName, appPk, localFields, system, cancellationToken).ConfigureAwait(false);

                        if (item.ModifiedUtc.HasValue)
                        {
                            if (!maxModified.HasValue || item.ModifiedUtc.Value > maxModified.Value)
                            {
                                maxModified = item.ModifiedUtc.Value;
                                maxId = item.Id;
                            }
                            else if (item.ModifiedUtc.Value == maxModified.Value)
                            {
                                if (!maxId.HasValue || item.Id > maxId.Value)
                                {
                                    maxId = item.Id;
                                }
                            }
                        }
                    }
                }

                next = page.NextPageUrl;
            }
            while (!string.IsNullOrWhiteSpace(next));

            state.LastSyncModifiedUtc = maxModified;
            state.LastSyncSpId = maxId;
            state.LastSuccessfulSyncUtc = DateTime.UtcNow;
            state.LastError = null;

            await _localStore.UpsertSyncStateAsync(state, cancellationToken).ConfigureAwait(false);
        }

        public async Task SyncUpAsync(LocalConfig config, int maxChanges = 100, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (config == null)
            {
                throw new ArgumentNullException(nameof(config));
            }

            var pending = await _localStore.GetPendingChangesAsync(maxChanges, cancellationToken).ConfigureAwait(false);
            if (pending == null || pending.Count == 0)
            {
                return;
            }

            foreach (var change in pending)
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    var table = FindTable(config, change.EntityName);
                    if (table == null)
                    {
                        await _localStore.MarkChangeFailedAsync(change.Id, "Unknown entity/table.", cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    await _entityStore.EnsureEntitySchemaAsync(table, cancellationToken).ConfigureAwait(false);

                    var payload = Json.Deserialize<Dictionary<string, object>>(change.PayloadJson) ?? new Dictionary<string, object>();

                    if (change.Operation == ChangeOperation.Insert)
                    {
                        payload[table.PkInternalName] = change.AppPK;

                        if (!payload.ContainsKey("Title"))
                        {
                            payload["Title"] = change.AppPK;
                        }

                        int createdId;
                        try
                        {
                            createdId = await _sharePoint.CreateListItemAsync(table.ListId, payload, cancellationToken).ConfigureAwait(false);
                        }
                        catch (SharePointRequestException ex)
                        {
                            if (!IsAlreadyExistsConflict(ex))
                            {
                                throw;
                            }

                            var serverItem = await TryGetServerItemByAppPkAsync(table, change.AppPK, cancellationToken).ConfigureAwait(false);
                            if (serverItem == null)
                            {
                                throw;
                            }

                            var applied = await HandleInsertAlreadyExistsAsync(change, table, payload, serverItem, cancellationToken).ConfigureAwait(false);
                            if (!applied)
                            {
                                continue;
                            }

                            createdId = serverItem.Id;
                        }

                        var existingLocal = await _entityStore.GetEntityAsync(change.EntityName, change.AppPK, cancellationToken).ConfigureAwait(false);
                        var localFields = existingLocal?.Fields ?? ExtractLocalFields(table, payload);
                        var system = existingLocal?.System ?? new LocalEntitySystemFields();

                        await _entityStore.UpsertEntityAsync(change.EntityName, change.AppPK, localFields, new LocalEntitySystemFields
                        {
                            SharePointId = createdId,
                            SharePointModifiedUtc = system.SharePointModifiedUtc,
                            SharePointETag = system.SharePointETag,
                            IsDeleted = system.IsDeleted,
                            DeletedAtUtc = system.DeletedAtUtc
                        }, cancellationToken).ConfigureAwait(false);

                        try
                        {
                            await RefreshMirrorFromServerAsync(table, change.AppPK, createdId, cancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                        }
                    }
                    else if (change.Operation == ChangeOperation.Update)
                    {
                        var local = await _entityStore.GetEntityAsync(change.EntityName, change.AppPK, cancellationToken).ConfigureAwait(false);
                        var itemId = local?.System?.SharePointId;

                        if (!itemId.HasValue)
                        {
                            itemId = await ResolveSharePointIdByAppPkAsync(table, change.AppPK, cancellationToken).ConfigureAwait(false);
                        }

                        if (!itemId.HasValue)
                        {
                            await _localStore.MarkChangeFailedAsync(change.Id, "Cannot resolve SharePoint ID for update.", cancellationToken).ConfigureAwait(false);
                            continue;
                        }

                        payload[table.PkInternalName] = change.AppPK;

                        try
                        {
                            await _sharePoint.UpdateListItemAsync(table.ListId, itemId.Value, payload, local?.System?.SharePointETag, cancellationToken).ConfigureAwait(false);
                        }
                        catch (SharePointRequestException ex)
                        {
                            if (!IsConcurrencyConflict(ex))
                            {
                                throw;
                            }

                            var resolved = await ResolveConcurrencyConflictAsync(change, table, local, itemId.Value, payload, cancellationToken).ConfigureAwait(false);
                            if (!resolved)
                            {
                                continue;
                            }
                        }

                        try
                        {
                            await RefreshMirrorFromServerAsync(table, change.AppPK, itemId.Value, cancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                        }
                    }
                    else if (change.Operation == ChangeOperation.SoftDelete)
                    {
                        var local = await _entityStore.GetEntityAsync(change.EntityName, change.AppPK, cancellationToken).ConfigureAwait(false);
                        var itemId = local?.System?.SharePointId;

                        if (!itemId.HasValue)
                        {
                            itemId = await ResolveSharePointIdByAppPkAsync(table, change.AppPK, cancellationToken).ConfigureAwait(false);
                        }

                        if (!itemId.HasValue)
                        {
                            await _localStore.MarkChangeFailedAsync(change.Id, "Cannot resolve SharePoint ID for delete.", cancellationToken).ConfigureAwait(false);
                            continue;
                        }

                        var update = new Dictionary<string, object>
                        {
                            { table.PkInternalName, change.AppPK },
                            { "IsDeleted", true },
                            { "DeletedAtUtc", DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture) }
                        };


                        try
                        {
                            await _sharePoint.UpdateListItemAsync(table.ListId, itemId.Value, update, local?.System?.SharePointETag, cancellationToken).ConfigureAwait(false);
                        }
                        catch (SharePointRequestException ex)
                        {
                            if (!IsConcurrencyConflict(ex))
                            {
                                throw;
                            }

                            var resolved = await ResolveConcurrencyConflictAsync(change, table, local, itemId.Value, update, cancellationToken).ConfigureAwait(false);
                            if (!resolved)
                            {
                                continue;
                            }
                        }

                        try
                        {
                            await RefreshMirrorFromServerAsync(table, change.AppPK, itemId.Value, cancellationToken).ConfigureAwait(false);
                        }
                        catch
                        {
                        }
                    }

                    await _localStore.MarkChangeAppliedAsync(change.Id, DateTime.UtcNow, cancellationToken).ConfigureAwait(false);
                }
                catch (SharePointRequestException ex)
                {
                    await _localStore.MarkChangeFailedAsync(change.Id, ex.Message, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await _localStore.MarkChangeFailedAsync(change.Id, ex.Message, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        private async Task<int?> ResolveSharePointIdByAppPkAsync(AppTableConfig table, string appPk, CancellationToken cancellationToken)
        {
            var filter = $"{table.PkInternalName} eq '{EscapeODataString(appPk)}'";

            var query = new SharePointListQuery
            {
                SelectFields = new[] { table.PkInternalName },
                Filter = filter,
                OrderBy = "Id asc",
                Top = 1
            };

            var page = await _sharePoint.QueryListItemsAsync(table.ListId, query, cancellationToken).ConfigureAwait(false);
            if (page.Items == null || page.Items.Count == 0)
            {
                return null;
            }

            return page.Items[0].Id;
        }

        private static IReadOnlyList<string> BuildSyncSelectFields(AppTableConfig tableConfig)
        {
            var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (tableConfig.SelectFields != null)
            {
                foreach (var f in tableConfig.SelectFields)
                {
                    if (!string.IsNullOrWhiteSpace(f))
                    {
                        set.Add(f);
                    }
                }
            }

            if (!string.IsNullOrWhiteSpace(tableConfig.PkInternalName))
            {
                set.Add(tableConfig.PkInternalName);
            }

            set.Add("IsDeleted");
            set.Add("DeletedAtUtc");

            return set.ToList();
        }

        private static string BuildIncrementalFilter(SyncState state, TimeSpan overlap)
        {
            if (state == null || !state.LastSyncModifiedUtc.HasValue)
            {
                return null;
            }

            var dt = state.LastSyncModifiedUtc.Value.ToUniversalTime();

            if (overlap > TimeSpan.Zero)
            {
                var overlapped = dt - overlap;
                var overlappedLiteral = $"datetime'{overlapped:yyyy-MM-ddTHH:mm:ssZ}'";
                return $"Modified ge {overlappedLiteral}";
            }

            var dtLiteral = $"datetime'{dt:yyyy-MM-ddTHH:mm:ssZ}'";

            if (state.LastSyncSpId.HasValue)
            {
                return $"(Modified gt {dtLiteral}) or (Modified eq {dtLiteral} and Id gt {state.LastSyncSpId.Value})";
            }

            return $"Modified gt {dtLiteral}";
        }

        private static bool IsConcurrencyConflict(SharePointRequestException ex)
        {
            if (ex == null)
            {
                return false;
            }

            if (ex.StatusCode == 409 || ex.StatusCode == 412)
            {
                return true;
            }

            if (ex.StatusCode == 400 && !string.IsNullOrWhiteSpace(ex.ResponseContent))
            {
                if (ex.ResponseContent.IndexOf("etag", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    ex.ResponseContent.IndexOf("precondition", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
            }

            return false;
        }

        private static bool IsAlreadyExistsConflict(SharePointRequestException ex)
        {
            if (ex == null)
            {
                return false;
            }

            if (ex.StatusCode == 409)
            {
                return true;
            }

            if ((ex.StatusCode == 400 || ex.StatusCode == 500) && !string.IsNullOrWhiteSpace(ex.ResponseContent))
            {
                if (ex.ResponseContent.IndexOf("unique", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    ex.ResponseContent.IndexOf("already", StringComparison.OrdinalIgnoreCase) >= 0 ||
                    ex.ResponseContent.IndexOf("duplicate", StringComparison.OrdinalIgnoreCase) >= 0)
                {
                    return true;
                }
            }

            return false;
        }

        private async Task<SharePointListItem> TryGetServerItemByAppPkAsync(AppTableConfig table, string appPk, CancellationToken cancellationToken)
        {
            var filter = $"{table.PkInternalName} eq '{EscapeODataString(appPk)}'";

            var query = new SharePointListQuery
            {
                SelectFields = BuildSyncSelectFields(table),
                Filter = filter,
                OrderBy = "Id asc",
                Top = 1
            };

            var page = await _sharePoint.QueryListItemsAsync(table.ListId, query, cancellationToken).ConfigureAwait(false);
            if (page.Items == null || page.Items.Count == 0)
            {
                return null;
            }

            return page.Items[0];
        }

        private async Task RefreshMirrorFromServerAsync(AppTableConfig table, string appPk, int itemId, CancellationToken cancellationToken)
        {
            SharePointListItem serverItem;

            try
            {
                serverItem = await _sharePoint.GetListItemAsync(table.ListId, itemId, BuildSyncSelectFields(table), cancellationToken).ConfigureAwait(false);
            }
            catch (SharePointRequestException)
            {
                serverItem = await TryGetServerItemByAppPkAsync(table, appPk, cancellationToken).ConfigureAwait(false);
                if (serverItem == null)
                {
                    throw;
                }
            }

            await UpsertMirrorFromServerItemAsync(table, appPk, serverItem, cancellationToken).ConfigureAwait(false);
        }

        private async Task<bool> HandleInsertAlreadyExistsAsync(
            ChangeLogEntry change,
            AppTableConfig table,
            IDictionary<string, object> payload,
            SharePointListItem existingServerItem,
            CancellationToken cancellationToken)
        {
            var policy = table.ConflictPolicy;

            await _localStore.LogConflictAsync(new ConflictLogEntry
            {
                OccurredUtc = DateTime.UtcNow,
                EntityName = change.EntityName,
                AppPK = change.AppPK,
                ChangeId = change.Id,
                Operation = change.Operation,
                Policy = policy,
                SharePointId = existingServerItem.Id,
                LocalETag = null,
                ServerETag = existingServerItem.ETag,
                LocalPayloadJson = change.PayloadJson,
                ServerFieldsJson = Json.Serialize(existingServerItem.Fields),
                Message = "Insert conflict: item already exists."
            }, cancellationToken).ConfigureAwait(false);

            if (policy == ConflictResolutionPolicy.Manual)
            {
                await _localStore.MarkChangeConflictedAsync(change.Id, "Insert conflict: item already exists.", cancellationToken).ConfigureAwait(false);
                await UpsertMirrorFromServerItemAsync(table, change.AppPK, existingServerItem, cancellationToken).ConfigureAwait(false);
                return false;
            }

            if (policy == ConflictResolutionPolicy.ClientWins)
            {
                try
                {
                    await _sharePoint.UpdateListItemAsync(table.ListId, existingServerItem.Id, payload, existingServerItem.ETag, cancellationToken).ConfigureAwait(false);
                    await RefreshMirrorFromServerAsync(table, change.AppPK, existingServerItem.Id, cancellationToken).ConfigureAwait(false);
                    return true;
                }
                catch (SharePointRequestException ex)
                {
                    if (IsConcurrencyConflict(ex))
                    {
                        await _localStore.MarkChangeConflictedAsync(change.Id, "Insert conflict persists after update retry.", cancellationToken).ConfigureAwait(false);
                        return false;
                    }

                    throw;
                }
            }

            await UpsertMirrorFromServerItemAsync(table, change.AppPK, existingServerItem, cancellationToken).ConfigureAwait(false);
            return true;
        }

        private async Task<bool> ResolveConcurrencyConflictAsync(
            ChangeLogEntry change,
            AppTableConfig table,
            LocalEntityRow local,
            int itemId,
            IDictionary<string, object> desiredPayload,
            CancellationToken cancellationToken)
        {
            SharePointListItem serverItem = null;
            try
            {
                serverItem = await _sharePoint.GetListItemAsync(table.ListId, itemId, BuildSyncSelectFields(table), cancellationToken).ConfigureAwait(false);
            }
            catch
            {
            }

            var policy = table.ConflictPolicy;
            var serverFieldsJson = serverItem == null ? null : Json.Serialize(serverItem.Fields);

            await _localStore.LogConflictAsync(new ConflictLogEntry
            {
                OccurredUtc = DateTime.UtcNow,
                EntityName = change.EntityName,
                AppPK = change.AppPK,
                ChangeId = change.Id,
                Operation = change.Operation,
                Policy = policy,
                SharePointId = itemId,
                LocalETag = local?.System?.SharePointETag,
                ServerETag = serverItem?.ETag,
                LocalPayloadJson = change.PayloadJson,
                ServerFieldsJson = serverFieldsJson,
                Message = "Concurrency conflict (ETag mismatch)."
            }, cancellationToken).ConfigureAwait(false);

            if (policy == ConflictResolutionPolicy.Manual)
            {
                await _localStore.MarkChangeConflictedAsync(change.Id, "Concurrency conflict (ETag mismatch).", cancellationToken).ConfigureAwait(false);

                if (serverItem != null)
                {
                    await UpsertMirrorFromServerItemAsync(table, change.AppPK, serverItem, cancellationToken).ConfigureAwait(false);
                }

                return false;
            }

            if (policy == ConflictResolutionPolicy.ServerWins)
            {
                if (serverItem != null)
                {
                    await UpsertMirrorFromServerItemAsync(table, change.AppPK, serverItem, cancellationToken).ConfigureAwait(false);
                }

                return true;
            }

            var retryETag = serverItem?.ETag;
            try
            {
                await _sharePoint.UpdateListItemAsync(table.ListId, itemId, desiredPayload, string.IsNullOrWhiteSpace(retryETag) ? "*" : retryETag, cancellationToken).ConfigureAwait(false);
                await RefreshMirrorFromServerAsync(table, change.AppPK, itemId, cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (SharePointRequestException ex)
            {
                if (IsConcurrencyConflict(ex))
                {
                    await _localStore.MarkChangeConflictedAsync(change.Id, "Concurrency conflict persists after retry.", cancellationToken).ConfigureAwait(false);
                    return false;
                }

                throw;
            }
        }

        private Task UpsertMirrorFromServerItemAsync(AppTableConfig tableConfig, string appPk, SharePointListItem serverItem, CancellationToken cancellationToken)
        {
            if (serverItem == null)
            {
                return Task.CompletedTask;
            }

            var localFields = ExtractLocalFields(tableConfig, serverItem.Fields);
            var system = new LocalEntitySystemFields
            {
                SharePointId = serverItem.Id,
                SharePointModifiedUtc = serverItem.ModifiedUtc,
                SharePointETag = serverItem.ETag,
                IsDeleted = GetFieldAsBool(serverItem.Fields, "IsDeleted"),
                DeletedAtUtc = GetFieldAsDateTimeUtc(serverItem.Fields, "DeletedAtUtc")
            };

            return _entityStore.UpsertEntityAsync(tableConfig.EntityName, appPk, localFields, system, cancellationToken);
        }

        private static AppTableConfig FindTable(LocalConfig config, string entityName)
        {
            if (config?.Tables == null || string.IsNullOrWhiteSpace(entityName))
            {
                return null;
            }

            foreach (var t in config.Tables)
            {
                if (t != null && string.Equals(t.EntityName, entityName, StringComparison.OrdinalIgnoreCase))
                {
                    return t;
                }
            }

            return null;
        }

        private static IReadOnlyDictionary<string, object> ExtractLocalFields(AppTableConfig tableConfig, IReadOnlyDictionary<string, object> itemFields)
        {
            var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            if (tableConfig.SelectFields == null || itemFields == null)
            {
                return dict;
            }

            foreach (var f in tableConfig.SelectFields)
            {
                if (string.IsNullOrWhiteSpace(f))
                {
                    continue;
                }

                if (string.Equals(f, tableConfig.PkInternalName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (string.Equals(f, "IsDeleted", StringComparison.OrdinalIgnoreCase) || string.Equals(f, "DeletedAtUtc", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                object value;
                if (itemFields.TryGetValue(f, out value))
                {
                    dict[f] = value;
                }
            }

            return dict;
        }

        private static string GetFieldAsString(IReadOnlyDictionary<string, object> fields, string fieldName)
        {
            if (fields == null || string.IsNullOrWhiteSpace(fieldName))
            {
                return null;
            }

            object value;
            if (!fields.TryGetValue(fieldName, out value) || value == null)
            {
                return null;
            }

            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        private static bool GetFieldAsBool(IReadOnlyDictionary<string, object> fields, string fieldName)
        {
            var text = GetFieldAsString(fields, fieldName);
            if (string.IsNullOrWhiteSpace(text))
            {
                return false;
            }

            bool b;
            if (bool.TryParse(text, out b))
            {
                return b;
            }

            return string.Equals(text, "1", StringComparison.OrdinalIgnoreCase) || string.Equals(text, "yes", StringComparison.OrdinalIgnoreCase);
        }

        private static DateTime? GetFieldAsDateTimeUtc(IReadOnlyDictionary<string, object> fields, string fieldName)
        {
            var text = GetFieldAsString(fields, fieldName);
            if (string.IsNullOrWhiteSpace(text))
            {
                return null;
            }

            return ParseSharePointDateUtc(text);
        }

        private static string EscapeODataString(string value)
        {
            return (value ?? string.Empty).Replace("'", "''");
        }

        private static DateTime? ParseSharePointDateUtc(string text)
        {
            if (string.IsNullOrWhiteSpace(text))
            {
                return null;
            }

            if (text.StartsWith("/Date(", StringComparison.OrdinalIgnoreCase))
            {
                var start = text.IndexOf('(');
                var end = text.IndexOf(')');
                if (start >= 0 && end > start)
                {
                    var inner = text.Substring(start + 1, end - start - 1);

                    var tzIndex = inner.IndexOf('+');
                    if (tzIndex < 0)
                    {
                        tzIndex = inner.IndexOf('-', 1);
                    }

                    if (tzIndex > 0)
                    {
                        inner = inner.Substring(0, tzIndex);
                    }

                    long ms;
                    if (long.TryParse(inner, NumberStyles.Integer, CultureInfo.InvariantCulture, out ms))
                    {
                        return DateTimeOffset.FromUnixTimeMilliseconds(ms).UtcDateTime;
                    }
                }
            }

            DateTime dt;
            if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out dt))
            {
                return DateTime.SpecifyKind(dt, DateTimeKind.Utc).ToUniversalTime();
            }

            return null;
        }
    }
}
