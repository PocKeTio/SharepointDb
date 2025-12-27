using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Core;

namespace SharePointDb.Sync
{
    public sealed class SharePointConfigurationManager
    {
        private readonly ISharePointConnector _sharePoint;
        private readonly ILocalStore _localStore;

        public SharePointConfigurationManager(ISharePointConnector sharePoint, ILocalStore localStore)
        {
            _sharePoint = sharePoint ?? throw new ArgumentNullException(nameof(sharePoint));
            _localStore = localStore ?? throw new ArgumentNullException(nameof(localStore));
        }

        public async Task<LocalConfig> EnsureLocalConfigUpToDateAsync(string appId, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(appId))
            {
                throw new ArgumentException("AppId is required.", nameof(appId));
            }

            var local = await _localStore.GetLocalConfigAsync(appId, cancellationToken).ConfigureAwait(false);

            var remote = await GetRemoteAppConfigAsync(appId, cancellationToken).ConfigureAwait(false);
            if (remote == null)
            {
                return local;
            }

            if (remote.ConfigVersion <= local.ConfigVersion)
            {
                return local;
            }

            var tables = await GetRemoteTablesAsync(cancellationToken).ConfigureAwait(false);

            var updated = new LocalConfig
            {
                AppId = appId,
                ConfigVersion = remote.ConfigVersion,
                Tables = tables,
                UpdatedUtc = DateTime.UtcNow
            };

            await _localStore.UpsertLocalConfigAsync(updated, cancellationToken).ConfigureAwait(false);
            return updated;
        }

        public async Task<AppConfig> GetRemoteAppConfigAsync(string appId, CancellationToken cancellationToken = default(CancellationToken))
        {
            var configListId = await _sharePoint.GetListIdByTitleAsync("APP_Config", cancellationToken).ConfigureAwait(false);

            var query = new SharePointListQuery
            {
                SelectFields = new[] { "AppId", "ConfigVersion", "MinClientVersion", "LastModifiedUtc" },
                Top = 1,
                Filter = $"AppId eq '{EscapeODataString(appId)}'"
            };

            var page = await _sharePoint.QueryListItemsAsync(configListId, query, cancellationToken).ConfigureAwait(false);
            if (page.Items == null || page.Items.Count == 0)
            {
                return null;
            }

            var fields = page.Items[0].Fields;

            return new AppConfig
            {
                AppId = GetString(fields, "AppId"),
                ConfigVersion = GetInt(fields, "ConfigVersion"),
                MinClientVersion = GetString(fields, "MinClientVersion"),
                LastModifiedUtc = GetDateTimeUtc(fields, "LastModifiedUtc")
            };
        }

        public async Task<IReadOnlyList<AppTableConfig>> GetRemoteTablesAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            var tablesListId = await _sharePoint.GetListIdByTitleAsync("APP_Tables", cancellationToken).ConfigureAwait(false);

            var selectFields = new[]
            {
                "EntityName",
                "ListId",
                "ListTitle",
                "Enabled",
                "PkInternalName",
                "SelectFieldsJson",
                "SyncPolicy",
                "Priority",
                "AttachmentsMode",
                "PartitionStrategy",
                "ConflictPolicy",
                "ExpectedIndexesJson"
            };

            var query = new SharePointListQuery
            {
                SelectFields = selectFields,
                Top = 200,
                OrderBy = "Priority asc"
            };

            var all = new List<AppTableConfig>();
            string next = null;

            do
            {
                query.NextPageUrl = next;

                SharePointListItemPage page;
                try
                {
                    page = await _sharePoint.QueryListItemsAsync(tablesListId, query, cancellationToken).ConfigureAwait(false);
                }
                catch (SharePointRequestException ex)
                {
                    if (ex.StatusCode != 400 || string.IsNullOrWhiteSpace(ex.ResponseContent) || ex.ResponseContent.IndexOf("ConflictPolicy", StringComparison.OrdinalIgnoreCase) < 0)
                    {
                        throw;
                    }

                    query.SelectFields = selectFields.Where(f => !string.Equals(f, "ConflictPolicy", StringComparison.OrdinalIgnoreCase)).ToArray();
                    page = await _sharePoint.QueryListItemsAsync(tablesListId, query, cancellationToken).ConfigureAwait(false);
                }

                if (page.Items != null)
                {
                    foreach (var item in page.Items)
                    {
                        var f = item.Fields;

                        var listIdText = GetString(f, "ListId");
                        Guid listId;
                        if (!Guid.TryParse(listIdText, out listId))
                        {
                            continue;
                        }

                        var selectJson = GetString(f, "SelectFieldsJson");
                        var expectedIdxJson = GetString(f, "ExpectedIndexesJson");

                        var select = Json.Deserialize<List<string>>(selectJson) ?? new List<string>();
                        var expectedIdx = Json.Deserialize<List<string>>(expectedIdxJson) ?? new List<string>();

                        all.Add(new AppTableConfig
                        {
                            EntityName = GetString(f, "EntityName"),
                            ListId = listId,
                            ListTitle = GetString(f, "ListTitle"),
                            Enabled = GetBool(f, "Enabled"),
                            PkInternalName = GetString(f, "PkInternalName") ?? "AppPK",
                            SelectFields = select,
                            SyncPolicy = ParseEnum<SyncPolicy>(GetString(f, "SyncPolicy")),
                            Priority = GetInt(f, "Priority"),
                            AttachmentsMode = ParseEnum<AttachmentsMode>(GetString(f, "AttachmentsMode")),
                            PartitionStrategy = ParseEnum<PartitionStrategy>(GetString(f, "PartitionStrategy")),
                            ConflictPolicy = ParseEnum<ConflictResolutionPolicy>(GetString(f, "ConflictPolicy")),
                            ExpectedIndexes = expectedIdx
                        });
                    }
                }

                next = page.NextPageUrl;
            }
            while (!string.IsNullOrWhiteSpace(next));

            return all;
        }

        private static string GetString(IReadOnlyDictionary<string, object> fields, string name)
        {
            if (fields == null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            object value;
            if (!fields.TryGetValue(name, out value) || value == null)
            {
                return null;
            }

            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        private static int GetInt(IReadOnlyDictionary<string, object> fields, string name)
        {
            var text = GetString(fields, name);
            if (string.IsNullOrWhiteSpace(text))
            {
                return 0;
            }

            int i;
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out i))
            {
                return i;
            }

            long l;
            if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out l))
            {
                return (int)l;
            }

            double d;
            if (double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out d))
            {
                return (int)d;
            }

            return 0;
        }

        private static bool GetBool(IReadOnlyDictionary<string, object> fields, string name)
        {
            var text = GetString(fields, name);
            if (string.IsNullOrWhiteSpace(text))
            {
                return false;
            }

            bool b;
            if (bool.TryParse(text, out b))
            {
                return b;
            }

            if (string.Equals(text, "1", StringComparison.OrdinalIgnoreCase) || string.Equals(text, "yes", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            return false;
        }

        private static DateTime? GetDateTimeUtc(IReadOnlyDictionary<string, object> fields, string name)
        {
            var text = GetString(fields, name);
            if (string.IsNullOrWhiteSpace(text))
            {
                return null;
            }

            return ParseSharePointDateUtc(text);
        }

        private static T ParseEnum<T>(string value) where T : struct
        {
            T parsed;
            if (Enum.TryParse(value ?? string.Empty, true, out parsed))
            {
                return parsed;
            }

            if (!string.IsNullOrWhiteSpace(value))
            {
                double d;
                if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out d))
                {
                    var i = (int)d;
                    return (T)Enum.ToObject(typeof(T), i);
                }
            }

            return default(T);
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
