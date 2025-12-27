using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Access;
using SharePointDb.Core;
using SharePointDb.SharePoint;

namespace SharePointDb.Migration
{
    internal static class AccessImportCommand
    {
        private const int DefaultTextMaxLength = 255;
        private const int SharePointInternalNameMaxLength = 64;
        private const string DefaultPkInternalName = "AppPK";

        public static async Task RunAsync(
            SharePointRestConnector connector,
            Guid configListId,
            Guid tablesListId,
            string appId,
            string[] args,
            CancellationToken cancellationToken)
        {
            if (connector == null)
            {
                throw new ArgumentNullException(nameof(connector));
            }

            if (string.IsNullOrWhiteSpace(appId))
            {
                throw new ArgumentException("AppId is required.", nameof(appId));
            }

            if (args == null)
            {
                throw new ArgumentNullException(nameof(args));
            }

            var accessFile = GetArg(args, "--access");
            var tableName = GetArg(args, "--table");
            if (string.IsNullOrWhiteSpace(accessFile) || string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("access-import requires --access <file.accdb> and --table <TableName>.");
            }

            var entityName = GetArg(args, "--entity");
            if (string.IsNullOrWhiteSpace(entityName))
            {
                entityName = tableName;
            }

            var pkSourceColumn = GetArg(args, "--pk");
            var maxRows = GetIntArg(args, "--max");

            Console.WriteLine($"Access import: file='{accessFile}', table='{tableName}', entity='{entityName}'");

            var schema = AccessTableReader.GetTableSchema(accessFile, tableName);
            if (schema == null || schema.Count == 0)
            {
                throw new InvalidOperationException("Access table schema is empty.");
            }

            pkSourceColumn = ResolvePkSourceColumn(pkSourceColumn, schema);
            var titleSourceColumn = FindColumnName(schema, "Title");

            Console.WriteLine($"Access import: PK source column='{(pkSourceColumn ?? "<none>")}'");
            if (!string.IsNullOrWhiteSpace(titleSourceColumn))
            {
                Console.WriteLine($"Access import: Title source column='{titleSourceColumn}'");
            }

            var mappings = BuildMappings(schema, pkSourceColumn, titleSourceColumn);

            var listTitle = entityName;
            Console.WriteLine($"Ensuring SharePoint list '{listTitle}'...");
            var entityListId = await connector.EnsureListAsync(listTitle, $"Imported from Access ({tableName})", baseTemplate: 100, cancellationToken: cancellationToken).ConfigureAwait(false);

            Console.WriteLine("Ensuring SharePoint fields...");
            await EnsureTextFieldAsync(connector, entityListId, internalName: DefaultPkInternalName, displayName: DefaultPkInternalName, required: true, maxLength: DefaultTextMaxLength, enforceUnique: true, indexed: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureBooleanFieldAsync(connector, entityListId, internalName: "IsDeleted", displayName: "IsDeleted", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureDateTimeFieldAsync(connector, entityListId, internalName: "DeletedAtUtc", displayName: "DeletedAtUtc", required: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            foreach (var mapping in mappings)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await EnsureFieldForMappingAsync(connector, entityListId, mapping, cancellationToken).ConfigureAwait(false);
            }

            Console.WriteLine("Upserting APP_Tables entry...");
            await UpsertAppTablesRowAsync(connector, tablesListId, entityName, entityListId, listTitle, mappings, cancellationToken).ConfigureAwait(false);

            Console.WriteLine("Importing rows...");
            var result = ImportRows(connector, entityListId, accessFile, tableName, pkSourceColumn, titleSourceColumn, mappings, maxRows, cancellationToken);
            Console.WriteLine($"Import completed: read={result.Read.ToString(CultureInfo.InvariantCulture)}, created={result.Created.ToString(CultureInfo.InvariantCulture)}, updated={result.Updated.ToString(CultureInfo.InvariantCulture)}, errors={result.Errors.ToString(CultureInfo.InvariantCulture)}");

            Console.WriteLine("Bumping APP_Config.ConfigVersion...");
            await BumpConfigVersionAsync(connector, configListId, appId, cancellationToken).ConfigureAwait(false);
        }

        private enum SharePointFieldKind
        {
            Text = 0,
            Note = 1,
            Number = 2,
            Boolean = 3,
            DateTime = 4,
            Guid = 5
        }

        private sealed class AccessFieldMapping
        {
            public string AccessColumnName { get; set; }
            public string SharePointInternalName { get; set; }
            public string SharePointDisplayName { get; set; }
            public SharePointFieldKind FieldKind { get; set; }
            public int? ColumnSize { get; set; }
        }

        private sealed class ImportResult
        {
            public int Read { get; set; }
            public int Created { get; set; }
            public int Updated { get; set; }
            public int Errors { get; set; }
        }

        private static ImportResult ImportRows(
            SharePointRestConnector connector,
            Guid listId,
            string accessFile,
            string accessTable,
            string pkSourceColumn,
            string titleSourceColumn,
            IReadOnlyList<AccessFieldMapping> mappings,
            int? maxRows,
            CancellationToken cancellationToken)
        {
            var result = new ImportResult();

            var rowIndex = 0;
            AccessTableReader.ReadTableRows(accessFile, accessTable, maxRows, row =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                rowIndex++;
                result.Read++;

                if (rowIndex % 200 == 0)
                {
                    Console.WriteLine($"  imported {rowIndex.ToString(CultureInfo.InvariantCulture)} rows...");
                }

                var appPk = GetAppPkValue(row, pkSourceColumn, rowIndex);
                var title = GetTitleValue(row, titleSourceColumn, appPk);

                var payload = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                {
                    { "Title", title },
                    { DefaultPkInternalName, appPk },
                    { "IsDeleted", false },
                    { "DeletedAtUtc", null }
                };

                if (mappings != null)
                {
                    foreach (var mapping in mappings)
                    {
                        if (mapping == null || string.IsNullOrWhiteSpace(mapping.AccessColumnName) || string.IsNullOrWhiteSpace(mapping.SharePointInternalName))
                        {
                            continue;
                        }

                        object raw;
                        if (!row.TryGetValue(mapping.AccessColumnName, out raw))
                        {
                            continue;
                        }

                        payload[mapping.SharePointInternalName] = ConvertValueForSharePoint(mapping, raw);
                    }
                }

                try
                {
                    connector.CreateListItemAsync(listId, payload, cancellationToken).GetAwaiter().GetResult();
                    result.Created++;
                }
                catch (SharePointRequestException ex)
                {
                    if (!IsAlreadyExistsConflict(ex))
                    {
                        result.Errors++;
                        Console.Error.WriteLine($"Row {rowIndex.ToString(CultureInfo.InvariantCulture)} failed: {ex.Message}");
                        return;
                    }

                    var existingId = ResolveSharePointIdByAppPk(connector, listId, appPk, cancellationToken);
                    if (!existingId.HasValue)
                    {
                        result.Errors++;
                        Console.Error.WriteLine($"Row {rowIndex.ToString(CultureInfo.InvariantCulture)} conflict: could not resolve existing item by AppPK='{appPk}'.");
                        return;
                    }

                    connector.UpdateListItemAsync(listId, existingId.Value, payload, eTag: null, cancellationToken: cancellationToken).GetAwaiter().GetResult();
                    result.Updated++;
                }
            }, cancellationToken);

            return result;
        }

        private static int? ResolveSharePointIdByAppPk(SharePointRestConnector connector, Guid listId, string appPk, CancellationToken cancellationToken)
        {
            var query = new SharePointListQuery
            {
                SelectFields = new[] { "Id", DefaultPkInternalName },
                Filter = $"{DefaultPkInternalName} eq '{EscapeODataString(appPk)}'",
                Top = 1
            };

            var page = connector.QueryListItemsAsync(listId, query, cancellationToken).GetAwaiter().GetResult();
            if (page.Items == null || page.Items.Count == 0)
            {
                return null;
            }

            return page.Items[0].Id;
        }

        private static string GetAppPkValue(IReadOnlyDictionary<string, object> row, string pkSourceColumn, int rowIndex)
        {
            if (row != null && !string.IsNullOrWhiteSpace(pkSourceColumn))
            {
                object pk;
                if (row.TryGetValue(pkSourceColumn, out pk) && pk != null)
                {
                    var s = ConvertToInvariantString(pk);
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        return s;
                    }
                }
            }

            return "Row_" + rowIndex.ToString(CultureInfo.InvariantCulture);
        }

        private static string GetTitleValue(IReadOnlyDictionary<string, object> row, string titleSourceColumn, string fallback)
        {
            if (!string.IsNullOrWhiteSpace(titleSourceColumn) && row != null)
            {
                object title;
                if (row.TryGetValue(titleSourceColumn, out title) && title != null)
                {
                    var s = ConvertToInvariantString(title);
                    if (!string.IsNullOrWhiteSpace(s))
                    {
                        return TruncateText(s, DefaultTextMaxLength);
                    }
                }
            }

            return TruncateText(fallback, DefaultTextMaxLength);
        }

        private static object ConvertValueForSharePoint(AccessFieldMapping mapping, object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is DateTime)
            {
                var dt = (DateTime)value;
                if (dt.Kind == DateTimeKind.Local)
                {
                    dt = dt.ToUniversalTime();
                }
                else if (dt.Kind == DateTimeKind.Unspecified)
                {
                    dt = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                }

                return dt.ToString("o", CultureInfo.InvariantCulture);
            }

            if (value is Guid)
            {
                return value.ToString();
            }

            if (value is byte[])
            {
                return Convert.ToBase64String((byte[])value);
            }

            if (value is bool)
            {
                return value;
            }

            if (IsNumeric(value))
            {
                return value;
            }

            var text = ConvertToInvariantString(value);
            if (mapping != null && mapping.FieldKind == SharePointFieldKind.Text)
            {
                return TruncateText(text, DefaultTextMaxLength);
            }

            return text;
        }

        private static string ConvertToInvariantString(object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is DateTime)
            {
                return ((DateTime)value).ToString("o", CultureInfo.InvariantCulture);
            }

            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        private static bool IsNumeric(object value)
        {
            return value is byte ||
                value is sbyte ||
                value is short ||
                value is ushort ||
                value is int ||
                value is uint ||
                value is long ||
                value is ulong ||
                value is float ||
                value is double ||
                value is decimal;
        }

        private static async Task UpsertAppTablesRowAsync(
            SharePointRestConnector connector,
            Guid tablesListId,
            string entityName,
            Guid entityListId,
            string entityListTitle,
            IReadOnlyList<AccessFieldMapping> mappings,
            CancellationToken cancellationToken)
        {
            var selectFields = new List<string> { "Title" };
            if (mappings != null)
            {
                foreach (var m in mappings)
                {
                    if (m == null || string.IsNullOrWhiteSpace(m.SharePointInternalName))
                    {
                        continue;
                    }

                    selectFields.Add(m.SharePointInternalName);
                }
            }

            selectFields = selectFields
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            var selectJson = Json.Serialize(selectFields);

            var query = new SharePointListQuery
            {
                SelectFields = new[] { "Id", "EntityName" },
                Filter = $"EntityName eq '{EscapeODataString(entityName)}'",
                Top = 1
            };

            var page = await connector.QueryListItemsAsync(tablesListId, query, cancellationToken).ConfigureAwait(false);
            var existingId = page.Items != null && page.Items.Count > 0 ? (int?)page.Items[0].Id : null;

            var payload = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                { "Title", entityName },
                { "EntityName", entityName },
                { "ListId", entityListId.ToString() },
                { "ListTitle", entityListTitle },
                { "Enabled", true },
                { "PkInternalName", DefaultPkInternalName },
                { "SelectFieldsJson", selectJson },
                { "SyncPolicy", (int)SyncPolicy.OnOpen },
                { "Priority", 0 },
                { "AttachmentsMode", (int)AttachmentsMode.None },
                { "PartitionStrategy", (int)PartitionStrategy.None },
                { "ConflictPolicy", (int)ConflictResolutionPolicy.ServerWins },
                { "ExpectedIndexesJson", "[]" }
            };

            if (existingId.HasValue)
            {
                await connector.UpdateListItemAsync(tablesListId, existingId.Value, payload, eTag: null, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await connector.CreateListItemAsync(tablesListId, payload, cancellationToken).ConfigureAwait(false);
            }
        }

        private static async Task BumpConfigVersionAsync(SharePointRestConnector connector, Guid configListId, string appId, CancellationToken cancellationToken)
        {
            var query = new SharePointListQuery
            {
                SelectFields = new[] { "Id", "AppId", "ConfigVersion" },
                Filter = $"AppId eq '{EscapeODataString(appId)}'",
                Top = 1
            };

            var page = await connector.QueryListItemsAsync(configListId, query, cancellationToken).ConfigureAwait(false);
            if (page.Items == null || page.Items.Count == 0)
            {
                await connector.CreateListItemAsync(configListId, new Dictionary<string, object>
                {
                    { "Title", appId },
                    { "AppId", appId },
                    { "ConfigVersion", 1 },
                    { "MinClientVersion", "" },
                    { "LastModifiedUtc", DateTime.UtcNow.ToString("o") }
                }, cancellationToken).ConfigureAwait(false);
                return;
            }

            var item = page.Items[0];
            var current = GetInt(item.Fields, "ConfigVersion");
            var next = current + 1;

            await connector.UpdateListItemAsync(configListId, item.Id, new Dictionary<string, object>
            {
                { "ConfigVersion", next },
                { "LastModifiedUtc", DateTime.UtcNow.ToString("o") }
            }, eTag: null, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        private static int GetInt(IReadOnlyDictionary<string, object> fields, string name)
        {
            if (fields == null || string.IsNullOrWhiteSpace(name))
            {
                return 0;
            }

            object value;
            if (!fields.TryGetValue(name, out value) || value == null)
            {
                return 0;
            }

            if (value is int)
            {
                return (int)value;
            }

            if (value is long)
            {
                return (int)(long)value;
            }

            if (value is double)
            {
                return (int)(double)value;
            }

            var text = Convert.ToString(value, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(text))
            {
                return 0;
            }

            int i;
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out i))
            {
                return i;
            }

            double d;
            if (double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out d))
            {
                return (int)d;
            }

            return 0;
        }

        private static IReadOnlyList<AccessFieldMapping> BuildMappings(IReadOnlyList<AccessTableColumn> schema, string pkSourceColumn, string titleSourceColumn)
        {
            var used = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                DefaultPkInternalName,
                "Title",
                "IsDeleted",
                "DeletedAtUtc"
            };

            var list = new List<AccessFieldMapping>();
            if (schema == null)
            {
                return list;
            }

            foreach (var col in schema)
            {
                if (col == null || string.IsNullOrWhiteSpace(col.Name))
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(pkSourceColumn) && string.Equals(col.Name, pkSourceColumn, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(titleSourceColumn) && string.Equals(col.Name, titleSourceColumn, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (string.Equals(col.Name, "IsDeleted", StringComparison.OrdinalIgnoreCase) || string.Equals(col.Name, "DeletedAtUtc", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var internalName = ToSafeInternalName(col.Name, used);
                var kind = MapToSharePointFieldKind(col);

                list.Add(new AccessFieldMapping
                {
                    AccessColumnName = col.Name,
                    SharePointInternalName = internalName,
                    SharePointDisplayName = col.Name,
                    FieldKind = kind,
                    ColumnSize = col.ColumnSize
                });
            }

            return list;
        }

        private static SharePointFieldKind MapToSharePointFieldKind(AccessTableColumn col)
        {
            var type = col?.DataType;
            if (type == typeof(bool))
            {
                return SharePointFieldKind.Boolean;
            }

            if (type == typeof(DateTime))
            {
                return SharePointFieldKind.DateTime;
            }

            if (type == typeof(Guid))
            {
                return SharePointFieldKind.Guid;
            }

            if (type == typeof(byte[]))
            {
                return SharePointFieldKind.Note;
            }

            if (type == typeof(string))
            {
                if (col.ColumnSize.HasValue && col.ColumnSize.Value > DefaultTextMaxLength)
                {
                    return SharePointFieldKind.Note;
                }

                return SharePointFieldKind.Text;
            }

            if (IsNumericType(type))
            {
                return SharePointFieldKind.Number;
            }

            return SharePointFieldKind.Text;
        }

        private static bool IsNumericType(Type type)
        {
            return type == typeof(byte) ||
                type == typeof(sbyte) ||
                type == typeof(short) ||
                type == typeof(ushort) ||
                type == typeof(int) ||
                type == typeof(uint) ||
                type == typeof(long) ||
                type == typeof(ulong) ||
                type == typeof(float) ||
                type == typeof(double) ||
                type == typeof(decimal);
        }

        private static string ResolvePkSourceColumn(string pkSourceColumn, IReadOnlyList<AccessTableColumn> schema)
        {
            if (!string.IsNullOrWhiteSpace(pkSourceColumn) && ColumnExists(schema, pkSourceColumn))
            {
                return pkSourceColumn;
            }

            if (ColumnExists(schema, DefaultPkInternalName))
            {
                return DefaultPkInternalName;
            }

            var id = FindColumnName(schema, "Id") ?? FindColumnName(schema, "ID");
            if (!string.IsNullOrWhiteSpace(id))
            {
                return id;
            }

            return null;
        }

        private static bool ColumnExists(IReadOnlyList<AccessTableColumn> schema, string name)
        {
            if (schema == null || string.IsNullOrWhiteSpace(name))
            {
                return false;
            }

            foreach (var c in schema)
            {
                if (c != null && string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        private static string FindColumnName(IReadOnlyList<AccessTableColumn> schema, string name)
        {
            if (schema == null || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            foreach (var c in schema)
            {
                if (c != null && string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase))
                {
                    return c.Name;
                }
            }

            return null;
        }

        private static string ToSafeInternalName(string input, HashSet<string> used)
        {
            var raw = (input ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(raw))
            {
                raw = "Field";
            }

            var sb = new StringBuilder();
            foreach (var ch in raw)
            {
                if (char.IsLetterOrDigit(ch))
                {
                    sb.Append(ch);
                }
                else
                {
                    sb.Append('_');
                }
            }

            var baseName = sb.ToString().Trim('_');
            if (string.IsNullOrWhiteSpace(baseName))
            {
                baseName = "Field";
            }

            if (char.IsDigit(baseName[0]))
            {
                baseName = "F_" + baseName;
            }

            if (baseName.Length > SharePointInternalNameMaxLength)
            {
                baseName = baseName.Substring(0, SharePointInternalNameMaxLength);
            }

            var candidate = baseName;
            var i = 1;
            while (used != null && used.Contains(candidate))
            {
                var suffix = "_" + i.ToString(CultureInfo.InvariantCulture);
                var trimmed = baseName;
                if (trimmed.Length + suffix.Length > SharePointInternalNameMaxLength)
                {
                    trimmed = trimmed.Substring(0, Math.Max(1, SharePointInternalNameMaxLength - suffix.Length));
                }

                candidate = trimmed + suffix;
                i++;
            }

            used?.Add(candidate);
            return candidate;
        }

        private static async Task EnsureFieldForMappingAsync(SharePointRestConnector connector, Guid listId, AccessFieldMapping mapping, CancellationToken cancellationToken)
        {
            if (mapping == null)
            {
                return;
            }

            var internalName = mapping.SharePointInternalName;
            var displayName = mapping.SharePointDisplayName;
            if (string.IsNullOrWhiteSpace(internalName) || string.IsNullOrWhiteSpace(displayName))
            {
                return;
            }

            if (mapping.FieldKind == SharePointFieldKind.Note)
            {
                await EnsureNoteFieldAsync(connector, listId, internalName, displayName, required: false, cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }

            if (mapping.FieldKind == SharePointFieldKind.Number)
            {
                await EnsureNumberFieldAsync(connector, listId, internalName, displayName, required: false, cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }

            if (mapping.FieldKind == SharePointFieldKind.Boolean)
            {
                await EnsureBooleanFieldAsync(connector, listId, internalName, displayName, required: false, cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }

            if (mapping.FieldKind == SharePointFieldKind.DateTime)
            {
                await EnsureDateTimeFieldAsync(connector, listId, internalName, displayName, required: false, cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }

            if (mapping.FieldKind == SharePointFieldKind.Guid)
            {
                await EnsureGuidFieldAsync(connector, listId, internalName, displayName, required: false, cancellationToken: cancellationToken).ConfigureAwait(false);
                return;
            }

            var max = mapping.ColumnSize.HasValue && mapping.ColumnSize.Value > 0
                ? Math.Min(mapping.ColumnSize.Value, DefaultTextMaxLength)
                : DefaultTextMaxLength;

            await EnsureTextFieldAsync(connector, listId, internalName, displayName, required: false, maxLength: max, enforceUnique: false, indexed: false, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        private static async Task EnsureTextFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            int maxLength,
            bool enforceUnique,
            bool indexed,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Text' Name='{EscapeXmlAttribute(internalName)}' DisplayName='{EscapeXmlAttribute(displayName)}' Required='{(required ? "TRUE" : "FALSE")}' MaxLength='{maxLength}' />";
            await connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken).ConfigureAwait(false);

            var update = new Dictionary<string, object>
            {
                { "Indexed", indexed },
                { "EnforceUniqueValues", enforceUnique }
            };

            await connector.UpdateFieldAsync(listId, internalName, update, cancellationToken).ConfigureAwait(false);
        }

        private static Task EnsureNoteFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Note' Name='{EscapeXmlAttribute(internalName)}' DisplayName='{EscapeXmlAttribute(displayName)}' Required='{(required ? "TRUE" : "FALSE")}' RichText='FALSE' NumLines='6' />";
            return connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken);
        }

        private static Task EnsureNumberFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Number' Name='{EscapeXmlAttribute(internalName)}' DisplayName='{EscapeXmlAttribute(displayName)}' Required='{(required ? "TRUE" : "FALSE")}' />";
            return connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken);
        }

        private static Task EnsureGuidFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Guid' Name='{EscapeXmlAttribute(internalName)}' DisplayName='{EscapeXmlAttribute(displayName)}' Required='{(required ? "TRUE" : "FALSE")}' />";
            return connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken);
        }

        private static Task EnsureBooleanFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Boolean' Name='{EscapeXmlAttribute(internalName)}' DisplayName='{EscapeXmlAttribute(displayName)}' Required='{(required ? "TRUE" : "FALSE")}' />";
            return connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken);
        }

        private static Task EnsureDateTimeFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='DateTime' Format='DateTime' Name='{EscapeXmlAttribute(internalName)}' DisplayName='{EscapeXmlAttribute(displayName)}' Required='{(required ? "TRUE" : "FALSE")}' />";
            return connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken);
        }

        private static string TruncateText(string text, int maxLength)
        {
            if (string.IsNullOrEmpty(text))
            {
                return text;
            }

            if (maxLength <= 0 || text.Length <= maxLength)
            {
                return text;
            }

            return text.Substring(0, maxLength);
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

        private static string GetArg(string[] args, string name)
        {
            if (args == null || args.Length == 0 || string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            for (var i = 0; i < args.Length; i++)
            {
                if (!string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (i + 1 >= args.Length)
                {
                    return null;
                }

                return args[i + 1];
            }

            return null;
        }

        private static int? GetIntArg(string[] args, string name)
        {
            var text = GetArg(args, name);
            if (string.IsNullOrWhiteSpace(text))
            {
                return null;
            }

            int value;
            if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
            {
                return value;
            }

            return null;
        }

        private static string EscapeODataString(string value)
        {
            return (value ?? string.Empty).Replace("'", "''");
        }

        private static string EscapeXmlAttribute(string value)
        {
            if (value == null)
            {
                return string.Empty;
            }

            return value
                .Replace("&", "&amp;")
                .Replace("\"", "&quot;")
                .Replace("'", "&apos;")
                .Replace("<", "&lt;")
                .Replace(">", "&gt;");
        }
    }
}
