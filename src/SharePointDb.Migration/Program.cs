using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Auth.WinForms;
using SharePointDb.SharePoint;

namespace SharePointDb.Migration
{
    internal static class Program
    {
        private const int DefaultTextMaxLength = 255;

        public static int Main(string[] args)
        {
            try
            {
                return RunAsync(args, CancellationToken.None).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex);
                return 1;
            }
        }

        private static async Task<int> RunAsync(string[] args, CancellationToken cancellationToken)
        {
            var siteUrl = GetArg(args, "--site") ?? GetArg(args, "-s");
            if (string.IsNullOrWhiteSpace(siteUrl))
            {
                Console.Error.WriteLine("Usage: SharePointDb.Migration --site <https://sharepoint/site/> [--appId <APP>] [--cmd provision|access-import]");
                Console.Error.WriteLine("  access-import args: --access <file.accdb> --table <TableName> [--entity <EntityName>] [--pk <ColumnName>] [--max <N>]");
                return 2;
            }

            var appId = GetArg(args, "--appId") ?? GetArg(args, "-a") ?? "APP";
            var cmd = GetArg(args, "--cmd") ?? GetArg(args, "-c") ?? "provision";

            var siteUri = new Uri(siteUrl, UriKind.Absolute);

            var cookieProvider = new WebView2CookieProvider();
            var connector = new SharePointRestConnector(new SharePointRestConnectorOptions(siteUri), cookieProvider);

            var systemLists = await EnsureSystemListsAsync(connector, appId, cancellationToken).ConfigureAwait(false);

            if (string.Equals(cmd, "access-import", StringComparison.OrdinalIgnoreCase))
            {
                await AccessImportCommand.RunAsync(connector, systemLists.ConfigListId, systemLists.TablesListId, appId, args, cancellationToken).ConfigureAwait(false);
            }
            else if (!string.Equals(cmd, "provision", StringComparison.OrdinalIgnoreCase))
            {
                Console.Error.WriteLine($"Unknown --cmd '{cmd}'.");
                return 2;
            }

            Console.WriteLine("Done.");
            return 0;
        }

        private sealed class SystemLists
        {
            public Guid ConfigListId { get; set; }
            public Guid TablesListId { get; set; }
        }

        private static async Task<SystemLists> EnsureSystemListsAsync(SharePointRestConnector connector, string appId, CancellationToken cancellationToken)
        {
            Console.WriteLine("Ensuring lists...");

            var configListId = await connector.EnsureListAsync("APP_Config", "SharePointDb configuration", baseTemplate: 100, cancellationToken: cancellationToken).ConfigureAwait(false);
            var tablesListId = await connector.EnsureListAsync("APP_Tables", "SharePointDb tables", baseTemplate: 100, cancellationToken: cancellationToken).ConfigureAwait(false);

            Console.WriteLine("Ensuring fields for APP_Config...");
            await EnsureTextFieldAsync(connector, configListId, internalName: "AppId", displayName: "AppId", required: true, maxLength: DefaultTextMaxLength, enforceUnique: true, indexed: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNumberFieldAsync(connector, configListId, internalName: "ConfigVersion", displayName: "ConfigVersion", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureTextFieldAsync(connector, configListId, internalName: "MinClientVersion", displayName: "MinClientVersion", required: false, maxLength: DefaultTextMaxLength, enforceUnique: false, indexed: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureDateTimeFieldAsync(connector, configListId, internalName: "LastModifiedUtc", displayName: "LastModifiedUtc", required: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            Console.WriteLine("Ensuring fields for APP_Tables...");
            await EnsureTextFieldAsync(connector, tablesListId, internalName: "EntityName", displayName: "EntityName", required: true, maxLength: DefaultTextMaxLength, enforceUnique: true, indexed: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureGuidFieldAsync(connector, tablesListId, internalName: "ListId", displayName: "ListId", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureTextFieldAsync(connector, tablesListId, internalName: "ListTitle", displayName: "ListTitle", required: false, maxLength: DefaultTextMaxLength, enforceUnique: false, indexed: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureBooleanFieldAsync(connector, tablesListId, internalName: "Enabled", displayName: "Enabled", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureTextFieldAsync(connector, tablesListId, internalName: "PkInternalName", displayName: "PkInternalName", required: false, maxLength: DefaultTextMaxLength, enforceUnique: false, indexed: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNoteFieldAsync(connector, tablesListId, internalName: "SelectFieldsJson", displayName: "SelectFieldsJson", required: false, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNumberFieldAsync(connector, tablesListId, internalName: "SyncPolicy", displayName: "SyncPolicy", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNumberFieldAsync(connector, tablesListId, internalName: "Priority", displayName: "Priority", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNumberFieldAsync(connector, tablesListId, internalName: "AttachmentsMode", displayName: "AttachmentsMode", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNumberFieldAsync(connector, tablesListId, internalName: "PartitionStrategy", displayName: "PartitionStrategy", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNumberFieldAsync(connector, tablesListId, internalName: "ConflictPolicy", displayName: "ConflictPolicy", required: true, cancellationToken: cancellationToken).ConfigureAwait(false);
            await EnsureNoteFieldAsync(connector, tablesListId, internalName: "ExpectedIndexesJson", displayName: "ExpectedIndexesJson", required: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            Console.WriteLine("Seeding initial APP_Config entry...");
            await EnsureAppConfigRowAsync(connector, configListId, appId, cancellationToken).ConfigureAwait(false);

            return new SystemLists
            {
                ConfigListId = configListId,
                TablesListId = tablesListId
            };
        }

        private static async Task EnsureAppConfigRowAsync(SharePointRestConnector connector, Guid listId, string appId, CancellationToken cancellationToken)
        {
            var query = new SharePointDb.Core.SharePointListQuery
            {
                SelectFields = new[] { "Id", "AppId" },
                Filter = $"AppId eq '{EscapeODataString(appId)}'",
                Top = 1
            };

            var page = await connector.QueryListItemsAsync(listId, query, cancellationToken).ConfigureAwait(false);
            if (page.Items != null && page.Items.Count > 0)
            {
                return;
            }

            await connector.CreateListItemAsync(listId, new Dictionary<string, object>
            {
                { "Title", appId },
                { "AppId", appId },
                { "ConfigVersion", 1 },
                { "MinClientVersion", "" },
                { "LastModifiedUtc", DateTime.UtcNow.ToString("o") }
            }, cancellationToken).ConfigureAwait(false);
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
            var schemaXml = $"<Field Type='Text' Name='{internalName}' DisplayName='{displayName}' Required='{(required ? "TRUE" : "FALSE")}' MaxLength='{maxLength}' />";
            await connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken).ConfigureAwait(false);

            var update = new Dictionary<string, object>
            {
                { "Indexed", indexed },
                { "EnforceUniqueValues", enforceUnique }
            };

            await connector.UpdateFieldAsync(listId, internalName, update, cancellationToken).ConfigureAwait(false);
        }

        private static async Task EnsureNoteFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Note' Name='{internalName}' DisplayName='{displayName}' Required='{(required ? "TRUE" : "FALSE")}' RichText='FALSE' NumLines='6' />";
            await connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken).ConfigureAwait(false);
        }

        private static async Task EnsureNumberFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Number' Name='{internalName}' DisplayName='{displayName}' Required='{(required ? "TRUE" : "FALSE")}' />";
            await connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken).ConfigureAwait(false);
        }

        private static async Task EnsureGuidFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Guid' Name='{internalName}' DisplayName='{displayName}' Required='{(required ? "TRUE" : "FALSE")}' />";
            await connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken).ConfigureAwait(false);
        }

        private static async Task EnsureBooleanFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='Boolean' Name='{internalName}' DisplayName='{displayName}' Required='{(required ? "TRUE" : "FALSE")}' />";
            await connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken).ConfigureAwait(false);
        }

        private static async Task EnsureDateTimeFieldAsync(
            SharePointRestConnector connector,
            Guid listId,
            string internalName,
            string displayName,
            bool required,
            CancellationToken cancellationToken)
        {
            var schemaXml = $"<Field Type='DateTime' Format='DateTime' Name='{internalName}' DisplayName='{displayName}' Required='{(required ? "TRUE" : "FALSE")}' />";
            await connector.EnsureFieldAsXmlAsync(listId, internalName, schemaXml, cancellationToken).ConfigureAwait(false);
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

        private static string EscapeODataString(string value)
        {
            return (value ?? string.Empty).Replace("'", "''");
        }
    }
}
