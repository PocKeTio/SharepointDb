using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SharePointDb.Auth.WinForms;
using SharePointDb.Core;

namespace SharePointDb.Sample
{
    internal static class Program
    {
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
                PrintUsage();
                return 2;
            }

            var appId = GetArg(args, "--appId") ?? GetArg(args, "-a") ?? "APP";
            var localDbKindText = (GetArg(args, "--dbkind") ?? GetArg(args, "--kind") ?? "sqlite").Trim();
            var localDbFilePath = GetArg(args, "--dbfile")
                ?? GetArg(args, "--sqlite")
                ?? GetArg(args, "--access")
                ?? GetArg(args, "-db");

            LocalDbKind localDbKind;
            if (string.Equals(localDbKindText, "access", StringComparison.OrdinalIgnoreCase) || string.Equals(localDbKindText, "accdb", StringComparison.OrdinalIgnoreCase) || string.Equals(localDbKindText, "mdb", StringComparison.OrdinalIgnoreCase))
            {
                localDbKind = LocalDbKind.Access;
            }
            else
            {
                localDbKind = LocalDbKind.Sqlite;
            }

            if (string.IsNullOrWhiteSpace(localDbFilePath))
            {
                localDbFilePath = localDbKind == LocalDbKind.Access
                    ? "SharePointDb.Sample.accdb"
                    : "SharePointDb.Sample.sqlite";
            }

            var cmd = (GetArg(args, "--cmd") ?? GetArg(args, "-c") ?? "sync-on-open").Trim();

            var entity = GetArg(args, "--entity") ?? GetArg(args, "-e");
            var pk = GetArg(args, "--pk") ?? GetArg(args, "-k");
            var title = GetArg(args, "--title");
            var value = GetArg(args, "--value");
            var maxText = GetArg(args, "--max");

            int max;
            if (!int.TryParse(maxText ?? string.Empty, NumberStyles.Integer, CultureInfo.InvariantCulture, out max))
            {
                max = 10;
            }

            var siteUri = new Uri(siteUrl, UriKind.Absolute);

            var cookieProvider = new WebView2CookieProvider();
            var options = new SharePointDbClientOptions(siteUri, appId, localDbKind, localDbFilePath);

            using (var client = new SharePointDbClient(options, cookieProvider))
            {
                await client.InitializeAsync(cancellationToken).ConfigureAwait(false);

                if (string.Equals(cmd, "config", StringComparison.OrdinalIgnoreCase))
                {
                    var config = await client.EnsureConfigAsync(cancellationToken).ConfigureAwait(false);
                    Console.WriteLine(Json.Serialize(config));
                    return 0;
                }

                if (string.Equals(cmd, "sync-on-open", StringComparison.OrdinalIgnoreCase))
                {
                    await client.SyncOnOpenAsync(cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("OK");
                    return 0;
                }

                if (string.Equals(cmd, "sync-all", StringComparison.OrdinalIgnoreCase))
                {
                    await client.SyncAllAsync(cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("OK");
                    return 0;
                }

                if (string.Equals(cmd, "sync-table", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(entity))
                    {
                        Console.Error.WriteLine("Missing --entity.");
                        return 2;
                    }

                    await client.SyncTableAsync(entity, cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("OK");
                    return 0;
                }

                if (string.Equals(cmd, "get", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(entity) || string.IsNullOrWhiteSpace(pk))
                    {
                        Console.Error.WriteLine("Missing --entity and/or --pk.");
                        return 2;
                    }

                    var row = await client.GetLocalAsync(entity, pk, cancellationToken).ConfigureAwait(false);
                    if (row == null)
                    {
                        Console.WriteLine("NOT_FOUND");
                        return 0;
                    }

                    Console.WriteLine(Json.Serialize(row));
                    return 0;
                }

                if (string.Equals(cmd, "enqueue-insert", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(entity) || string.IsNullOrWhiteSpace(pk))
                    {
                        Console.Error.WriteLine("Missing --entity and/or --pk.");
                        return 2;
                    }

                    var fields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
                    {
                        { "Title", string.IsNullOrWhiteSpace(title) ? pk : title }
                    };

                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        fields["Value"] = value;
                    }

                    await client.UpsertLocalAndEnqueueInsertAsync(entity, pk, fields, cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("ENQUEUED");
                    return 0;
                }

                if (string.Equals(cmd, "enqueue-update", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(entity) || string.IsNullOrWhiteSpace(pk))
                    {
                        Console.Error.WriteLine("Missing --entity and/or --pk.");
                        return 2;
                    }

                    var fields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

                    if (!string.IsNullOrWhiteSpace(title))
                    {
                        fields["Title"] = title;
                    }

                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        fields["Value"] = value;
                    }

                    if (fields.Count == 0)
                    {
                        Console.Error.WriteLine("Nothing to update. Provide --title and/or --value.");
                        return 2;
                    }

                    await client.UpsertLocalAndEnqueueUpdateAsync(entity, pk, fields, cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("ENQUEUED");
                    return 0;
                }

                if (string.Equals(cmd, "enqueue-delete", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrWhiteSpace(entity) || string.IsNullOrWhiteSpace(pk))
                    {
                        Console.Error.WriteLine("Missing --entity and/or --pk.");
                        return 2;
                    }

                    await client.MarkLocalDeletedAndEnqueueSoftDeleteAsync(entity, pk, cancellationToken).ConfigureAwait(false);
                    Console.WriteLine("ENQUEUED");
                    return 0;
                }

                if (string.Equals(cmd, "recent-conflicts", StringComparison.OrdinalIgnoreCase))
                {
                    var list = await client.GetRecentConflictsAsync(max, cancellationToken).ConfigureAwait(false);
                    Console.WriteLine(Json.Serialize(list ?? Array.Empty<ConflictLogEntry>()));
                    return 0;
                }
            }

            Console.Error.WriteLine("Unknown --cmd: " + cmd);
            PrintUsage();
            return 2;
        }

        private static void PrintUsage()
        {
            Console.Error.WriteLine("Usage:");
            Console.Error.WriteLine("  SharePointDb.Sample --site <https://sharepoint/site/> [--appId <APP>] [--dbkind <sqlite|access>] [--dbfile <file>] --cmd <cmd> [args]");
            Console.Error.WriteLine("  Backward compatible: [--sqlite <file>] and [-db <file>] still work as aliases for --dbfile");
            Console.Error.WriteLine("Cmd:");
            Console.Error.WriteLine("  config");
            Console.Error.WriteLine("  sync-on-open");
            Console.Error.WriteLine("  sync-all");
            Console.Error.WriteLine("  sync-table --entity <EntityName>");
            Console.Error.WriteLine("  get --entity <EntityName> --pk <AppPK>");
            Console.Error.WriteLine("  enqueue-insert --entity <EntityName> --pk <AppPK> [--title <text>] [--value <text>]");
            Console.Error.WriteLine("  enqueue-update --entity <EntityName> --pk <AppPK> [--title <text>] [--value <text>]");
            Console.Error.WriteLine("  enqueue-delete --entity <EntityName> --pk <AppPK>");
            Console.Error.WriteLine("  recent-conflicts [--max <N>]");
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
    }
}
