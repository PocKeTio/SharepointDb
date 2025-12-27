using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;

namespace SharePointDb.Access
{
    public sealed class AccessTableColumn
    {
        public string Name { get; set; }
        public Type DataType { get; set; }
        public int? ColumnSize { get; set; }
        public bool AllowDbNull { get; set; }
    }

    public static class AccessTableReader
    {
        public static IReadOnlyList<AccessTableColumn> GetTableSchema(string filePath, string tableName)
        {
            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("Access file path is required.", nameof(filePath));
            }

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("Access file not found.", filePath);
            }

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("TableName is required.", nameof(tableName));
            }

            using (var connection = new OleDbConnection(BuildConnectionString(filePath)))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = $"SELECT * FROM {QuoteTableName(tableName)} WHERE 1=0";

                    using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                    {
                        var schema = reader?.GetSchemaTable();
                        if (schema == null)
                        {
                            return Array.Empty<AccessTableColumn>();
                        }

                        var list = new List<AccessTableColumn>();
                        foreach (DataRow row in schema.Rows)
                        {
                            var name = Convert.ToString(row["ColumnName"], CultureInfo.InvariantCulture);
                            if (string.IsNullOrWhiteSpace(name))
                            {
                                continue;
                            }

                            var dataType = row["DataType"] as Type;

                            int size;
                            int? sizeOrNull = null;
                            if (row.Table.Columns.Contains("ColumnSize") && row["ColumnSize"] != DBNull.Value && int.TryParse(Convert.ToString(row["ColumnSize"], CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out size))
                            {
                                sizeOrNull = size;
                            }

                            bool allowNull = true;
                            if (row.Table.Columns.Contains("AllowDBNull") && row["AllowDBNull"] != DBNull.Value)
                            {
                                allowNull = Convert.ToBoolean(row["AllowDBNull"], CultureInfo.InvariantCulture);
                            }

                            list.Add(new AccessTableColumn
                            {
                                Name = name,
                                DataType = dataType,
                                ColumnSize = sizeOrNull,
                                AllowDbNull = allowNull
                            });
                        }

                        return list;
                    }
                }
            }
        }

        public static void ReadTableRows(
            string filePath,
            string tableName,
            int? maxRows,
            Action<IReadOnlyDictionary<string, object>> onRow,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(filePath))
            {
                throw new ArgumentException("Access file path is required.", nameof(filePath));
            }

            if (!File.Exists(filePath))
            {
                throw new FileNotFoundException("Access file not found.", filePath);
            }

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentException("TableName is required.", nameof(tableName));
            }

            if (onRow == null)
            {
                throw new ArgumentNullException(nameof(onRow));
            }

            using (var connection = new OleDbConnection(BuildConnectionString(filePath)))
            {
                connection.Open();

                using (var cmd = connection.CreateCommand())
                {
                    var top = maxRows.HasValue && maxRows.Value > 0 ? maxRows.Value : 0;
                    cmd.CommandText = top > 0
                        ? $"SELECT TOP {top.ToString(CultureInfo.InvariantCulture)} * FROM {QuoteTableName(tableName)}"
                        : $"SELECT * FROM {QuoteTableName(tableName)}";

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader == null)
                        {
                            return;
                        }

                        var fieldCount = reader.FieldCount;
                        var names = new string[fieldCount];
                        for (var i = 0; i < fieldCount; i++)
                        {
                            names[i] = reader.GetName(i);
                        }

                        while (reader.Read())
                        {
                            cancellationToken.ThrowIfCancellationRequested();

                            var dict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                            for (var i = 0; i < fieldCount; i++)
                            {
                                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                dict[names[i]] = value;
                            }

                            onRow(dict);
                        }
                    }
                }
            }
        }

        private static string BuildConnectionString(string filePath)
        {
            var ext = Path.GetExtension(filePath) ?? string.Empty;
            if (ext.Equals(".mdb", StringComparison.OrdinalIgnoreCase) || ext.Equals(".accdb", StringComparison.OrdinalIgnoreCase))
            {
                return $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={filePath};Persist Security Info=False;";
            }

            return $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={filePath};Persist Security Info=False;";
        }

        private static string QuoteTableName(string tableName)
        {
            var trimmed = (tableName ?? string.Empty).Trim();
            if (trimmed.StartsWith("[", StringComparison.Ordinal) && trimmed.EndsWith("]", StringComparison.Ordinal))
            {
                return trimmed;
            }

            return "[" + trimmed.Replace("]", "]]") + "]";
        }
    }
}
