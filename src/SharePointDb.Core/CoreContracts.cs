using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace SharePointDb.Core
{
    public enum SyncPolicy
    {
        OnOpen = 0,
        OnDemand = 1,
        Never = 2
    }

    public enum AttachmentsMode
    {
        None = 0,
        OnDemand = 1,
        Prefetch = 2
    }

    public enum PartitionStrategy
    {
        None = 0,
        ByDateRange = 1,
        ByIdRange = 2
    }

    public enum ConflictResolutionPolicy
    {
        ServerWins = 0,
        ClientWins = 1,
        Manual = 2
    }

    public enum ChangeOperation
    {
        Insert = 0,
        Update = 1,
        SoftDelete = 2
    }

    public enum ChangeStatus
    {
        Pending = 0,
        Applied = 1,
        Conflict = 2
    }

    public sealed class AppConfig
    {
        public string AppId { get; set; }
        public int ConfigVersion { get; set; }
        public string MinClientVersion { get; set; }
        public DateTime? LastModifiedUtc { get; set; }
    }

    public sealed class AppTableConfig
    {
        public string EntityName { get; set; }
        public Guid ListId { get; set; }
        public string ListTitle { get; set; }
        public bool Enabled { get; set; }
        public string PkInternalName { get; set; }
        public IReadOnlyList<string> SelectFields { get; set; }
        public SyncPolicy SyncPolicy { get; set; }
        public int Priority { get; set; }
        public AttachmentsMode AttachmentsMode { get; set; }
        public PartitionStrategy PartitionStrategy { get; set; }
        public ConflictResolutionPolicy ConflictPolicy { get; set; }
        public IReadOnlyList<string> ExpectedIndexes { get; set; }
    }

    public sealed class LocalConfig
    {
        public string AppId { get; set; }
        public int ConfigVersion { get; set; }
        public IReadOnlyList<AppTableConfig> Tables { get; set; }
        public DateTime UpdatedUtc { get; set; }
    }

    public sealed class SyncState
    {
        public string EntityName { get; set; }
        public DateTime? LastSyncModifiedUtc { get; set; }
        public int? LastSyncSpId { get; set; }
        public DateTime? LastSuccessfulSyncUtc { get; set; }
        public int? LastConfigVersionApplied { get; set; }
        public string LastError { get; set; }
    }

    public sealed class ChangeLogEntry
    {
        public long Id { get; set; }
        public string EntityName { get; set; }
        public string AppPK { get; set; }
        public ChangeOperation Operation { get; set; }
        public string PayloadJson { get; set; }
        public DateTime CreatedUtc { get; set; }
        public ChangeStatus Status { get; set; }
        public int AttemptCount { get; set; }
        public DateTime? AppliedUtc { get; set; }
        public string LastError { get; set; }
    }

    public sealed class ConflictLogEntry
    {
        public long Id { get; set; }
        public DateTime OccurredUtc { get; set; }

        public string EntityName { get; set; }
        public string AppPK { get; set; }
        public long? ChangeId { get; set; }
        public ChangeOperation Operation { get; set; }
        public ConflictResolutionPolicy Policy { get; set; }

        public int? SharePointId { get; set; }
        public string LocalETag { get; set; }
        public string ServerETag { get; set; }

        public string LocalPayloadJson { get; set; }
        public string ServerFieldsJson { get; set; }
        public string Message { get; set; }
    }

    public static class Json
    {
        public static string Serialize<T>(T value)
        {
            return JsonConvert.SerializeObject(value);
        }

        public static T Deserialize<T>(string json)
        {
            if (string.IsNullOrWhiteSpace(json))
            {
                return default(T);
            }

            return JsonConvert.DeserializeObject<T>(json);
        }
    }

    public interface ILocalStore : IDisposable
    {
        Task InitializeAsync(CancellationToken cancellationToken = default(CancellationToken));

        Task<LocalConfig> GetLocalConfigAsync(string appId, CancellationToken cancellationToken = default(CancellationToken));
        Task UpsertLocalConfigAsync(LocalConfig config, CancellationToken cancellationToken = default(CancellationToken));

        Task<SyncState> GetSyncStateAsync(string entityName, CancellationToken cancellationToken = default(CancellationToken));
        Task UpsertSyncStateAsync(SyncState state, CancellationToken cancellationToken = default(CancellationToken));

        Task EnqueueChangeAsync(ChangeLogEntry entry, CancellationToken cancellationToken = default(CancellationToken));
        Task<IReadOnlyList<ChangeLogEntry>> GetPendingChangesAsync(int maxCount, CancellationToken cancellationToken = default(CancellationToken));
        Task MarkChangeAppliedAsync(long changeId, DateTime appliedUtc, CancellationToken cancellationToken = default(CancellationToken));
        Task MarkChangeFailedAsync(long changeId, string error, CancellationToken cancellationToken = default(CancellationToken));
        Task MarkChangeConflictedAsync(long changeId, string error, CancellationToken cancellationToken = default(CancellationToken));

        Task LogConflictAsync(ConflictLogEntry entry, CancellationToken cancellationToken = default(CancellationToken));
        Task<IReadOnlyList<ConflictLogEntry>> GetRecentConflictsAsync(int maxCount, CancellationToken cancellationToken = default(CancellationToken));
    }

    public sealed class LocalEntitySystemFields
    {
        public int? SharePointId { get; set; }
        public DateTime? SharePointModifiedUtc { get; set; }
        public string SharePointETag { get; set; }

        public bool IsDeleted { get; set; }
        public DateTime? DeletedAtUtc { get; set; }
    }

    public sealed class LocalEntityRow
    {
        public string AppPK { get; set; }
        public IReadOnlyDictionary<string, object> Fields { get; set; }
        public LocalEntitySystemFields System { get; set; }
    }

    public interface ILocalEntityStore
    {
        Task EnsureEntitySchemaAsync(AppTableConfig tableConfig, CancellationToken cancellationToken = default(CancellationToken));

        Task UpsertEntityAsync(
            string entityName,
            string appPk,
            IReadOnlyDictionary<string, object> fields,
            LocalEntitySystemFields system,
            CancellationToken cancellationToken = default(CancellationToken));

        Task<LocalEntityRow> GetEntityAsync(
            string entityName,
            string appPk,
            CancellationToken cancellationToken = default(CancellationToken));
    }
}
