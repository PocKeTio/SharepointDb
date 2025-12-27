using System;

namespace SharePointDb.Sample
{
    public enum LocalDbKind
    {
        Sqlite = 0,
        Access = 1
    }

    public sealed class SharePointDbClientOptions
    {
        public SharePointDbClientOptions(Uri siteUri, string appId, string sqliteFilePath)
            : this(siteUri, appId, LocalDbKind.Sqlite, sqliteFilePath)
        {
        }

        public SharePointDbClientOptions(Uri siteUri, string appId, LocalDbKind localDbKind, string localDbFilePath)
        {
            if (siteUri == null)
            {
                throw new ArgumentNullException(nameof(siteUri));
            }

            if (!siteUri.IsAbsoluteUri)
            {
                throw new ArgumentException("SiteUri must be absolute.", nameof(siteUri));
            }

            if (string.IsNullOrWhiteSpace(appId))
            {
                throw new ArgumentException("AppId is required.", nameof(appId));
            }

            if (string.IsNullOrWhiteSpace(localDbFilePath))
            {
                throw new ArgumentException("Local DB file path is required.", nameof(localDbFilePath));
            }

            SiteUri = siteUri;
            AppId = appId;
            LocalDbKind = localDbKind;
            LocalDbFilePath = localDbFilePath;
        }

        public Uri SiteUri { get; }

        public string AppId { get; }

        public LocalDbKind LocalDbKind { get; }

        public string LocalDbFilePath { get; }
    }
}
