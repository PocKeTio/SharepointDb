using System;

namespace SharePointDb.Sample
{
    public sealed class SharePointDbClientOptions
    {
        public SharePointDbClientOptions(Uri siteUri, string appId, string sqliteFilePath)
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

            if (string.IsNullOrWhiteSpace(sqliteFilePath))
            {
                throw new ArgumentException("SQLite file path is required.", nameof(sqliteFilePath));
            }

            SiteUri = siteUri;
            AppId = appId;
            SqliteFilePath = sqliteFilePath;
        }

        public Uri SiteUri { get; }

        public string AppId { get; }

        public string SqliteFilePath { get; }
    }
}
