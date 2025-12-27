using System;

namespace SharePointDb.SharePoint
{
    public sealed class SharePointRestConnectorOptions
    {
        public SharePointRestConnectorOptions(Uri siteUri)
        {
            if (siteUri == null)
            {
                throw new ArgumentNullException(nameof(siteUri));
            }

            if (!siteUri.IsAbsoluteUri)
            {
                throw new ArgumentException("SiteUri must be absolute.", nameof(siteUri));
            }

            var siteUriText = siteUri.AbsoluteUri;
            if (!siteUriText.EndsWith("/", StringComparison.Ordinal))
            {
                siteUriText += "/";
            }

            SiteUri = new Uri(siteUriText);
        }

        public Uri SiteUri { get; }

        public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(100);

        public string UserAgent { get; set; } = "SharePointDb/1.0";

        public int DefaultPageSize { get; set; } = 200;
    }
}
