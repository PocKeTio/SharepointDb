using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace SharePointDb.Core
{
    public sealed class SharePointListQuery
    {
        public IReadOnlyList<string> SelectFields { get; set; }
        public string Filter { get; set; }
        public string OrderBy { get; set; }
        public int? Top { get; set; }
        public string NextPageUrl { get; set; }
    }

    public sealed class SharePointListItem
    {
        public int Id { get; set; }
        public string ETag { get; set; }
        public DateTime? ModifiedUtc { get; set; }
        public IReadOnlyDictionary<string, object> Fields { get; set; }
    }

    public sealed class SharePointListItemPage
    {
        public IReadOnlyList<SharePointListItem> Items { get; set; }
        public string NextPageUrl { get; set; }
    }

    public sealed class SharePointAttachmentInfo
    {
        public string FileName { get; set; }
        public string ServerRelativeUrl { get; set; }
    }

    public sealed class SharePointRequestException : Exception
    {
        public int StatusCode { get; }
        public string ReasonPhrase { get; }
        public string ResponseContent { get; }

        public SharePointRequestException(string message, int statusCode, string reasonPhrase, string responseContent, Exception innerException = null)
            : base(message, innerException)
        {
            StatusCode = statusCode;
            ReasonPhrase = reasonPhrase;
            ResponseContent = responseContent;
        }
    }

    public interface ISharePointCookieProvider
    {
        Task<CookieContainer> AcquireCookiesAsync(Uri siteUri, CancellationToken cancellationToken = default(CancellationToken));
    }

    public sealed class StaticCookieProvider : ISharePointCookieProvider
    {
        private readonly CookieContainer _cookies;

        public StaticCookieProvider(CookieContainer cookies)
        {
            _cookies = cookies ?? throw new ArgumentNullException(nameof(cookies));
        }

        public Task<CookieContainer> AcquireCookiesAsync(Uri siteUri, CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(_cookies);
        }
    }

    public interface ISharePointConnector : IDisposable
    {
        Uri SiteUri { get; }

        Task<Guid> GetListIdByTitleAsync(string listTitle, CancellationToken cancellationToken = default(CancellationToken));

        Task<SharePointListItemPage> QueryListItemsAsync(Guid listId, SharePointListQuery query, CancellationToken cancellationToken = default(CancellationToken));
        Task<SharePointListItem> GetListItemAsync(Guid listId, int itemId, IReadOnlyList<string> selectFields, CancellationToken cancellationToken = default(CancellationToken));

        Task<int> CreateListItemAsync(Guid listId, IDictionary<string, object> fieldValues, CancellationToken cancellationToken = default(CancellationToken));
        Task UpdateListItemAsync(Guid listId, int itemId, IDictionary<string, object> fieldValues, string eTag, CancellationToken cancellationToken = default(CancellationToken));

        Task<IReadOnlyList<SharePointAttachmentInfo>> ListAttachmentsAsync(Guid listId, int itemId, CancellationToken cancellationToken = default(CancellationToken));
        Task DownloadAttachmentAsync(Guid listId, int itemId, string fileName, Stream destination, CancellationToken cancellationToken = default(CancellationToken));
        Task UploadAttachmentAsync(Guid listId, int itemId, string fileName, Stream content, CancellationToken cancellationToken = default(CancellationToken));
        Task DeleteAttachmentAsync(Guid listId, int itemId, string fileName, CancellationToken cancellationToken = default(CancellationToken));
    }
}
