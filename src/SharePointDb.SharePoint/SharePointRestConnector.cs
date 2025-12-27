using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using SharePointDb.Core;

namespace SharePointDb.SharePoint
{
    public sealed class SharePointRestConnector : ISharePointConnector
    {
        private const string JsonVerboseMime = "application/json;odata=verbose";

        private readonly SharePointRestConnectorOptions _options;
        private readonly ISharePointCookieProvider _cookieProvider;
        private readonly HttpClientHandler _httpHandler;
        private readonly HttpClient _httpClient;

        private CookieContainer _cookieContainer;
        private string _formDigestValue;
        private DateTime _formDigestExpiresUtc;

        private readonly object _entityTypeLock = new object();
        private readonly Dictionary<Guid, string> _listItemEntityTypeFullNameCache = new Dictionary<Guid, string>();

        public SharePointRestConnector(SharePointRestConnectorOptions options, ISharePointCookieProvider cookieProvider)
        {
            _options = options ?? throw new ArgumentNullException(nameof(options));
            _cookieProvider = cookieProvider ?? throw new ArgumentNullException(nameof(cookieProvider));

            _httpHandler = new HttpClientHandler
            {
                UseCookies = true,
                CookieContainer = new CookieContainer(),
                AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate
            };

            _httpClient = new HttpClient(_httpHandler)
            {
                Timeout = _options.Timeout
            };

            if (!string.IsNullOrWhiteSpace(_options.UserAgent))
            {
                _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("User-Agent", _options.UserAgent);
            }
        }

        public Uri SiteUri => _options.SiteUri;

        public async Task<Guid> GetListIdByTitleAsync(string listTitle, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(listTitle))
            {
                throw new ArgumentException("ListTitle is required.", nameof(listTitle));
            }

            var escaped = EscapeODataString(listTitle);
            var url = BuildApiUri($"_api/web/lists/getbytitle('{escaped}')?$select=Id");

            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                return req;
            }, requiresDigest: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var idText = root.SelectToken("d.Id")?.Value<string>();
            Guid id;
            if (!Guid.TryParse(idText, out id))
            {
                throw new InvalidOperationException("List Id was not returned or is not a GUID.");
            }

            return id;
        }

        public async Task<Guid> EnsureListAsync(string listTitle, string description = null, int baseTemplate = 100, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(listTitle))
            {
                throw new ArgumentException("ListTitle is required.", nameof(listTitle));
            }

            try
            {
                return await GetListIdByTitleAsync(listTitle, cancellationToken).ConfigureAwait(false);
            }
            catch (SharePointRequestException ex)
            {
                if (ex.StatusCode != 404)
                {
                    throw;
                }
            }

            return await CreateListAsync(listTitle, description, baseTemplate, cancellationToken).ConfigureAwait(false);
        }

        public async Task<Guid> CreateListAsync(string listTitle, string description = null, int baseTemplate = 100, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(listTitle))
            {
                throw new ArgumentException("ListTitle is required.", nameof(listTitle));
            }

            var url = BuildApiUri("_api/web/lists");

            var payload = new JObject
            {
                ["__metadata"] = new JObject { ["type"] = "SP.List" },
                ["BaseTemplate"] = baseTemplate,
                ["Title"] = listTitle
            };

            if (!string.IsNullOrWhiteSpace(description))
            {
                payload["Description"] = description;
            }

            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                req.Headers.TryAddWithoutValidation("X-RequestDigest", digest);
                req.Content = new StringContent(payload.ToString(), Encoding.UTF8, JsonVerboseMime);
                return req;
            }, requiresDigest: true, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var idText = root.SelectToken("d.Id")?.Value<string>();
            Guid id;
            if (!Guid.TryParse(idText, out id))
            {
                throw new InvalidOperationException("Create list response did not contain a valid Id.");
            }

            return id;
        }

        public async Task<bool> FieldExistsAsync(Guid listId, string internalNameOrTitle, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(internalNameOrTitle))
            {
                throw new ArgumentException("InternalNameOrTitle is required.", nameof(internalNameOrTitle));
            }

            var escaped = EscapeODataString(internalNameOrTitle);
            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/fields/getbyinternalnameortitle('{escaped}')?$select=Id");

            try
            {
                await SendJsonForStringAsync(digest =>
                {
                    var req = new HttpRequestMessage(HttpMethod.Get, url);
                    req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                    return req;
                }, requiresDigest: false, cancellationToken: cancellationToken).ConfigureAwait(false);
                return true;
            }
            catch (SharePointRequestException ex)
            {
                if (ex.StatusCode == 404)
                {
                    return false;
                }

                throw;
            }
        }

        public async Task CreateFieldAsXmlAsync(Guid listId, string schemaXml, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(schemaXml))
            {
                throw new ArgumentException("SchemaXml is required.", nameof(schemaXml));
            }

            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/fields/createfieldasxml");

            var payload = new JObject
            {
                ["parameters"] = new JObject
                {
                    ["__metadata"] = new JObject { ["type"] = "SP.XmlSchemaFieldCreationInformation" },
                    ["SchemaXml"] = schemaXml
                }
            };

            await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                req.Headers.TryAddWithoutValidation("X-RequestDigest", digest);
                req.Content = new StringContent(payload.ToString(), Encoding.UTF8, JsonVerboseMime);
                return req;
            }, requiresDigest: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async Task EnsureFieldAsXmlAsync(Guid listId, string internalNameOrTitle, string schemaXml, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(internalNameOrTitle))
            {
                throw new ArgumentException("InternalNameOrTitle is required.", nameof(internalNameOrTitle));
            }

            if (await FieldExistsAsync(listId, internalNameOrTitle, cancellationToken).ConfigureAwait(false))
            {
                return;
            }

            await CreateFieldAsXmlAsync(listId, schemaXml, cancellationToken).ConfigureAwait(false);
        }

        public async Task UpdateFieldAsync(Guid listId, string internalNameOrTitle, IDictionary<string, object> fieldValues, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (string.IsNullOrWhiteSpace(internalNameOrTitle))
            {
                throw new ArgumentException("InternalNameOrTitle is required.", nameof(internalNameOrTitle));
            }

            if (fieldValues == null)
            {
                throw new ArgumentNullException(nameof(fieldValues));
            }

            var escaped = EscapeODataString(internalNameOrTitle);
            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/fields/getbyinternalnameortitle('{escaped}')");

            var payload = new JObject
            {
                ["__metadata"] = new JObject { ["type"] = "SP.Field" }
            };

            foreach (var kvp in fieldValues)
            {
                payload[kvp.Key] = kvp.Value == null ? JValue.CreateNull() : JToken.FromObject(kvp.Value);
            }

            await SendJsonNoContentAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                req.Headers.TryAddWithoutValidation("X-RequestDigest", digest);
                req.Headers.TryAddWithoutValidation("X-HTTP-Method", "MERGE");
                req.Headers.TryAddWithoutValidation("IF-MATCH", "*");
                req.Content = new StringContent(payload.ToString(), Encoding.UTF8, JsonVerboseMime);
                return req;
            }, requiresDigest: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public Task<SharePointListItemPage> QueryListItemsAsync(Guid listId, SharePointListQuery query, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (query == null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            return QueryListItemsInternalAsync(listId, query, cancellationToken);
        }

        public async Task<SharePointListItem> GetListItemAsync(Guid listId, int itemId, IReadOnlyList<string> selectFields, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (itemId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(itemId));
            }

            var url = BuildListItemUri(listId, itemId, selectFields);
            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                return req;
            }, requiresDigest: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var itemObj = (JObject)root["d"];

            return ParseListItem(itemObj);
        }

        public async Task<int> CreateListItemAsync(Guid listId, IDictionary<string, object> fieldValues, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (fieldValues == null)
            {
                throw new ArgumentNullException(nameof(fieldValues));
            }

            var entityType = await GetListItemEntityTypeFullNameAsync(listId, cancellationToken).ConfigureAwait(false);
            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/items");

            var payload = new JObject
            {
                ["__metadata"] = new JObject { ["type"] = entityType }
            };

            foreach (var kvp in fieldValues)
            {
                payload[kvp.Key] = kvp.Value == null ? JValue.CreateNull() : JToken.FromObject(kvp.Value);
            }

            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                req.Headers.TryAddWithoutValidation("X-RequestDigest", digest);
                req.Content = new StringContent(payload.ToString(), Encoding.UTF8, JsonVerboseMime);
                return req;
            }, requiresDigest: true, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var idToken = root.SelectToken("d.Id") ?? root.SelectToken("d.ID");
            if (idToken == null)
            {
                throw new InvalidOperationException("Create response did not contain an Id.");
            }

            return idToken.Value<int>();
        }

        public async Task UpdateListItemAsync(Guid listId, int itemId, IDictionary<string, object> fieldValues, string eTag, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (itemId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(itemId));
            }

            if (fieldValues == null)
            {
                throw new ArgumentNullException(nameof(fieldValues));
            }

            var entityType = await GetListItemEntityTypeFullNameAsync(listId, cancellationToken).ConfigureAwait(false);
            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/items({itemId})");

            var payload = new JObject
            {
                ["__metadata"] = new JObject { ["type"] = entityType }
            };

            foreach (var kvp in fieldValues)
            {
                payload[kvp.Key] = kvp.Value == null ? JValue.CreateNull() : JToken.FromObject(kvp.Value);
            }

            await SendJsonNoContentAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                req.Headers.TryAddWithoutValidation("X-RequestDigest", digest);
                req.Headers.TryAddWithoutValidation("X-HTTP-Method", "MERGE");
                req.Headers.TryAddWithoutValidation("IF-MATCH", string.IsNullOrWhiteSpace(eTag) ? "*" : eTag);
                req.Content = new StringContent(payload.ToString(), Encoding.UTF8, JsonVerboseMime);
                return req;
            }, requiresDigest: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async Task<IReadOnlyList<SharePointAttachmentInfo>> ListAttachmentsAsync(Guid listId, int itemId, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (itemId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(itemId));
            }

            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/items({itemId})/AttachmentFiles");
            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                return req;
            }, requiresDigest: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var results = root.SelectToken("d.results") as JArray;

            var list = new List<SharePointAttachmentInfo>();
            if (results != null)
            {
                foreach (var token in results.OfType<JObject>())
                {
                    list.Add(new SharePointAttachmentInfo
                    {
                        FileName = token.Value<string>("FileName"),
                        ServerRelativeUrl = token.Value<string>("ServerRelativeUrl")
                    });
                }
            }

            return list;
        }

        public async Task DownloadAttachmentAsync(Guid listId, int itemId, string fileName, Stream destination, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (itemId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(itemId));
            }

            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException("FileName is required.", nameof(fileName));
            }

            if (destination == null)
            {
                throw new ArgumentNullException(nameof(destination));
            }

            var escapedFileName = EscapeODataString(fileName);
            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/items({itemId})/AttachmentFiles('{escapedFileName}')/$value");

            await DownloadToStreamAsync(createRequest: () =>
            {
                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.TryAddWithoutValidation("Accept", "*/*");
                return req;
            }, destination: destination, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public async Task UploadAttachmentAsync(Guid listId, int itemId, string fileName, Stream content, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (itemId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(itemId));
            }

            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException("FileName is required.", nameof(fileName));
            }

            if (content == null)
            {
                throw new ArgumentNullException(nameof(content));
            }

            var originalPosition = content.CanSeek ? (long?)content.Position : null;
            var escapedFileName = EscapeODataString(fileName);
            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/items({itemId})/AttachmentFiles/add(FileName='{escapedFileName}')");

            for (var attempt = 0; attempt < 2; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (attempt > 0)
                {
                    if (originalPosition.HasValue)
                    {
                        content.Position = originalPosition.Value;
                    }
                    else
                    {
                        throw new InvalidOperationException("Cannot retry upload because the content stream is not seekable.");
                    }
                }

                await EnsureCookiesAsync(cancellationToken).ConfigureAwait(false);
                var digest = await EnsureFormDigestAsync(cancellationToken).ConfigureAwait(false);

                using (var req = new HttpRequestMessage(HttpMethod.Post, url))
                {
                    req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                    req.Headers.TryAddWithoutValidation("X-RequestDigest", digest);
                    req.Content = new StreamContent(content);
                    req.Content.Headers.TryAddWithoutValidation("Content-Type", "application/octet-stream");

                    using (var resp = await _httpClient.SendAsync(req, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false))
                    {
                        if (attempt == 0 && (resp.StatusCode == HttpStatusCode.Unauthorized || resp.StatusCode == HttpStatusCode.Forbidden))
                        {
                            InvalidateAuthentication();
                            continue;
                        }

                        var respText = resp.Content == null ? null : await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
                        if (!resp.IsSuccessStatusCode)
                        {
                            throw CreateRequestException(resp, respText);
                        }

                        return;
                    }
                }
            }
        }

        public async Task DeleteAttachmentAsync(Guid listId, int itemId, string fileName, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (itemId <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(itemId));
            }

            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException("FileName is required.", nameof(fileName));
            }

            var escapedFileName = EscapeODataString(fileName);
            var url = BuildApiUri($"_api/web/lists(guid'{listId}')/items({itemId})/AttachmentFiles('{escapedFileName}')");

            await SendJsonNoContentAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                req.Headers.TryAddWithoutValidation("X-RequestDigest", digest);
                req.Headers.TryAddWithoutValidation("X-HTTP-Method", "DELETE");
                req.Headers.TryAddWithoutValidation("IF-MATCH", "*");
                return req;
            }, requiresDigest: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _httpClient.Dispose();
            _httpHandler.Dispose();
        }

        private async Task<SharePointListItemPage> QueryListItemsInternalAsync(Guid listId, SharePointListQuery query, CancellationToken cancellationToken)
        {
            var url = BuildListQueryUri(listId, query);

            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                return req;
            }, requiresDigest: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var d = (JObject)root["d"];
            var itemsArray = d?["results"] as JArray;
            var next = d?.Value<string>("__next");

            var items = new List<SharePointListItem>();
            if (itemsArray != null)
            {
                foreach (var itemObj in itemsArray.OfType<JObject>())
                {
                    items.Add(ParseListItem(itemObj));
                }
            }

            return new SharePointListItemPage
            {
                Items = items,
                NextPageUrl = next
            };
        }

        private Uri BuildListQueryUri(Guid listId, SharePointListQuery query)
        {
            if (!string.IsNullOrWhiteSpace(query.NextPageUrl))
            {
                if (Uri.IsWellFormedUriString(query.NextPageUrl, UriKind.Absolute))
                {
                    return new Uri(query.NextPageUrl, UriKind.Absolute);
                }

                return new Uri(SiteUri, query.NextPageUrl);
            }

            var selectFields = new List<string>();
            if (query.SelectFields != null)
            {
                selectFields.AddRange(query.SelectFields.Where(f => !string.IsNullOrWhiteSpace(f)));
            }

            if (!selectFields.Any(f => string.Equals(f, "Id", StringComparison.OrdinalIgnoreCase) || string.Equals(f, "ID", StringComparison.OrdinalIgnoreCase)))
            {
                selectFields.Insert(0, "Id");
            }

            if (!selectFields.Any(f => string.Equals(f, "Modified", StringComparison.OrdinalIgnoreCase)))
            {
                selectFields.Add("Modified");
            }

            var sb = new StringBuilder();
            sb.Append($"_api/web/lists(guid'{listId}')/items?");
            sb.Append("$select=").Append(string.Join(",", selectFields));

            if (!string.IsNullOrWhiteSpace(query.Filter))
            {
                sb.Append("&$filter=").Append(Uri.EscapeDataString(query.Filter));
            }

            if (!string.IsNullOrWhiteSpace(query.OrderBy))
            {
                sb.Append("&$orderby=").Append(Uri.EscapeDataString(query.OrderBy));
            }

            var top = query.Top.HasValue && query.Top.Value > 0 ? query.Top.Value : _options.DefaultPageSize;
            sb.Append("&$top=").Append(top.ToString(CultureInfo.InvariantCulture));

            return BuildApiUri(sb.ToString());
        }

        private Uri BuildListItemUri(Guid listId, int itemId, IReadOnlyList<string> selectFields)
        {
            var fields = new List<string>();
            if (selectFields != null)
            {
                fields.AddRange(selectFields.Where(f => !string.IsNullOrWhiteSpace(f)));
            }

            if (!fields.Any(f => string.Equals(f, "Id", StringComparison.OrdinalIgnoreCase) || string.Equals(f, "ID", StringComparison.OrdinalIgnoreCase)))
            {
                fields.Insert(0, "Id");
            }

            if (!fields.Any(f => string.Equals(f, "Modified", StringComparison.OrdinalIgnoreCase)))
            {
                fields.Add("Modified");
            }

            var sb = new StringBuilder();
            sb.Append($"_api/web/lists(guid'{listId}')/items({itemId})?");
            sb.Append("$select=").Append(string.Join(",", fields));

            return BuildApiUri(sb.ToString());
        }

        private Uri BuildApiUri(string relative)
        {
            if (string.IsNullOrWhiteSpace(relative))
            {
                return SiteUri;
            }

            if (relative.StartsWith("/", StringComparison.Ordinal))
            {
                relative = relative.Substring(1);
            }

            return new Uri(SiteUri, relative);
        }

        private async Task EnsureCookiesAsync(CancellationToken cancellationToken)
        {
            if (_cookieContainer != null)
            {
                return;
            }

            var cookies = await _cookieProvider.AcquireCookiesAsync(SiteUri, cancellationToken).ConfigureAwait(false);
            if (cookies == null)
            {
                throw new InvalidOperationException("Cookie provider returned null.");
            }

            _cookieContainer = cookies;
            _httpHandler.CookieContainer = cookies;
        }

        private void InvalidateAuthentication()
        {
            _cookieContainer = null;
            _formDigestValue = null;
            _formDigestExpiresUtc = default(DateTime);
        }

        private async Task<string> EnsureFormDigestAsync(CancellationToken cancellationToken)
        {
            if (!string.IsNullOrWhiteSpace(_formDigestValue) && _formDigestExpiresUtc > DateTime.UtcNow.AddSeconds(10))
            {
                return _formDigestValue;
            }

            var url = BuildApiUri("_api/contextinfo");
            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                req.Content = new StringContent(string.Empty, Encoding.UTF8, JsonVerboseMime);
                return req;
            }, requiresDigest: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var digestValue = root.SelectToken("d.GetContextWebInformation.FormDigestValue")?.Value<string>();
            var timeoutSeconds = root.SelectToken("d.GetContextWebInformation.FormDigestTimeoutSeconds")?.Value<int?>() ?? 0;

            if (string.IsNullOrWhiteSpace(digestValue) || timeoutSeconds <= 0)
            {
                throw new InvalidOperationException("Could not retrieve form digest.");
            }

            _formDigestValue = digestValue;
            _formDigestExpiresUtc = DateTime.UtcNow.AddSeconds(timeoutSeconds - 30);

            return _formDigestValue;
        }

        private async Task<string> GetListItemEntityTypeFullNameAsync(Guid listId, CancellationToken cancellationToken)
        {
            lock (_entityTypeLock)
            {
                string cached;
                if (_listItemEntityTypeFullNameCache.TryGetValue(listId, out cached))
                {
                    return cached;
                }
            }

            var url = BuildApiUri($"_api/web/lists(guid'{listId}')?$select=ListItemEntityTypeFullName");
            var json = await SendJsonForStringAsync(digest =>
            {
                var req = new HttpRequestMessage(HttpMethod.Get, url);
                req.Headers.TryAddWithoutValidation("Accept", JsonVerboseMime);
                return req;
            }, requiresDigest: false, cancellationToken: cancellationToken).ConfigureAwait(false);

            var root = JObject.Parse(json);
            var typeName = root.SelectToken("d.ListItemEntityTypeFullName")?.Value<string>();
            if (string.IsNullOrWhiteSpace(typeName))
            {
                throw new InvalidOperationException("ListItemEntityTypeFullName was not returned.");
            }

            lock (_entityTypeLock)
            {
                _listItemEntityTypeFullNameCache[listId] = typeName;
            }

            return typeName;
        }

        private async Task<string> SendJsonForStringAsync(Func<string, HttpRequestMessage> createRequest, bool requiresDigest, CancellationToken cancellationToken)
        {
            if (createRequest == null)
            {
                throw new ArgumentNullException(nameof(createRequest));
            }

            for (var attempt = 0; attempt < 2; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await EnsureCookiesAsync(cancellationToken).ConfigureAwait(false);
                var digest = requiresDigest ? await EnsureFormDigestAsync(cancellationToken).ConfigureAwait(false) : null;

                using (var req = createRequest(digest))
                using (var resp = await _httpClient.SendAsync(req, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false))
                {
                    if (attempt == 0 && (resp.StatusCode == HttpStatusCode.Unauthorized || resp.StatusCode == HttpStatusCode.Forbidden))
                    {
                        InvalidateAuthentication();
                        continue;
                    }

                    var content = resp.Content == null ? null : await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (!resp.IsSuccessStatusCode)
                    {
                        throw CreateRequestException(resp, content);
                    }

                    return content ?? string.Empty;
                }
            }

            throw new InvalidOperationException("Authentication failed.");
        }

        private async Task SendJsonNoContentAsync(Func<string, HttpRequestMessage> createRequest, bool requiresDigest, CancellationToken cancellationToken)
        {
            if (createRequest == null)
            {
                throw new ArgumentNullException(nameof(createRequest));
            }

            for (var attempt = 0; attempt < 2; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await EnsureCookiesAsync(cancellationToken).ConfigureAwait(false);
                var digest = requiresDigest ? await EnsureFormDigestAsync(cancellationToken).ConfigureAwait(false) : null;

                using (var req = createRequest(digest))
                using (var resp = await _httpClient.SendAsync(req, HttpCompletionOption.ResponseContentRead, cancellationToken).ConfigureAwait(false))
                {
                    if (attempt == 0 && (resp.StatusCode == HttpStatusCode.Unauthorized || resp.StatusCode == HttpStatusCode.Forbidden))
                    {
                        InvalidateAuthentication();
                        continue;
                    }

                    var content = resp.Content == null ? null : await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
                    if (!resp.IsSuccessStatusCode)
                    {
                        throw CreateRequestException(resp, content);
                    }

                    return;
                }
            }

            throw new InvalidOperationException("Authentication failed.");
        }

        private async Task DownloadToStreamAsync(Func<HttpRequestMessage> createRequest, Stream destination, CancellationToken cancellationToken)
        {
            if (createRequest == null)
            {
                throw new ArgumentNullException(nameof(createRequest));
            }

            if (destination == null)
            {
                throw new ArgumentNullException(nameof(destination));
            }

            for (var attempt = 0; attempt < 2; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                await EnsureCookiesAsync(cancellationToken).ConfigureAwait(false);

                using (var req = createRequest())
                using (var resp = await _httpClient.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false))
                {
                    if (attempt == 0 && (resp.StatusCode == HttpStatusCode.Unauthorized || resp.StatusCode == HttpStatusCode.Forbidden))
                    {
                        InvalidateAuthentication();
                        continue;
                    }

                    if (!resp.IsSuccessStatusCode)
                    {
                        var content = resp.Content == null ? null : await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw CreateRequestException(resp, content);
                    }

                    using (var stream = await resp.Content.ReadAsStreamAsync().ConfigureAwait(false))
                    {
                        await stream.CopyToAsync(destination, 81920, cancellationToken).ConfigureAwait(false);
                        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }
            }

            throw new InvalidOperationException("Authentication failed.");
        }

        private static SharePointRequestException CreateRequestException(HttpResponseMessage response, string responseText)
        {
            var statusCode = (int)response.StatusCode;
            var reason = response.ReasonPhrase;
            var message = $"SharePoint request failed: HTTP {statusCode} {reason}.";
            return new SharePointRequestException(message, statusCode, reason, responseText);
        }

        private static SharePointListItem ParseListItem(JObject itemObj)
        {
            if (itemObj == null)
            {
                return null;
            }

            var idToken = itemObj["Id"] ?? itemObj["ID"];
            var id = idToken == null ? 0 : idToken.Value<int>();

            var metadata = itemObj["__metadata"] as JObject;
            var etag = metadata?.Value<string>("etag");

            var modifiedUtc = ParseSharePointDateUtc(itemObj["Modified"]);

            var fields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            foreach (var prop in itemObj.Properties())
            {
                if (string.Equals(prop.Name, "__metadata", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                fields[prop.Name] = ConvertToken(prop.Value);
            }

            return new SharePointListItem
            {
                Id = id,
                ETag = etag,
                ModifiedUtc = modifiedUtc,
                Fields = fields
            };
        }

        private static DateTime? ParseSharePointDateUtc(JToken token)
        {
            if (token == null || token.Type == JTokenType.Null)
            {
                return null;
            }

            if (token.Type == JTokenType.Date)
            {
                return token.Value<DateTime>().ToUniversalTime();
            }

            var text = token.ToString();
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

        private static object ConvertToken(JToken token)
        {
            if (token == null || token.Type == JTokenType.Null)
            {
                return null;
            }

            var value = token as JValue;
            if (value != null)
            {
                return value.Value;
            }

            return token.ToString();
        }

        private static string EscapeODataString(string value)
        {
            return value?.Replace("'", "''");
        }
    }
}
