using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using System.Windows.Forms;
using Microsoft.Web.WebView2.Core;
using Microsoft.Web.WebView2.WinForms;

namespace SharePointDb.Auth.WinForms
{
    public sealed class WebView2LoginForm : Form
    {
        private readonly Uri _siteUri;
        private readonly WebView2 _webView;
        private readonly Button _continueButton;
        private readonly Button _cancelButton;

        public CookieContainer Cookies { get; private set; }

        public WebView2LoginForm(Uri siteUri)
        {
            if (siteUri == null)
            {
                throw new ArgumentNullException(nameof(siteUri));
            }

            _siteUri = siteUri;

            Text = "SharePoint Login";
            Width = 1100;
            Height = 800;
            StartPosition = FormStartPosition.CenterScreen;

            _webView = new WebView2
            {
                Dock = DockStyle.Fill
            };

            _continueButton = new Button
            {
                Text = "Continue",
                Dock = DockStyle.Right,
                Width = 120
            };
            _continueButton.Click += ContinueButton_Click;

            _cancelButton = new Button
            {
                Text = "Cancel",
                Dock = DockStyle.Right,
                Width = 120
            };
            _cancelButton.Click += (s, e) =>
            {
                DialogResult = DialogResult.Cancel;
                Close();
            };

            var bottomPanel = new Panel
            {
                Dock = DockStyle.Bottom,
                Height = 44
            };

            bottomPanel.Controls.Add(_continueButton);
            bottomPanel.Controls.Add(_cancelButton);

            Controls.Add(_webView);
            Controls.Add(bottomPanel);
        }

        protected override async void OnShown(EventArgs e)
        {
            base.OnShown(e);

            try
            {
                await _webView.EnsureCoreWebView2Async();
                _webView.CoreWebView2.Navigate(_siteUri.ToString());
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, ex.Message, "WebView2", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private async void ContinueButton_Click(object sender, EventArgs e)
        {
            _continueButton.Enabled = false;

            try
            {
                Cookies = await ReadCookiesAsync(_siteUri);
                DialogResult = DialogResult.OK;
                Close();
            }
            catch (Exception ex)
            {
                _continueButton.Enabled = true;
                MessageBox.Show(this, ex.Message, "SharePoint Login", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        }

        private async Task<CookieContainer> ReadCookiesAsync(Uri siteUri)
        {
            await _webView.EnsureCoreWebView2Async();

            var cookies = await _webView.CoreWebView2.CookieManager.GetCookiesAsync(siteUri.ToString());
            return ConvertToCookieContainer(siteUri, cookies);
        }

        private static CookieContainer ConvertToCookieContainer(Uri siteUri, IReadOnlyList<CoreWebView2Cookie> cookies)
        {
            var container = new CookieContainer();

            if (cookies == null)
            {
                return container;
            }

            foreach (var c in cookies)
            {
                try
                {
                    var domain = string.IsNullOrWhiteSpace(c.Domain) ? siteUri.Host : c.Domain.TrimStart('.');
                    var cookie = new Cookie(c.Name, c.Value, c.Path, domain)
                    {
                        Secure = c.IsSecure,
                        HttpOnly = c.IsHttpOnly
                    };

                    var uri = new Uri(siteUri.Scheme + "://" + domain);
                    container.Add(uri, cookie);
                }
                catch
                {
                }
            }

            return container;
        }
    }
}
