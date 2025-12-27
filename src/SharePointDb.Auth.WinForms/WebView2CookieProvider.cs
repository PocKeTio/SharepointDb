using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;
using SharePointDb.Core;

namespace SharePointDb.Auth.WinForms
{
    public sealed class WebView2CookieProvider : ISharePointCookieProvider
    {
        public Task<CookieContainer> AcquireCookiesAsync(Uri siteUri, CancellationToken cancellationToken = default(CancellationToken))
        {
            if (siteUri == null)
            {
                throw new ArgumentNullException(nameof(siteUri));
            }

            if (!siteUri.IsAbsoluteUri)
            {
                throw new ArgumentException("SiteUri must be absolute.", nameof(siteUri));
            }

            if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled<CookieContainer>(cancellationToken);
            }

            var tcs = new TaskCompletionSource<CookieContainer>();

            var thread = new Thread(() =>
            {
                try
                {
                    Application.EnableVisualStyles();
                    Application.SetCompatibleTextRenderingDefault(false);

                    using (var form = new WebView2LoginForm(siteUri))
                    {
                        if (cancellationToken.CanBeCanceled)
                        {
                            cancellationToken.Register(() =>
                            {
                                try
                                {
                                    form.BeginInvoke(new Action(() =>
                                    {
                                        if (!form.IsDisposed)
                                        {
                                            form.DialogResult = DialogResult.Cancel;
                                            form.Close();
                                        }
                                    }));
                                }
                                catch
                                {
                                }
                            });
                        }

                        var result = form.ShowDialog();

                        if (result == DialogResult.OK)
                        {
                            if (form.Cookies == null)
                            {
                                tcs.TrySetException(new InvalidOperationException("No cookies were returned."));
                            }
                            else
                            {
                                tcs.TrySetResult(form.Cookies);
                            }
                        }
                        else
                        {
                            tcs.TrySetCanceled();
                        }
                    }
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
            });

            thread.IsBackground = true;
            thread.SetApartmentState(ApartmentState.STA);
            thread.Start();

            return tcs.Task;
        }
    }
}
