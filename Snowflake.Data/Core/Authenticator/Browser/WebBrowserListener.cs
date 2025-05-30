using System;
using System.Net;
using System.Text;
using System.Threading;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class WebBrowserListener<T> : IDisposable
        where T : class
    {
        private readonly HttpListener _httpListener;
        private readonly Func<HttpListenerRequest, Result<T, IBrowserError>> _extractor;
        private readonly string _successResponse;
        private readonly string _unexpectedErrorResponse;
        private readonly ManualResetEvent _successEvent;
        private T _result;
        private string _browserError;
        private Exception _exception;
        private bool _isDisposed;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WebBrowserListener<T>>();

        public WebBrowserListener(HttpListener httpListener, Func<HttpListenerRequest, Result<T, IBrowserError>> extractor, string successResponse, string unexpectedErrorResponse)
        {
            _httpListener = httpListener;
            _extractor = extractor;
            _successResponse = successResponse;
            _unexpectedErrorResponse = unexpectedErrorResponse;
            _successEvent = new ManualResetEvent(false);
            _result = null;
            _browserError = null;
            _exception = null;
            _isDisposed = false;
        }

        public T WaitAndGetResult(TimeSpan timeout)
        {
            try
            {
                _httpListener.BeginGetContext(GetContextCallback, _httpListener);
                if (!_successEvent.WaitOne(timeout))
                {
                    s_logger.Warn("Browser response timeout");
                    throw new SnowflakeDbException(SFError.BROWSER_RESPONSE_TIMEOUT, timeout.TotalSeconds);
                }
            }
            finally
            {
                _httpListener.Stop();
            }

            if (_exception != null)
            {
                throw _exception;
            }

            return _result;
        }

        private void GetContextCallback(IAsyncResult result)
        {
            if (_isDisposed)
                return;
            HttpListener httpListener = (HttpListener)result.AsyncState;
            if (httpListener.IsListening)
            {
                HttpListenerContext context = null;
                try
                {
                    context = httpListener.EndGetContext(result);
                }
                catch (HttpListenerException ex)
                {
                    s_logger.Error("Error while trying to get context from HttpListener", ex);
                    WakeUpAwaitingThread();
                    return;
                }

                HttpListenerRequest request = context.Request;
                bool success;
                try
                {
                    var extracted = _extractor(request);
                    success = extracted.IsSuccess();
                    if (success)
                    {
                        _result = extracted.Success;
                    }
                    else
                    {
                        _browserError = extracted.Error.GetBrowserError();
                        _exception = extracted.Error.GetException();
                    }
                }
                catch (Exception exception)
                {
                    _exception = exception;
                    _browserError = _unexpectedErrorResponse;
                    success = false;
                }
                if (success)
                    RespondToBrowser(context);
                else
                {
                    RespondToBrowserWithError(context);
                }
            }
            WakeUpAwaitingThread();
        }

        private void WakeUpAwaitingThread()
        {
            try
            {
                _successEvent.Set();
            }
            catch (ObjectDisposedException)
            {
                s_logger.Warn("Could not wake up the thread waiting for the browser response because the resource was already disposed");
            }
            catch (Exception exception)
            {
                s_logger.Warn("Could not wake up the thread waiting for the browser response because of error", exception);
            }
        }

        private void RespondToBrowser(HttpListenerContext context)
        {
            byte[] okResponseBytes = Encoding.UTF8.GetBytes(_successResponse);
            HttpListenerResponse response = context.Response;
            WriteMessageToBrowser(response, okResponseBytes);
        }

        private void RespondToBrowserWithError(HttpListenerContext context)
        {
            byte[] errorResponseBytes = Encoding.UTF8.GetBytes(_browserError);
            HttpListenerResponse response = context.Response;
            response.StatusCode = (int)HttpStatusCode.BadRequest;
            WriteMessageToBrowser(response, errorResponseBytes);
        }

        private void WriteMessageToBrowser(HttpListenerResponse response, byte[] responseBytes)
        {
            try
            {
                using (var output = response.OutputStream)
                {
                    output.Write(responseBytes, 0, responseBytes.Length);
                }
            }
            catch
            {
                // Ignore the exception as it does not affect the overall authentication flow
                s_logger.Warn("Browser response not sent out");
            }
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                _isDisposed = true;
                ((IDisposable)_httpListener)?.Dispose();
                _successEvent?.Dispose();
            }
        }
    }
}
