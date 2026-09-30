using System;
using System.Net;
using System.Text;
using System.Threading;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Tools;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    /// <summary>
    /// Listens on an <see cref="HttpListener"/> for browser callback requests, dispatches each
    /// one through a handler, and signals the waiting thread when a terminal response is received.
    /// </summary>
    internal class WebBrowserListener<T> : IDisposable
        where T : class
    {
        private readonly HttpListener _httpListener;
        /// <summary>
        /// Called for every incoming request. Returns <c>null</c> to continue listening for the
        /// next request, or a <see cref="Result{T,IBrowserError}"/> to deliver a terminal outcome
        /// and stop the listener.
        /// </summary>
        private readonly Func<HttpListenerContext, Result<T, IBrowserError>> _handler;
        private readonly string _successResponse;
        private readonly string _unexpectedErrorResponse;
        private readonly ManualResetEvent _successEvent;
        private readonly object _wakeEventLock = new object();
        private T _result;
        private string _browserError;
        private Exception _exception;
        private volatile bool _isDisposed;
        private volatile bool _isShuttingDown;

        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<WebBrowserListener<T>>();

        /// <summary>
        /// Initialises the listener with the already-started <paramref name="httpListener"/>,
        /// a handler that processes each request and either returns <c>null</c> to keep listening
        /// or a <see cref="Result{T,IBrowserError}"/> to stop, and HTML bodies for the terminal
        /// success and error responses.
        /// </summary>
        public WebBrowserListener(
            HttpListener httpListener,
            Func<HttpListenerContext, Result<T, IBrowserError>> handler,
            string successResponse,
            string unexpectedErrorResponse)
        {
            _httpListener = httpListener;
            _handler = handler;
            _successResponse = successResponse;
            _unexpectedErrorResponse = unexpectedErrorResponse;
            _successEvent = new ManualResetEvent(false);
            _result = null;
            _browserError = null;
            _exception = null;
            _isDisposed = false;
            _isShuttingDown = false;
        }

        /// <summary>
        /// Blocks until the browser delivers a valid token, the <paramref name="timeout"/> elapses,
        /// or an unrecoverable error occurs. Stops the listener before returning.
        /// </summary>
        /// <exception cref="SnowflakeDbException">Thrown on timeout or on a browser error reported by the handler.</exception>
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
                _isShuttingDown = true;
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
            HttpListener httpListener = (HttpListener)result.AsyncState;
            HttpListenerContext context;
            try
            {
                // The pending operation is always completed, also while shutting down, so that
                // the listener does not leak it when the wait is re-armed and then stopped.
                context = httpListener.EndGetContext(result);
            }
            catch (Exception exception) when (IsListenerClosedException(exception))
            {
                if (IsShutdownExpected(httpListener))
                {
                    s_logger.Debug("Stopped waiting for the browser response because the listener was closed");
                    return;
                }
                s_logger.Error("Error while trying to get context from HttpListener", exception);
                _exception = exception;
                WakeUpAwaitingThread();
                return;
            }

            if (IsShutdownExpected(httpListener))
            {
                s_logger.Debug("Ignoring the browser response received after the listener was closed");
                return;
            }

            Result<T, IBrowserError> extracted;
            try
            {
                extracted = _handler(context);
            }
            catch (Exception exception)
            {
                _exception = exception;
                _browserError = _unexpectedErrorResponse;
                RespondToBrowserWithError(context);
                WakeUpAwaitingThread();
                return;
            }

            if (extracted == null)
            {
                ContinueListening(httpListener);
                return;
            }

            bool success = extracted.IsSuccess();
            if (success)
            {
                _result = extracted.Success;
            }
            else
            {
                _browserError = extracted.Error.GetBrowserError();
                _exception = extracted.Error.GetException();
            }
            if (success)
                RespondToBrowser(context);
            else
                RespondToBrowserWithError(context);
            WakeUpAwaitingThread();
        }

        private void ContinueListening(HttpListener httpListener)
        {
            try
            {
                if (IsShutdownExpected(httpListener))
                {
                    s_logger.Debug("Stopped waiting for the browser response because the listener was closed");
                    return;
                }
                httpListener.BeginGetContext(GetContextCallback, httpListener);
            }
            catch (Exception exception) when (IsListenerClosedException(exception))
            {
                if (IsShutdownExpected(httpListener))
                {
                    s_logger.Debug("Stopped waiting for the browser response because the listener was closed");
                    return;
                }
                s_logger.Error("Error while waiting for another browser response", exception);
                _exception = exception;
                WakeUpAwaitingThread();
            }
        }

        private static bool IsListenerClosedException(Exception exception) =>
            exception is HttpListenerException ||
            exception is ObjectDisposedException ||
            exception is InvalidOperationException;

        private bool IsShutdownExpected(HttpListener httpListener)
        {
            if (_isDisposed || _isShuttingDown)
                return true;
            try
            {
                return !httpListener.IsListening;
            }
            catch (ObjectDisposedException)
            {
                return true;
            }
        }

        private void WakeUpAwaitingThread()
        {
            lock (_wakeEventLock)
            {
                if (_isDisposed)
                    return;
                _successEvent.Set();
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
                response.ContentLength64 = responseBytes.Length;
                response.KeepAlive = false;
                response.OutputStream.Write(responseBytes, 0, responseBytes.Length);
                response.Close();
            }
            catch
            {
                // Ignore the exception as it does not affect the overall authentication flow
                s_logger.Warn("Browser response not sent out");
            }
        }

        public void Dispose()
        {
            lock (_wakeEventLock)
            {
                if (_isDisposed)
                    return;
                _isShuttingDown = true;
                _isDisposed = true;
            }
            ((IDisposable)_httpListener)?.Dispose();
            _successEvent?.Dispose();
        }
    }
}
