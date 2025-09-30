using System;
using System.Net;
using System.Net.Sockets;

namespace Snowflake.Data.Core.Authenticator.Browser
{
    internal class WebListenerStarter
    {
        public static WebListenerStarter Instance = new WebListenerStarter();

        public virtual HttpListener StartHttpListener(string url)
        {
            return StartHttpListener(new[] { url });
        }

        public virtual HttpListener StartHttpListener(string[] urls)
        {
            var listener = new HttpListener();
            try
            {
                foreach (var url in urls)
                {
                    listener.Prefixes.Add(url);
                }
                listener.Start();
            }
            catch (Exception)
            {
                ((IDisposable)listener).Dispose();
                throw;
            }
            return listener;
        }

        public virtual int GetRandomUnusedPort()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
