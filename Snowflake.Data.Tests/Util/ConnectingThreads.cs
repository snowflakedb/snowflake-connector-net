using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Snowflake.Data.Client;
using Snowflake.Data.Core.Session;
using Snowflake.Data.Tests.Util;

namespace Snowflake.Data.Tests.IntegrationTests
{
    class ConnectingThreads
    {
        private string _connectionString; 

        private ConcurrentQueue<ThreadEvent> _events = new ConcurrentQueue<ThreadEvent>();

        private List<Thread> threads = new List<Thread>();

        public ConnectingThreads(string connectionString)
        {
            _connectionString = connectionString;
        }

        public ConnectingThreads NewThread(string name,
            long waitBeforeConnectMillis,
            long waitAfterConnectMillis,
            bool closeOnExit)
        {
            var thread = new ConnectingThread(
                name,
                _events,
                _connectionString,
                waitBeforeConnectMillis,
                waitAfterConnectMillis,
                closeOnExit).Build();
            threads.Add(thread);
            return this;
        }

        public ConnectingThreads StartAll()
        {
            threads.ForEach(thread => thread.Start());
            return this;
        }

        public ConnectingThreads JoinAll()
        {
            threads.ForEach(thread => thread.Join());
            return this;
        }

        public IEnumerable<ThreadEvent> Events() => _events.ToArray().OfType<ThreadEvent>();

        public void Enqueue(ThreadEvent threadEvent) => _events.Enqueue(threadEvent);
    }
    
    class ConnectingThread
    {
        private string _name;
        
        private ConcurrentQueue<ThreadEvent> _events;

        private string _connectionString;

        private long _waitBeforeConnectMillis;

        private long _waitAfterConnectMillis;

        private bool _closeOnExit;
        
        internal const string NamePrefix = "thread_";
        
        public ConnectingThread(
            string name,
            ConcurrentQueue<ThreadEvent> events,
            string connectionString,
            long waitBeforeConnectMillis,
            long waitAfterConnectMillis,
            bool closeOnExit)
        {
            _name = name;
            _events = events;
            _connectionString = connectionString;
            _waitBeforeConnectMillis = waitBeforeConnectMillis;
            _waitAfterConnectMillis = waitAfterConnectMillis;
            _closeOnExit = closeOnExit;
        }

        public Thread Build()
        {
            var thread = new Thread(Execute);
            thread.Name = NamePrefix + _name;
            return thread;
        }

        private void Execute()
        {
            var connection = new SnowflakeDbConnection();
            connection.ConnectionString = _connectionString;
            Sleep(_waitBeforeConnectMillis);
            var watch = new StopWatch();
            watch.Start();
            var connected = false;
            try
            {
                connection.Open();
                connected = true;
            }
            catch (Exception exception)
            {
                watch.Stop();
                _events.Enqueue(ThreadEvent.EventConnectingFailed(_name, exception, watch.ElapsedMilliseconds));
            }
            if (connected)
            {
                watch.Stop();
                _events.Enqueue(ThreadEvent.EventConnected(_name, watch.ElapsedMilliseconds));
            }
            Sleep(_waitAfterConnectMillis);
            if (_closeOnExit)
            {
                connection.Close();
            }
        }

        private void Sleep(long millis)
        {
            if (millis <= 0)
            {
                return;
            }
            Thread.Sleep((int) millis);
        }
    }

    class ThreadEvent
    {
        public string ThreadName { get; set; }

        public string EventName { get; set; }
        
        public Exception Error { get; set; }

        public long Timestamp { get; set; }

        public long Duration { get; set; }

        private const string Connected = "CONNECTED";
        private const string WaitingForSession = "WAITING_FOR_SESSION";
        private const string FailedToConnect = "FAILED_TO_CONNECT";

        public ThreadEvent(string threadName, string eventName, Exception error, long duration)
        {
            ThreadName = threadName;
            EventName = eventName;
            Error = error;
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            Duration = duration;
        }

        public bool IsConnectedEvent() => EventName.Equals(Connected);

        public bool IsWaitingEvent() => EventName.Equals(WaitingForSession);
        
        public static ThreadEvent EventConnected(string threadName, long duration) =>
            new ThreadEvent(threadName, Connected, null, duration);

        public static ThreadEvent EventConnectingFailed(string threadName, Exception exception, long duration) =>
            new ThreadEvent(threadName, FailedToConnect, exception, duration);

        public static ThreadEvent EventWaitingForSessionStarted(string threadName) =>
            new ThreadEvent(threadName, WaitingForSession, null, 0);
    }

    class SessionPoolThreadEventHandler: SessionPoolEventHandler
    {
        private readonly ConnectingThreads _connectingThreads;

        public SessionPoolThreadEventHandler(ConnectingThreads connectingThreads)
        {
            _connectingThreads = connectingThreads;
        }

        public override void OnWaitingForSessionStarted(SessionPool sessionPool)
        {
            var threadName = Thread.CurrentThread.Name;
            var realThreadName = threadName.StartsWith(ConnectingThread.NamePrefix)
                ? threadName.Substring(ConnectingThread.NamePrefix.Length) : threadName;
            var waitingStartedEvent = ThreadEvent.EventWaitingForSessionStarted(realThreadName);
            _connectingThreads.Enqueue(waitingStartedEvent);
        }
    }
}
