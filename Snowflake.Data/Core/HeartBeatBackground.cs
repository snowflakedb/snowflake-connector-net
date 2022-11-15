using System;
using System.Collections.Generic;
using System.Threading;

namespace Snowflake.Data.Core
{
    public sealed class HeartBeatBackground
    {
        private static HeartBeatBackground _instance;
        private static readonly object HeartBeatLock = new object();
        private List<SFSession> _heartBeatConns;

        private Timer _heartbeatTimer;
        private long _heartbeatFrequencyInSecs;


        private HeartBeatBackground()
        {

        }

        ~HeartBeatBackground()
        {
            _heartbeatTimer?.Dispose();
        }

        public static HeartBeatBackground Instance
        {
            get
            {
                lock (HeartBeatLock)
                {
                    if (_instance == null)
                    {
                        _instance = new HeartBeatBackground();
                    }
                    return _instance;
                }
            }
        }

        public void AddConnection(SFSession conn, long heartbeatFrequencyInSecs)
        {
            lock(HeartBeatLock)
            {
                if (_heartBeatConns == null)
                {
                    _heartBeatConns = new List<SFSession>();
                }

                _heartBeatConns.Add(conn);
                UpdateHeartbeatFrequency(heartbeatFrequencyInSecs);
            }
        }

        public void UpdateHeartbeatFrequency(long heartbeatFrequencyInSecs)
        {
            lock (HeartBeatLock)
            {
                if (_heartbeatTimer == null)
                {
                    _heartbeatFrequencyInSecs = heartbeatFrequencyInSecs;
                    _heartbeatTimer = new Timer(HeartBeatAll, null, TimeSpan.FromSeconds(_heartbeatFrequencyInSecs),
                        TimeSpan.FromSeconds(_heartbeatFrequencyInSecs));
                }
                else if (heartbeatFrequencyInSecs < _heartbeatFrequencyInSecs)
                {
                    //update validation time
                    _heartbeatFrequencyInSecs = heartbeatFrequencyInSecs;
                    _heartbeatTimer.Change(TimeSpan.Zero, TimeSpan.FromSeconds(_heartbeatFrequencyInSecs));
                }
            }
        }

        public void RemoveConnection(SFSession conn)
        {
            lock(HeartBeatLock)
            {
                _heartBeatConns.Remove(conn);
                if (_heartBeatConns.Count == 0 && _heartbeatTimer != null)
                {
                    //Stop timer when there are no more connections left
                    _heartbeatTimer?.Dispose();
                    _heartbeatTimer = null;
                }
            }
        }

        private void HeartBeatAll(object state)
        {
            var copyOfHeartBeatQueue = new List<SFSession>();
            
            lock (HeartBeatLock)
            {
                copyOfHeartBeatQueue.AddRange(_heartBeatConns);
            }

            foreach (var session in copyOfHeartBeatQueue)
            {
                session.Heartbeat();
            }
        }
    }
}
