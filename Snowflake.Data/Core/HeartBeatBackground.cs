using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Snowflake.Data.Core;

internal sealed class HeartBeatBackground
{
    private static HeartBeatBackground instance = null;
    private static readonly object heartBeatLock = new();

    private static readonly object _heartBeatLock = new();
    private static List<SFSession> heartBeatConns = null;

    private static Thread heartBeatThread = null;
    private static long masterTokenValidationTimeInSec = 0;
    private static bool isHeartBeatEnd = false;

    private HeartBeatBackground()
    {

    }

    ~HeartBeatBackground()
    {
        isHeartBeatEnd = true;
    }

    internal static int GetQueueLength()
    {
        lock (heartBeatLock)
        {
            if (heartBeatConns == null)
            {
                return 0;
            }
            return heartBeatConns.Count();
        }
    }

    internal static void SetValidity(long validity)
    {
        masterTokenValidationTimeInSec = validity;
    }

    public static HeartBeatBackground Instance
    {
        get
        {
            lock (heartBeatLock)
            {
                if (instance == null)
                {
                    instance = new HeartBeatBackground();
                }
                return instance;
            }
        }
    }

    private long GetHeartBeatInterval(long masterTokenValidationTime)
    {
        return masterTokenValidationTime / 4;
    }

    public void AddConnection(SFSession conn, long masterTokenValidityInSecs)
    {
        lock (heartBeatLock)
        {
            if (heartBeatConns == null)
            {
                heartBeatConns = new List<SFSession>();
            }

            heartBeatConns.Add(conn);
            // for test purpose that Validity can be set to non-zero value
            if ((masterTokenValidationTimeInSec == 0) ||
                (masterTokenValidityInSecs < masterTokenValidationTimeInSec))
            {
                //update validation time
                masterTokenValidationTimeInSec = masterTokenValidityInSecs;
            }
            if (heartBeatThread == null)
            {
                heartBeatThread = new Thread(HeartBeatAll) { IsBackground = true };
                heartBeatThread.Start();
            }
        }
    }

    public void RemoveConnection(SFSession conn)
    {
        lock (heartBeatLock)
        {
            heartBeatConns.Remove(conn);
        }
    }

    void HeartBeatAll()
    {
        while (true)
        {
            List<SFSession> copyOfHeartBeatQueue = new List<SFSession>();

            long heartBeatInterval = GetHeartBeatInterval(masterTokenValidationTimeInSec);
            Thread.Sleep(TimeSpan.FromSeconds(heartBeatInterval));

            if (isHeartBeatEnd)
            {
                return;
            }
            else
            {
                lock (heartBeatLock)
                {
                    copyOfHeartBeatQueue.AddRange(heartBeatConns);
                }
            }

            for (int i = 0; i < copyOfHeartBeatQueue.Count(); i++)
            {
                copyOfHeartBeatQueue[i].Heartbeat();
            }
        }
    }
}
