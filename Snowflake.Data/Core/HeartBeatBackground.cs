using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core;

internal sealed class HeartBeatBackground
{
    private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<HeartBeatBackground>();

    private static HeartBeatBackground instance;
    private static readonly object heartBeatLock = new();

    private static List<SFSession> heartBeatConns;

    private static Thread heartBeatThread;
    private static TimeSpan s_masterTokenValidationTime = TimeSpan.Zero;
    private static TimeSpan? s_userHeartbeatFrequency;
    private static bool isHeartBeatEnd;

    private static readonly TimeSpan s_maxHeartbeatInterval = TimeSpan.FromHours(1);
    private static readonly TimeSpan s_defaultMasterValidity = TimeSpan.FromHours(4);

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

    internal static void SetValidity(long validity) => s_masterTokenValidationTime = TimeSpan.FromSeconds(validity);

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

    internal static TimeSpan ComputeHeartBeatInterval(TimeSpan masterTokenValidationTime, TimeSpan userFrequency)
    {
        var master = masterTokenValidationTime > TimeSpan.Zero ? masterTokenValidationTime : s_defaultMasterValidity;
        var minBound = new TimeSpan(master.Ticks / 16);
        var maxBound = new TimeSpan(master.Ticks / 4);
        if (maxBound > s_maxHeartbeatInterval)
            maxBound = s_maxHeartbeatInterval;

        // When no user frequency is provided, default to master/4 (preserving legacy behavior)
        if (userFrequency <= TimeSpan.Zero)
            return maxBound;

        var result = userFrequency;
        if (result < minBound)
            result = minBound;
        if (result > maxBound)
            result = maxBound;

        if (result != userFrequency)
            s_logger.Warn($"Heartbeat frequency {userFrequency.TotalSeconds}s clamped to {result.TotalSeconds}s (bounds: [{minBound.TotalSeconds}s, {maxBound.TotalSeconds}s])");

        return result;
    }

    public void AddConnection(SFSession conn, TimeSpan masterTokenValidity, TimeSpan? heartbeatFrequency = null)
    {
        lock (heartBeatLock)
        {
            if (heartBeatConns == null)
            {
                heartBeatConns = new List<SFSession>();
            }

            heartBeatConns.Add(conn);
            // for test purpose that Validity can be set to non-zero value
            if ((s_masterTokenValidationTime == TimeSpan.Zero) ||
                (masterTokenValidity < s_masterTokenValidationTime))
            {
                //update validation time
                s_masterTokenValidationTime = masterTokenValidity;
            }
            if (heartbeatFrequency.HasValue)
            {
                s_userHeartbeatFrequency = heartbeatFrequency;
            }
            if (heartBeatThread == null)
            {
                heartBeatThread = new Thread(HeartBeatAll) { IsBackground = true };
                heartBeatThread.Start();
            }
        }
    }

    public static void RemoveConnection(SFSession conn)
    {
        lock (heartBeatLock)
        {
            heartBeatConns.Remove(conn);
        }
    }

    private void HeartBeatAll()
    {
        while (true)
        {
            var copyOfHeartBeatQueue = new List<SFSession>();

            var heartBeatInterval = ComputeHeartBeatInterval(s_masterTokenValidationTime, s_userHeartbeatFrequency ?? TimeSpan.Zero);
            Thread.Sleep(heartBeatInterval);

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

            for (var i = 0; i < copyOfHeartBeatQueue.Count(); i++)
            {
                copyOfHeartBeatQueue[i].Heartbeat();
            }
        }
    }
}
