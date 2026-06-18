using System;
using System.Threading;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core.Revocation
{
    internal class CrlCacheManager
    {
        private static readonly SFLogger s_logger = SFLoggerFactory.GetLogger<CrlCacheManager>();

        private readonly MemoryCrlCache _memoryCache;
        private readonly FileCrlCache _fileCache;
        private readonly Timer _cleanupTimer;
        private readonly TimeSpan _cleanupInterval;

        private CrlCacheManager(
            MemoryCrlCache memoryCache,
            FileCrlCache fileCache,
            TimeSpan cleanupInterval)
        {
            _memoryCache = memoryCache;
            _fileCache = fileCache;
            _cleanupInterval = cleanupInterval;

            // Create the timer but don't start it yet
            _cleanupTimer = new Timer(CleanupCallback, null, Timeout.Infinite, Timeout.Infinite);
        }

        public static CrlCacheManager Build(
            bool inMemoryCacheEnabled,
            bool onDiskCacheEnabled,
            TimeSpan cleanupInterval,
            TimeSpan cacheValidityTime)
        {
            MemoryCrlCache memoryCache = null;
            if (inMemoryCacheEnabled)
            {
                s_logger.Debug("Enabling in-memory CRL cache");
                memoryCache = new MemoryCrlCache(cacheValidityTime);
            }
            else
            {
                s_logger.Debug("In-memory CRL cache disabled");
            }

            FileCrlCache fileCache = null;
            if (onDiskCacheEnabled)
            {
                s_logger.Debug("Enabling file based CRL cache");
                fileCache = FileCrlCache.CreateInstance(cleanupInterval);
            }
            else
            {
                s_logger.Debug("File based CRL cache disabled");
            }

            var manager = new CrlCacheManager(memoryCache, fileCache, cleanupInterval);

            if (inMemoryCacheEnabled || onDiskCacheEnabled)
            {
                manager.StartPeriodicCleanup();
            }

            return manager;
        }

        public Crl Get(string crlUrl)
        {
            var crl = _memoryCache?.Get(crlUrl);
            if (crl != null)
            {
                return crl;
            }

            crl = _fileCache?.Get(crlUrl);
            if (crl != null)
            {
                _memoryCache?.Set(crlUrl, crl);
                return crl;
            }

            s_logger.Debug($"CRL not found in cache for {crlUrl}");
            return null;
        }

        public void Set(string crlUrl, Crl crl)
        {
            _memoryCache?.Set(crlUrl, crl);
            _fileCache?.Set(crlUrl, crl);
            s_logger.Debug($"CRL cached for {crlUrl}");
        }

        private void StartPeriodicCleanup()
        {
            var cleanupIntervalMs = (int)_cleanupInterval.TotalMilliseconds;
            _cleanupTimer.Change(cleanupIntervalMs, cleanupIntervalMs);

            s_logger.Debug(
                $"Scheduled CRL cache cleanup task to run every {_cleanupInterval.TotalSeconds} seconds.");
        }

        private void CleanupCallback(object state)
        {
            try
            {
                s_logger.Debug(
                    $"Running periodic CRL cache cleanup with interval {_cleanupInterval.TotalSeconds} seconds");
                _memoryCache?.Cleanup();
                _fileCache?.Cleanup();
            }
            catch (Exception e)
            {
                s_logger.Error("An error occurred during scheduled CRL cache cleanup.", e);
            }
        }
    }
}

