using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Linq;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    internal class QueryContextElement
    {
        // database id as key. (bigint)
        public long Id { get; set; }

        // When the query context read (bigint). Compare for same id.
        public long ReadTimestamp { get; set; }

        // Priority of the query context (bigint). Compare for different ids.
        public long Priority { get; set; }

        // Opaque information (object with a value of base64 encoded string).
        public string Context { get; set; }

        public QueryContextElement(long id, long timestamp,
                            long pri, string context)
        {
            Id = id;
            ReadTimestamp = timestamp;
            Priority = pri;
            Context = context;
        }

        public override bool Equals(Object obj)
        {
            if (obj == null || !(obj is QueryContextElement))
            {
                return false;
            }

            QueryContextElement other = (QueryContextElement)obj;
            return (Id == other.Id) &&
                   (Priority == other.Priority) &&
                   (ReadTimestamp == other.ReadTimestamp) &&
                   (Context.Equals(other.Context));
        }

        public override int GetHashCode()
        {
            int hash = 31;

            hash = hash * 31 + (int)Id;
            hash += (hash * 31) + (int)ReadTimestamp;
            hash += (hash * 31) + (int)Priority;
            hash += (hash * 31) + Context.GetHashCode();

            return hash;
        }
    };

    class ElementComparer : IComparer<QueryContextElement>
    {
        public int Compare(QueryContextElement x, QueryContextElement y)
        {
            if (x.Priority != y.Priority) return x.Priority > y.Priority ? 1 : -1;
            if (x.Id != y.Id) return x.Id > y.Id ? 1 : -1;
            if (x.ReadTimestamp != y.ReadTimestamp) return x.ReadTimestamp > y.ReadTimestamp ? 1 : -1;
            return 0;
        }
    }

    internal class QueryContextCache
    {
        private readonly object _qccLock;
        private int _capacity; // Capacity of the cache
        private Dictionary<long, QueryContextElement> _idMap; // Map for id and QCC
        private Dictionary<long, QueryContextElement> _priorityMap; // Map for priority and QCC
        private Dictionary<long, QueryContextElement> _newPriorityMap; // Intermediate map for priority and QCC for current round of merging
        private SortedSet<QueryContextElement> _cacheSet; // Order data as per priority
        private SFLogger _logger = SFLoggerFactory.GetLogger<QueryContextCache>();

        public QueryContextCache(int capacity)
        {
            _qccLock = new object();
            _capacity = capacity;
            _idMap = new Dictionary<long, QueryContextElement>();
            _priorityMap = new Dictionary<long, QueryContextElement>();
            _newPriorityMap = new Dictionary<long, QueryContextElement>();
            _cacheSet = new SortedSet<QueryContextElement>(new ElementComparer());
        }

        public void Merge(long id, long readTimestamp, long priority, string context)
        {
            if (_idMap.ContainsKey(id))
            {
                _logger.Debug(
                    $"Merge with existing id in cache = {id}, priority = {priority}");
                // ID found in the cache
                QueryContextElement qce = _idMap[id];
                if (readTimestamp > qce.ReadTimestamp)
                {
                    if (qce.Priority == priority)
                    {
                        // Same priority, overwrite new data at same place
                        qce.ReadTimestamp = readTimestamp;
                        qce.Context = context;
                    }
                    else
                    {
                        // Change in priority
                        QueryContextElement newQCE =
                            new QueryContextElement(id, readTimestamp, priority, context);

                        ReplaceQCE(qce, newQCE);
                    } // new priority
                } // new data is recent
                else if (readTimestamp == qce.ReadTimestamp && qce.Priority != priority)
                {
                    // Same read timestamp but change in priority
                    QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
                    ReplaceQCE(qce, newQCE);
                }
            } // id found
            else
            {
                // new id
                if (_priorityMap.ContainsKey(priority))
                {
                    _logger.Debug(
                        $"Merge with existing priority in cache = {id}, priority = {priority}");
                    // Same priority with different id
                    QueryContextElement qce = _priorityMap[priority];
                    // Replace with new data
                    QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
                    ReplaceQCE(qce, newQCE);
                }
                else
                {
                    // new priority
                    // Add new element in the cache
                    _logger.Debug(
                        $"Adding new QCC item with either id nor priority found in cache id = {id}, priority = {priority}");
                    QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
                    AddQCE(newQCE);
                }
            }
        }

        /** Sync the newPriorityMap with the priorityMap at the end of current round of merge */
        public void SyncPriorityMap()
        {
            _logger.Debug(
                $"syncPriorityMap called priorityMap size = {_priorityMap.Count}, newPrioirtyMap size = {_newPriorityMap.Count}");
            foreach (KeyValuePair<long, QueryContextElement> entry in _newPriorityMap)
            {
                _priorityMap.Add(entry.Key, entry.Value);
            }
            // clear the newPriorityMap for next round of QCC merge(a round consists of multiple entries)
            _newPriorityMap.Clear();
        }

        /**
         * After the merge, loop through priority list and make sure cache is at most capacity. Remove all
         * other elements from the list based on priority.
         */
        public void CheckCacheCapacity()
        {
            _logger.Debug(
                $"checkCacheCapacity() called. cacheSet size {_cacheSet.Count} cache capacity {_capacity}");
            if (_cacheSet.Count > _capacity)
            {
                // remove elements based on priority
                while (_cacheSet.Count > _capacity)
                {
                    QueryContextElement qce = _cacheSet.Last();
                    RemoveQCE(qce);
                }
            }

            _logger.Debug(
                $"checkCacheCapacity() returns. cacheSet size {_cacheSet.Count} cache capacity {_capacity}");
        }

        /** Clear the cache. */
        public void ClearCache()
        {
            _logger.Debug("clearCache() called");
            _idMap.Clear();
            _priorityMap.Clear();
            _cacheSet.Clear();
            _logger.Debug($"clearCache() returns. Number of entries in cache now {_cacheSet.Count}");
        }

        public void ClearCacheSafely()
        {
            lock (_qccLock)
            {
                ClearCache();
            }
        }

        public void SetCapacity(int cap)
        {
            // check without locking first for performance reason
            if (_capacity == cap)
                return;
            lock (_qccLock)
            {
                if (_capacity == cap)
                    return;

                _logger.Debug($"set capacity from {_capacity} to {cap}");
                _capacity = cap;
                CheckCacheCapacity();
                LogCacheEntries();
            }
        }

        /**
         * Get all elements in the cache in the order of the priority.
         * Make it public for test purpose.
         *
         * @return TreeSet containing cache elements
         */
        public SortedSet<QueryContextElement> getElements()
        {
            return _cacheSet;
        }

        public int GetSize()
        {
            return _cacheSet.Count;
        }

        /**
         * Update query context chache with the query context received in query response
         * @param queryContext: the QueryContext body parsed from query response
         */
        public void Update(ResponseQueryContext queryContext)
        {
            lock (_qccLock)
            {
                // Log existing cache entries
                LogCacheEntries();

                if (queryContext == null || queryContext.Entries == null)
                {
                    // Clear the cache
                    ClearCache();
                    return;
                }
                foreach (ResponseQueryContextElement entry in queryContext.Entries)
                {
                    Merge(entry.Id, entry.ReadTimestamp, entry.Priority, entry.Context);
                }

                SyncPriorityMap();

                // After merging all entries, truncate to capacity
                CheckCacheCapacity();
                // Log existing cache entries
                LogCacheEntries();
            }
        }

        /**
         * Get the query context can be sent through query request
         * @return the QueryContext body
         */
        public RequestQueryContext GetQueryContextRequest()
        {
            RequestQueryContext reqQCC = new RequestQueryContext();
            reqQCC.Entries = new List<RequestQueryContextElement>();
            lock (_qccLock)
            {
                foreach (QueryContextElement elem in _cacheSet)
                {
                    RequestQueryContextElement reqElem = new RequestQueryContextElement(elem);
                    reqQCC.Entries.Add(reqElem);
                }
            }

            return reqQCC;
        }

        /**
         * Get the query context in response format, for test purpose
         * @return the QueryContext body
         */
        public ResponseQueryContext GetQueryContextResponse()
        {
            ResponseQueryContext rspQCC = new ResponseQueryContext();
            rspQCC.Entries = new List<ResponseQueryContextElement>();
            lock (_qccLock)
            {
                foreach (QueryContextElement elem in _cacheSet)
                {
                    ResponseQueryContextElement rspElem = new ResponseQueryContextElement(elem);
                    rspQCC.Entries.Add(rspElem);
                }
            }

            return rspQCC;
        }

        /**
         * Add an element in the cache.
         *
         * @param qce element to add
         */
        private void AddQCE(QueryContextElement qce)
        {
            _idMap.Add(qce.Id, qce);
            // In a round of merge operations, we should save the new priority->qce mapping in an additional map
            // and sync `newPriorityMap` to `priorityMap` at the end of a for loop of `merge` operations
            _newPriorityMap.Add(qce.Priority, qce);
            _cacheSet.Add(qce);
        }

        /**
         * Remove an element from the cache.
         *
         * @param qce element to remove.
         */
        private void RemoveQCE(QueryContextElement qce)
        {
            _cacheSet.Remove(qce);
            _priorityMap.Remove(qce.Priority);
            _idMap.Remove(qce.Id);
        }

        /**
         * Replace the cache element with a new response element. Remove old element exist in the cache
         * and add a new element received.
         *
         * @param oldQCE an element exist in the cache
         * @param newQCE a new element just received.
         */
        private void ReplaceQCE(QueryContextElement oldQCE, QueryContextElement newQCE)
        {
            // Remove old element from the cache
            RemoveQCE(oldQCE);
            // Add new element in the cache
            AddQCE(newQCE);
        }

        /** Debugging purpose, log the all entries in the cache. */
        private void LogCacheEntries()
        {
            if (_logger.IsDebugEnabled())
            {
                foreach (QueryContextElement elem in _cacheSet)
                {
                    _logger.Debug($"Cache Entry: id: {elem.Id} readTimestamp: {elem.ReadTimestamp} priority: {elem.Priority}");
                }
            }
        }
    }
}
