using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Linq;
using Snowflake.Data.Log;

namespace Snowflake.Data.Core
{
    internal class QueryContextValue
    {
        // base64 encoded string of Opaque information
        [JsonProperty(PropertyName = "base64Data")]
        public string base64Data { get; set; }

        public QueryContextValue(string context)
        {
            base64Data = context;
        }
    }
    internal class QueryContextValueEmpty
    {
        // empty object with no filed
    }

    internal class QueryContextElement
    {
        // database id as key. (bigint)
        public long id { get; set; }

        // When the query context read (bigint). Compare for same id.
        public long readTimestamp { get; set; }

        // Priority of the query context (bigint). Compare for different ids.
        public long priority { get; set; }

        // Opaque information (object with a value of base64 encoded string).
        public string context { get; set; }

        public QueryContextElement(long in_id, long in_timestamp,
                            long in_pri, string in_context)
        {
            id = in_id;
            readTimestamp = in_timestamp;
            priority = in_pri;
            context = in_context;
        }

        public override bool Equals(Object obj)
        {
            if (obj == null || !(obj is QueryContextElement))
            {
                return false;
            }

            QueryContextElement other = (QueryContextElement)obj;
            return (id == other.id) &&
                   (priority == other.priority) &&
                   (readTimestamp == other.readTimestamp) &&
                   (context.Equals(other.context));
        }

        public override int GetHashCode()
        {
            int hash = 31;

            hash = hash * 31 + (int)id;
            hash += (hash * 31) + (int)readTimestamp;
            hash += (hash * 31) + (int)priority;
            hash += (hash * 31) + context.GetHashCode();

            return hash;
        }
    };

    class elementComparer : IComparer<QueryContextElement>
    {
        public int Compare(QueryContextElement x, QueryContextElement y)
        {
            if (x.priority != y.priority) return x.priority > y.priority ? 1 : -1;
            if (x.id != y.id) return x.id > y.id ? 1 : -1;
            if (x.readTimestamp != y.readTimestamp) return x.readTimestamp > y.readTimestamp ? 1 : -1;
            return 0;
        }
    }

    internal class QueryContextCache
    {
        private int capacity; // Capacity of the cache
        private Dictionary<long, QueryContextElement> idMap; // Map for id and QCC
        private Dictionary<long, QueryContextElement> priorityMap; // Map for priority and QCC
        private Dictionary<long, QueryContextElement>newPriorityMap; // Intermediate map for priority and QCC for current round of merging
        private SortedSet<QueryContextElement> cacheSet; // Order data as per priority
        private SFLogger logger = SFLoggerFactory.GetLogger<QueryContextCache>();

        public QueryContextCache(int capacity)
        {
            this.capacity = capacity;
            idMap = new Dictionary<long, QueryContextElement>();
            priorityMap = new Dictionary<long, QueryContextElement>();
            newPriorityMap = new Dictionary<long, QueryContextElement>();
            cacheSet = new SortedSet<QueryContextElement>(new elementComparer());
        }

        public void merge(long id, long readTimestamp, long priority, string context)
        {
            if (idMap.ContainsKey(id))
            {
                // ID found in the cache
                QueryContextElement qce = idMap[id];
                if (readTimestamp > qce.readTimestamp)
                {
                    if (qce.priority == priority)
                    {
                        // Same priority, overwrite new data at same place
                        qce.readTimestamp = readTimestamp;
                        qce.context = context;
                    }
                    else
                    {
                        // Change in priority
                        QueryContextElement newQCE =
                            new QueryContextElement(id, readTimestamp, priority, context);

                        replaceQCE(qce, newQCE);
                    } // new priority
                } // new data is recent
                else if (readTimestamp == qce.readTimestamp && qce.priority != priority)
                {
                    // Same read timestamp but change in priority
                    QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
                    replaceQCE(qce, newQCE);
                }
            } // id found
            else
            {
                // new id
                if (priorityMap.ContainsKey(priority))
                {
                    // Same priority with different id
                    QueryContextElement qce = priorityMap[priority];
                    // Replace with new data
                    QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
                    replaceQCE(qce, newQCE);
                }
                else
                {
                    // new priority
                    // Add new element in the cache
                    QueryContextElement newQCE = new QueryContextElement(id, readTimestamp, priority, context);
                    addQCE(newQCE);
                }
            }
        }

        /** Sync the newPriorityMap with the priorityMap at the end of current round of merge */
        public void syncPriorityMap()
        {
            logger.Debug(
                $"syncPriorityMap called priorityMap size = {priorityMap.Count}, newPrioirtyMap size = {newPriorityMap.Count}");
            foreach (KeyValuePair<long, QueryContextElement> entry in newPriorityMap)
            {
                priorityMap.Add(entry.Key, entry.Value);
            }
            // clear the newPriorityMap for next round of QCC merge(a round consists of multiple entries)
            newPriorityMap.Clear();
        }

        /**
         * After the merge, loop through priority list and make sure cache is at most capacity. Remove all
         * other elements from the list based on priority.
         */
        public void checkCacheCapacity()
        {
            logger.Debug(
                $"checkCacheCapacity() called. cacheSet size {cacheSet.Count} cache capacity {capacity}");
            if (cacheSet.Count > capacity)
            {
                // remove elements based on priority
                while (cacheSet.Count > capacity)
                {
                    QueryContextElement qce = cacheSet.Last();
                    removeQCE(qce);
                }
            }

            logger.Debug(
                $"checkCacheCapacity() returns. cacheSet size {cacheSet.Count} cache capacity {capacity}");
        }

        /** Clear the cache. */
        public void clearCache()
        {
            logger.Debug("clearCache() called");
            idMap.Clear();
            priorityMap.Clear();
            cacheSet.Clear();
            logger.Debug($"clearCache() returns. Number of entries in cache now {cacheSet.Count}");
        }

        public void setCapacity(int cap)
        {
            // check without locking first for performance reason
            if (capacity == cap)
                return;

            logger.Debug($"set capacity from {capacity} to {cap}");
            capacity = cap;
            checkCacheCapacity();
            logCacheEntries();
        }

        /**
         * Get all elements in the cache in the order of the priority.
         * Make it public for test purpose.
         *
         * @return TreeSet containing cache elements
         */
        public SortedSet<QueryContextElement> getElements()
        {
            return cacheSet;
        }

        public int getSize()
        {
            return cacheSet.Count;
        }

        /**
         * Update query context chache with the query context received in query response
         * @param queryContext: the QueryContext body parsed from query response
         */
        public void update(ResponseQueryContext queryContext)
        {
            // Log existing cache entries
            logCacheEntries();

            if (queryContext == null || queryContext.entries == null)
            {
                // Clear the cache
                clearCache();
                return;
            }
            foreach (ResponseQueryContextElement entry in queryContext.entries)
            {
                merge(entry.id, entry.readTimestamp, entry.priority, entry.context);
            }

            syncPriorityMap();

            // After merging all entries, truncate to capacity
            checkCacheCapacity();
            // Log existing cache entries
            logCacheEntries();
        }

        /**
         * Get the query context can be sent through query request
         * @return the QueryContext body
         */
        public RequestQueryContext getQueryContextRequest()
        {
            RequestQueryContext reqQCC = new RequestQueryContext();
            reqQCC.entries = new List<RequestQueryContextElement>();
            foreach (QueryContextElement elem in cacheSet)
            {
                RequestQueryContextElement reqElem = new RequestQueryContextElement();
                reqElem.id = elem.id;
                reqElem.priority = elem.priority;
                reqElem.readTimestamp = elem.readTimestamp;
                if (elem.context != null)
                {
                    reqElem.context = new QueryContextValue(elem.context);
                }
                else
                {
                    reqElem.context = new QueryContextValueEmpty();
                }
                reqQCC.entries.Add(reqElem);
            }

            return reqQCC;
        }

        /**
         * Get the query context in response format, for test purpose
         * @return the QueryContext body
         */
        public ResponseQueryContext getQueryContextResponse()
        {
            ResponseQueryContext rspQCC = new ResponseQueryContext();
            rspQCC.entries = new List<ResponseQueryContextElement>();
            foreach (QueryContextElement elem in cacheSet)
            {
                ResponseQueryContextElement rspElem = new ResponseQueryContextElement();
                rspElem.id = elem.id;
                rspElem.priority = elem.priority;
                rspElem.readTimestamp = elem.readTimestamp;
                rspElem.context = elem.context;
                rspQCC.entries.Add(rspElem);
            }

            return rspQCC;
        }

        /**
         * Add an element in the cache.
         *
         * @param qce element to add
         */
        private void addQCE(QueryContextElement qce)
        {
            idMap.Add(qce.id, qce);
            // In a round of merge operations, we should save the new priority->qce mapping in an additional map
            // and sync `newPriorityMap` to `priorityMap` at the end of a for loop of `merge` operations
            newPriorityMap.Add(qce.priority, qce);
            cacheSet.Add(qce);
        }

        /**
         * Remove an element from the cache.
         *
         * @param qce element to remove.
         */
        private void removeQCE(QueryContextElement qce)
        {
            cacheSet.Remove(qce);
            priorityMap.Remove(qce.priority);
            idMap.Remove(qce.id);
        }

        /**
         * Replace the cache element with a new response element. Remove old element exist in the cache
         * and add a new element received.
         *
         * @param oldQCE an element exist in the cache
         * @param newQCE a new element just received.
         */
        private void replaceQCE(QueryContextElement oldQCE, QueryContextElement newQCE)
        {
            // Remove old element from the cache
            removeQCE(oldQCE);
            // Add new element in the cache
            addQCE(newQCE);
        }

        /** Debugging purpose, log the all entries in the cache. */
        private void logCacheEntries()
        {
#if DEBUG
            foreach (QueryContextElement elem in cacheSet)
            {
                logger.Debug($"Cache Entry: id: {elem.id} readTimestamp: {elem.readTimestamp} priority: {elem.priority}");
            }
#endif
        }
    }
}
