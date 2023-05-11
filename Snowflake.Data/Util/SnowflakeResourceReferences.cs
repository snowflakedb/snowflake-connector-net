using Snowflake.Data.Log;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Linq;

namespace Snowflake.Data.Util
{
    public class SnowflakeResourceReferences : IDisposable
    {
        private static readonly SFLogger logger = SFLoggerFactory.GetLogger<SnowflakeResourceReferences>();

        private bool disposedValue;

        private readonly OrderedDictionary _items = new OrderedDictionary();

        public int Count
        {
            get
            {
                lock (_items)
                {
                    return _items.Count;
                }
            }
        }

        public string Name { get; }

        public bool IsDisposed { get => this.disposedValue; }

        public IEnumerable<string> Keys
        {
            get
            {
                lock (_items)
                {
                    return this._items.Keys.Cast<string>().ToList();
                }
            }
        }

        public SnowflakeResourceReferences()
        {
            Name = "global-" + Guid.NewGuid().ToString();
        }

        public SnowflakeResourceReferences(string name)
        {
            Name = name;
        }

        private void Trace(string message)
        {
#if DEBUG
            logger.Debug(message);
#endif
        }

        public void Add(ISnowflakeResource resource)
        {
            Debug.Assert(resource != null);

            disposedValue = false;
            var resourceRef = new WeakReference<ISnowflakeResource>(resource);
            lock (_items)
            {
                _items.Add(resource.ResourceID, resourceRef);
                resource.Disposed += this.OnResourceDisposed;
                Trace($"Added. name={Name} size={Count}");
            }
        }

        public void Remove(ISnowflakeResource resource)
        {
            Debug.Assert(resource != null);

            lock (_items)
            {
                this.Remove(resource.ResourceID);
            }
        }

        private void Remove(string id)
        {
            Debug.Assert(id != null);

            lock (_items)
            {
                _items.Remove(id);
                Trace($"Removed. name={Name}, size={Count}");
            }
        }

        public bool TryGetValue<T>(string id, out T value) where T : class
        {
            Debug.Assert(id != null);

            WeakReference<ISnowflakeResource> itemRef;
            lock (_items)
            {
                if (!_items.Contains(id))
                {
                    value = null;
                    return false;
                }
                itemRef = _items[id] as WeakReference<ISnowflakeResource>;
            }

            if (!itemRef.TryGetTarget(out var o))
            {
                value = null;
                return false;
            }

            value = (T)o;
            return true;
        }

        public void OnResourceDisposed(ISnowflakeResource resource, EventArgs e)
        {
            this.Remove(resource);
        }

        /// <summary>
        /// Dispose all resources. If empty, do nothing.
        /// </summary>
        public void DisposeAll()
        {
            lock (_items)
            {
                Stack<string> keyStack = new Stack<string>(this.Keys);
                while (keyStack.Any())
                {
                    var key = keyStack.Pop();
                    if (this.TryGetValue<ISnowflakeResource>(key, out var resource))
                    {
                        Trace($"try dispose. name={Name}, resource={resource.ResourceID}");
                        resource.Dispose();
                    }
                }
                disposedValue = true;
            }
        }

        public bool DisposeAllSilently()
        {
            try
            {
                DisposeAll();
                return true;
            }
            catch (Exception ex)
            {
                logger.Warn($"An error occurred during dispose, but the exception was suppressed. name={Name}", ex);
                return false;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    DisposeAllSilently();
                }
            }
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
