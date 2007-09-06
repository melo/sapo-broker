using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace PTCom.ApplicationBlocks.Messaging.Util
{
    public class BlockingQueue<T> : IEnumerable<T>
    {
        private Queue<T> _queue = new Queue<T>();

        public T Dequeue()
        {
            lock (_queue)
            {
                while (_queue.Count <= 0) Monitor.Wait(_queue);
                return _queue.Dequeue();
            }
        }

        public void Enqueue(T data)
        {
            if (data == null) throw new ArgumentNullException("data");
            lock (_queue)
            {
                _queue.Enqueue(data);
                Monitor.Pulse(_queue);
            }
        }

        IEnumerator<T> IEnumerable<T>.GetEnumerator()
        {
            while (true) yield return Dequeue();
        }


        public System.Collections.IEnumerator GetEnumerator()
        {
            return ((IEnumerable<T>)this).GetEnumerator();
        }

        public int Count
        {
            get
            {
                return _queue.Count;
            }
        }

    }
}
