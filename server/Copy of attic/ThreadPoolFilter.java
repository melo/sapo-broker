package org.apache.mina.filter.thread;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.mina.common.IdleStatus;
import org.apache.mina.common.IoFilterAdapter;
import org.apache.mina.common.IoFilterChain;
import org.apache.mina.common.IoHandler;
import org.apache.mina.common.IoSession;
import org.apache.mina.util.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Thread-pooling filter. This filter forwards {@link IoHandler} events to its
 * thread pool.
 * <p>
 * Use the {@link #init()} and {@link #destroy()} methods to force this filter
 * to start/stop processing events. Alternatively, {@link #init()} will be
 * called automatically the first time an instance of this filter is added to a
 * filter chain. Calling {@link #destroy()} is not required either since all
 * workers are daemon threads which means that any workers still alive when the
 * JVM terminates will die automatically.
 * 
 */
public class ThreadPoolFilter extends IoFilterAdapter
{
	private static final Logger logger = LoggerFactory.getLogger(ThreadPoolFilter.class.getName());

	private final ThreadPool threadPool;

	/**
	 * Creates a new instace with the concurrent thread pool implementation
	 * (@link ExecutorThreadPool}).
	 */
	public ThreadPoolFilter()
	{
		this(new ExecutorThreadPool());
	}

	/**
	 * Creates a new instance with the specified <tt>threadPool</tt>.
	 */
	public ThreadPoolFilter(ThreadPool threadPool)
	{
		if (threadPool == null)
		{
			throw new NullPointerException("threadPool");
		}

		this.threadPool = threadPool;
	}

	public void init()
	{
		threadPool.init();
	}

	public void destroy()
	{
		threadPool.destroy();
	}

	public void onPreAdd(IoFilterChain parent, String name, NextFilter nextFilter) throws Exception
	{
		if (!getThreadPool().isStarted())
		{
			init();
		}
	}

	/**
	 * Returns the underlying {@link ThreadPool} instance this filter uses.
	 */
	public ThreadPool getThreadPool()
	{
		return threadPool;
	}

	private void fireEvent(NextFilter nextFilter, IoSession session, EventType type, Object data)
	{
		Event event = new Event(type, nextFilter, data);
		SessionBuffer buf = SessionBuffer.getSessionBuffer(session);

		// synchronized (buf.eventQueue)
		// {
		// TODO: check-then-act ... possible threading problems
		buf.eventQueue.offer(event);
		if (buf.processingCompleted.getAndSet(false))
		{
			if (logger.isDebugEnabled())
			{
				logger.debug("Launching thread for " + session.getRemoteAddress());
			}

			threadPool.submit(new ProcessEventsRunnable(buf));
			// }
		}
	}

	private static class SessionBuffer
	{
		private static final String KEY = SessionBuffer.class.getName() + ".KEY";

		private static SessionBuffer getSessionBuffer(IoSession session)
		{
			synchronized (session)
			{
				SessionBuffer buf = (SessionBuffer) session.getAttribute(KEY);
				if (buf == null)
				{
					buf = new SessionBuffer(session);
					session.setAttribute(KEY, buf);
				}
				return buf;
			}
		}

		private final IoSession session;

		private final Queue<Event> eventQueue = new ConcurrentLinkedQueue<Event>();

		private AtomicBoolean processingCompleted = new AtomicBoolean(true);

		private SessionBuffer(IoSession session)
		{
			this.session = session;
		}
	}

	protected static class EventType
	{
		public static final EventType OPENED = new EventType("OPENED");

		public static final EventType CLOSED = new EventType("CLOSED");

		public static final EventType READ = new EventType("READ");

		public static final EventType WRITTEN = new EventType("WRITTEN");

		public static final EventType RECEIVED = new EventType("RECEIVED");

		public static final EventType SENT = new EventType("SENT");

		public static final EventType IDLE = new EventType("IDLE");

		public static final EventType EXCEPTION = new EventType("EXCEPTION");

		private final String value;

		private EventType(String value)
		{
			this.value = value;
		}

		public String toString()
		{
			return value;
		}
	}

	protected static class Event
	{
		private final EventType type;

		private final NextFilter nextFilter;

		private final Object data;

		Event(EventType type, NextFilter nextFilter, Object data)
		{
			this.type = type;
			this.nextFilter = nextFilter;
			this.data = data;
		}

		public Object getData()
		{
			return data;
		}

		public NextFilter getNextFilter()
		{
			return nextFilter;
		}

		public EventType getType()
		{
			return type;
		}
	}

	public void sessionCreated(NextFilter nextFilter, IoSession session)
	{
		nextFilter.sessionCreated(session);
	}

	public void sessionOpened(NextFilter nextFilter, IoSession session)
	{
		fireEvent(nextFilter, session, EventType.OPENED, null);
	}

	public void sessionClosed(NextFilter nextFilter, IoSession session)
	{
		fireEvent(nextFilter, session, EventType.CLOSED, null);
	}

	public void sessionIdle(NextFilter nextFilter, IoSession session, IdleStatus status)
	{
		fireEvent(nextFilter, session, EventType.IDLE, status);
	}

	public void exceptionCaught(NextFilter nextFilter, IoSession session, Throwable cause)
	{
		fireEvent(nextFilter, session, EventType.EXCEPTION, cause);
	}

	public void messageReceived(NextFilter nextFilter, IoSession session, Object message)
	{
		ByteBufferUtil.acquireIfPossible(message);
		fireEvent(nextFilter, session, EventType.RECEIVED, message);
	}

	public void messageSent(NextFilter nextFilter, IoSession session, Object message)
	{
		ByteBufferUtil.acquireIfPossible(message);
		fireEvent(nextFilter, session, EventType.SENT, message);
	}

	protected void processEvent(NextFilter nextFilter, IoSession session, EventType type, Object data)
	{
		if (type == EventType.RECEIVED)
		{
			nextFilter.messageReceived(session, data);
			ByteBufferUtil.releaseIfPossible(data);
		}
		else if (type == EventType.SENT)
		{
			nextFilter.messageSent(session, data);
			ByteBufferUtil.releaseIfPossible(data);
		}
		else if (type == EventType.EXCEPTION)
		{
			nextFilter.exceptionCaught(session, (Throwable) data);
		}
		else if (type == EventType.IDLE)
		{
			nextFilter.sessionIdle(session, (IdleStatus) data);
		}
		else if (type == EventType.OPENED)
		{
			nextFilter.sessionOpened(session);
		}
		else if (type == EventType.CLOSED)
		{
			nextFilter.sessionClosed(session);
		}
	}

	public void filterWrite(NextFilter nextFilter, IoSession session, WriteRequest writeRequest)
	{
		nextFilter.filterWrite(session, writeRequest);
	}

	public void filterClose(NextFilter nextFilter, IoSession session) throws Exception
	{
		nextFilter.filterClose(session);
	}

	private class ProcessEventsRunnable implements Runnable
	{
		private final SessionBuffer buffer;

		ProcessEventsRunnable(SessionBuffer buffer)
		{
			this.buffer = buffer;
		}

		public void run()
		{
			while (true)
			{
				Event event = buffer.eventQueue.poll();

				if (event == null)
				{
					buffer.processingCompleted.set(true);
					break;
				}

				processEvent(event.getNextFilter(), buffer.session, event.getType(), event.getData());
			}

			if (logger.isDebugEnabled())
			{
				logger.debug("Exiting since queue is empty for " + buffer.session.getRemoteAddress());
			}
		}
	}
}
