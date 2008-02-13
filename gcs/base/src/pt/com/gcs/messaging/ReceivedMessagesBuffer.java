package pt.com.gcs.messaging;

import java.util.HashMap;
import java.util.Map;

public class ReceivedMessagesBuffer
{
	private final Map<String, String> _store = new HashMap<String, String>();

	private final Map<Long, String> _index = new HashMap<Long, String>();

	private long _entry = 0L;

	private long _lastRemovedEntry = 0L;

	private static final int DEFAULT_BUFFER_SIZE = 5000;

	private final int _bufferSize;

	public ReceivedMessagesBuffer()
	{
		this(DEFAULT_BUFFER_SIZE);
	}

	public ReceivedMessagesBuffer(int bufferSize)
	{
		_bufferSize = bufferSize;
	}

	public boolean isDuplicate(String msgId)
	{
		synchronized (this)
		{
			if (_store.containsKey(msgId))
			{
				return true;
			}
			else
			{
				return false;
			}
		}
	}

	public void put(String msgId)
	{
		synchronized (this)
		{
			_store.put(msgId, msgId);
			_entry++;
			_index.put(_entry, msgId);
			if (_entry > _bufferSize)
			{
				_lastRemovedEntry++;
				String mid = _index.remove(_lastRemovedEntry);
				if (mid != null)
				{
					_store.remove(mid);
				}
			}
		}
	}

}
