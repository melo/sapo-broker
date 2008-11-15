package pt.com.gcs.messaging;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.caudexorigo.cryto.MD5;
import org.caudexorigo.text.StringUtils;

public class Message implements Externalizable
{
	private static final AtomicLong SEQ = new AtomicLong(0L);

	private static final long serialVersionUID = -3656321513130930115L;

	public static final int DEFAULT_PRIORITY = 4;

	private static final long DEFAULT_EXPIRY = 1000L * 3600L * 24L * 3L; // 3 days

	private String _content = "";

	private String _correlationId = "";

	private String _destination;

	private String _id;

	private int _priority = DEFAULT_PRIORITY;

	private String _sourceApp = "Undefined Source";

	private long _timestamp = System.currentTimeMillis();

	private long _expiration = System.currentTimeMillis() + DEFAULT_EXPIRY;

	private MessageType _type = MessageType.UNDEF;
	
	private boolean _isFromRemotePeer = false;

	private static final String SEPARATOR = "<#>";

	private static final String BASE_MESSAGE_ID;
	
	
	static
	{
		BASE_MESSAGE_ID = MD5.getHashString(UUID.randomUUID().toString());
	}
	
	protected static String getBaseMessageId()
	{
		return BASE_MESSAGE_ID + "#";
	}

	public Message()
	{
		_id = BASE_MESSAGE_ID + "#" + SEQ.incrementAndGet();
	}

	public Message(String destination, String content)
	{
		checkArg(destination);
		checkArg(content);
		_content = content;
		_destination = destination;
		_id = BASE_MESSAGE_ID + "#" + SEQ.incrementAndGet();
	}

	public Message(String id, String destination, String content)
	{
		checkArg(id);
		checkArg(destination);
		checkArg(content);
		_id = id;
		_destination = destination;
		_content = content;
	}

	private void checkArg(String value)
	{
		if (StringUtils.isBlank(value))
		{
			throw new IllegalArgumentException("Invalid argument. Message initializers must not empty");
		}
	}

	public String getContent()
	{
		return _content;
	}

	public String getCorrelationId()
	{
		return _correlationId;
	}

	public String getDestination()
	{
		return _destination;
	}

	public String getMessageId()
	{
		return _id;
	}

	public int getPriority()
	{
		return _priority;
	}

	public String getSourceApp()
	{
		return _sourceApp;
	}

	public long getTimestamp()
	{
		return _timestamp;
	}

	public long getExpiration()
	{
		return _expiration;
	}

	public MessageType getType()
	{
		return _type;
	}

	public void setContent(String content)
	{
		_content = content;
	}

	public void setCorrelationId(String cid)
	{
		if (StringUtils.isNotBlank(cid))
		{
			_correlationId = cid;
		}
	}

	public void setDestination(String destination)
	{
		_destination = destination;
	}

	public void setMessageId(String id)
	{
		if (StringUtils.isNotBlank(id))
		{
			_id = id;
		}
	}

	public void setPriority(int priority)
	{
		_priority = priority;
	}

	public void setSourceApp(String source)
	{
		_sourceApp = source;
	}

	public void setTimestamp(long timestamp)
	{
		_timestamp = timestamp;
	}

	public void setExpiration(long ttl)
	{
		_expiration = ttl;
	}

	public void setType(MessageType type)
	{
		_type = type;
	}

	public void readExternal(ObjectInput oin) throws IOException, ClassNotFoundException
	{
		_content = oin.readUTF();
		_correlationId = oin.readUTF();
		_destination = oin.readUTF();
		_id = oin.readUTF();
		_priority = oin.readInt();
		_sourceApp = oin.readUTF();
		_timestamp = oin.readLong();
		_expiration = oin.readLong();
		_type = MessageType.lookup(oin.readInt());
	}

	public void writeExternal(ObjectOutput oout) throws IOException
	{
		oout.writeUTF(_content);
		oout.writeUTF(_correlationId);
		oout.writeUTF(_destination);
		oout.writeUTF(_id);
		oout.writeInt(_priority);
		oout.writeUTF(_sourceApp);
		oout.writeLong(_timestamp);
		oout.writeLong(_expiration);
		oout.writeInt(getType().getValue());
	}

	@Override
	public String toString()
	{
		StringBuilder buf = new StringBuilder(100);
		buf.append(getContent());
		buf.append(SEPARATOR);
		buf.append(getCorrelationId());
		buf.append(SEPARATOR);
		buf.append(getDestination());
		buf.append(SEPARATOR);
		buf.append(getMessageId());
		buf.append(SEPARATOR);
		buf.append(getPriority());
		buf.append(SEPARATOR);
		buf.append(getSourceApp());
		buf.append(SEPARATOR);
		buf.append(getTimestamp());
		buf.append(SEPARATOR);
		buf.append(getExpiration());
		buf.append(SEPARATOR);
		buf.append(getType().getValue());

		return buf.toString();
	}
	
	public void setFromRemotePeer(boolean isFromRemotePeer)
	{
		_isFromRemotePeer = isFromRemotePeer;
	}

	public boolean isFromRemotePeer()
	{
		return _isFromRemotePeer;
	}
}
