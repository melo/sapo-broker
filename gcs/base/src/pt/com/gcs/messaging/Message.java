package pt.com.gcs.messaging;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.caudexorigo.cryto.MD5;
import org.caudexorigo.text.StringUtils;

public class Message implements Externalizable
{
	private static final AtomicLong SEQ = new AtomicLong(0L);

	private static final long serialVersionUID = -3656321513130930115L;

	public static final int DEFAULT_PRIORITY = 4;

	// private AckMode _ackm = AckMode.AUTO;

	private String _content = "";

	private String _correlationId = "";

	private String _destination;

	private String _id;

	private int _priority = DEFAULT_PRIORITY;

	private String _sourceApp = "Undefined Source";

	private long _timestamp = System.currentTimeMillis();

	private long _ttl = 0; // for ever

	private MessageType _type = MessageType.UNDEF;

	private static final String SEPARATOR = "<#>";

	private static final String BASE_MESSAGE_ID;

	private static final Pattern PSEPARATOR = Pattern.compile(SEPARATOR);

	static
	{
		BASE_MESSAGE_ID = MD5.getHashString(UUID.randomUUID().toString());
	}

	public Message()
	{
		_id = BASE_MESSAGE_ID + "#" + SEQ.incrementAndGet();
	}

	private Message(boolean dummy)
	{

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
		this(destination, content);
		checkArg(id);
		_id = id;
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

	public long getTtl()
	{
		return _ttl;
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

	public void setTtl(long ttl)
	{
		_ttl = ttl;
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
		 _ttl = oin.readLong();
		 _type = MessageType.lookup(oin.readInt());

//		String[] smsg = PSEPARATOR.split(oin.readUTF());
//
//		_content = smsg[0];
//		_correlationId = smsg[1];
//		_destination = smsg[2];
//		_id = smsg[3];
//		_priority = Integer.parseInt(smsg[4]);
//		_sourceApp = smsg[5];
//		_timestamp = Long.parseLong(smsg[6]);
//		_ttl = Long.parseLong(smsg[7]);
//		_type = MessageType.lookup(Integer.parseInt(smsg[8]));
	}

	public void writeExternal(ObjectOutput oout) throws IOException
	{
		 oout.writeUTF(getContent());
		 oout.writeUTF(getCorrelationId());
		 oout.writeUTF(getDestination());
		 oout.writeUTF(getMessageId());
		 oout.writeInt(getPriority());
		 oout.writeUTF(getSourceApp());
		 oout.writeLong(getTimestamp());
		 oout.writeLong(getTtl());
		 oout.writeInt(getType().getValue());

		//oout.writeUTF(this.toString());
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
		buf.append(getTtl());
		buf.append(SEPARATOR);
		buf.append(getType().getValue());

		return buf.toString();
	}

	public static Message fromString(String instr)
	{
		String[] smsg = PSEPARATOR.split(instr);

		Message msg = new Message(false);
		msg.setContent(smsg[0]);
		msg.setCorrelationId(smsg[1]);
		msg.setDestination(smsg[2]);
		msg.setMessageId(smsg[3]);
		msg.setPriority(Integer.parseInt(smsg[4]));
		msg.setSourceApp(smsg[5]);
		msg.setTimestamp(Long.parseLong(smsg[6]));
		msg.setTtl(Long.parseLong(smsg[7]));
		msg.setType(MessageType.lookup(Integer.parseInt(smsg[8])));

		return msg;
	}
}
