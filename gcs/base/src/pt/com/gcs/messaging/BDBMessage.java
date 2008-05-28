package pt.com.gcs.messaging;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;

import org.caudexorigo.io.UnsynchByteArrayInputStream;

public class BDBMessage implements Externalizable
{
	private long _sequence;
	private boolean _localConsumersOnly;
	private int _deliveryCount;
	private Message _message;
	private static final String SEPARATOR = "<#>";

	private BDBMessage()
	{
	}

	public BDBMessage(Message msg, long sequence, int deliveryCount, boolean localConsumersOnly)
	{
		_deliveryCount = deliveryCount;
		_localConsumersOnly = localConsumersOnly;
		_message = msg;
		_sequence = sequence;
	}

	public long getSequence()
	{
		return _sequence;
	}

	public boolean isLocalConsumersOnly()
	{
		return _localConsumersOnly;
	}

	public int getDeliveryCount()
	{
		return _deliveryCount;
	}

	public Message getMessage()
	{
		return _message;
	}

	public void setDeliveryCount(int count)
	{
		_deliveryCount = count;
	}

	public void readExternal(ObjectInput oin) throws IOException, ClassNotFoundException
	{
		_sequence = oin.readLong();
		_localConsumersOnly = oin.readBoolean();
		_deliveryCount = oin.readInt();
		Message m = new Message();
		m.readExternal(oin);
		_message = m;
	}

	public void writeExternal(ObjectOutput oout) throws IOException
	{
		oout.writeLong(_sequence);
		oout.writeBoolean(_localConsumersOnly);
		oout.writeInt(_deliveryCount);
		_message.writeExternal(oout);
	}

	@Override
	public String toString()
	{
		StringBuilder buf = new StringBuilder(100);
		buf.append(_message.toString());
		buf.append(SEPARATOR);
		buf.append(_sequence);
		buf.append(SEPARATOR);
		buf.append(_localConsumersOnly);

		return buf.toString();
	}

	protected static BDBMessage fromByteArray(byte[] buf) throws IOException, ClassNotFoundException
	{
		BDBMessage bm = new BDBMessage();
		ObjectInputStream oIn;

		oIn = new ObjectInputStream(new UnsynchByteArrayInputStream(buf));
		bm.readExternal(oIn);
		return bm;

	}

}
