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
	private boolean _preferLocalConsumer;
	private long _reserve;
	private int _deliveryCount;
	private Message _message;
	private static final String SEPARATOR = "<#>";

	private BDBMessage()
	{
	}

	public BDBMessage(Message msg, long sequence, int deliveryCount, boolean preferLocalConsumer)
	{
		_deliveryCount = deliveryCount;
		_preferLocalConsumer = preferLocalConsumer;
		_message = msg;
		_sequence = sequence;
		_reserve = 0L;
	}
	
	
	public boolean getPreferLocalConsumer()
	{
		return _preferLocalConsumer;
	}

	public void setPreferLocalConsumer(boolean localConsumer)
	{
		_preferLocalConsumer = localConsumer;
	}

	public long getSequence()
	{
		return _sequence;
	}
	
	public long getReserve()
	{
		return _reserve;
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
	
	public void setReserve(long reserve)
	{
		_reserve = reserve;
	}

	public void readExternal(ObjectInput oin) throws IOException, ClassNotFoundException
	{
		_sequence = oin.readLong();
		_preferLocalConsumer = oin.readBoolean();
		_deliveryCount = oin.readInt();
		_reserve = oin.readLong();
		Message m = new Message();
		m.readExternal(oin);
		_message = m;
	}

	public void writeExternal(ObjectOutput oout) throws IOException
	{
		oout.writeLong(_sequence);
		oout.writeBoolean(_preferLocalConsumer);
		oout.writeInt(_deliveryCount);
		oout.writeLong(_reserve);
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
		buf.append(_preferLocalConsumer);
		buf.append(SEPARATOR);
		buf.append(_reserve);		

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
