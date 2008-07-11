package pt.com.broker.client.net.codec;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

public abstract class SimpleFramingDecoder extends CumulativeProtocolDecoder
{

	public static final int HEADER_LENGTH = 4;

	public SimpleFramingDecoder()
	{
	}

	@Override
	public void decode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception
	{
		try
		{
			super.decode(session, in, out);
		}
		catch (Throwable e)
		{
			in.clear();
			(session.getHandler()).exceptionCaught(session, e);
		}
	}

	@Override
	protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception
	{
		try
		{
			// Remember the initial position.
			int start = in.position();

			if (in.remaining() < HEADER_LENGTH)
			{
				// We didn't receive enough bytes to decode the
				// message length. Cumulate remainder to decode later.
				in.position(start);
				return false;
			}

			// We can decode the message length
			int msize = in.getInt();

			if (in.remaining() < msize)
			{
				// We didn't receive enough bytes to decode the message body.
				// Cumulate remainder to decode later.
				in.position(start);
				return false;
			}

			// We have the all message body, unmarshal the gathered bytes and
			// forward the message.

			byte[] packet = new byte[msize];
			in.get(packet);
			out.write(processBody(packet));

			return true;

		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	public abstract Object processBody(byte[] packet);

	public abstract Object processBody(IoBuffer in);
}
