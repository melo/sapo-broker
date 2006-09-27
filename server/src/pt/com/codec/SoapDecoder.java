package pt.com.codec;

import java.io.ByteArrayInputStream;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.CumulativeProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolDecoderOutput;

import pt.com.broker.MQ;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapSerializer;

public class SoapDecoder extends CumulativeProtocolDecoder
{
	private static final String MESSAGE_SIZE = SoapDecoder.class.getName() + ".MessageSize";

	public SoapDecoder()
	{
	}

	@Override
	public void decode(IoSession session, ByteBuffer in, ProtocolDecoderOutput out) throws Exception
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
	protected boolean doDecode(IoSession session, ByteBuffer in, ProtocolDecoderOutput out) throws Exception
	{
		try
		{
			if (in.remaining() < 4)
			{
				// We didn't receive enough bytes to decode the
				// message length. Cumulate remainder to decode later.
				return false;
			}

			Integer MSize = (Integer) session.getAttribute(MESSAGE_SIZE);
			int msize = (MSize == null) ? 0 : MSize.intValue();

			// We can decode the message length, so find what it is and allocate
			// a holder for the message body.
			if (msize == 0)
			{
				msize = in.getInt();
				if (msize > MQ.MAX_MESSAGE_SIZE)
				{
					throw new IllegalArgumentException("Illegal message size!! The maximum allowed message size is " + MQ.MAX_MESSAGE_SIZE + " bytes.");
				}
				else if (msize <= 0)
				{
					throw new IllegalArgumentException("Illegal message size!! The message lenght must be a positive value.");
				}

				session.setAttribute(MESSAGE_SIZE, msize);
			}

			if (in.remaining() < msize)
			{
				// We didn't receive enough bytes to decode the message body.
				// Cumulate remainder to decode later.
				return false;
			}

			// We have the all message body, unmarshal the gathered bytes and
			// forward the Soap message.

			byte[] packet = new byte[msize];
			in.get(packet);

			ByteArrayInputStream bin = new ByteArrayInputStream(packet);
			SoapEnvelope msg = SoapSerializer.FromXml(bin);
			out.write(msg);
			packet = null;
			bin = null;
			session.setAttribute(MESSAGE_SIZE, 0);
			return true;

		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
	}

	@Override
	public void dispose(IoSession session) throws Exception
	{
		session.removeAttribute(MESSAGE_SIZE);
		super.dispose(session);
	}
}
