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
	private static final String BUFFER = SoapDecoder.class.getName() + ".Buffer";

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
				return false; // We didn't receive enough bytes to decode the message length. Cumulate remainder to decode later.
			}

			ReadBuffer readBuffer = (ReadBuffer) session.getAttribute(BUFFER);

			if (readBuffer == null)
			{
				readBuffer = new ReadBuffer();
				session.setAttribute(BUFFER, readBuffer);
			}

			// We can decode the message length, so find what it is and allocate a holder for the message body.
			if (readBuffer.lenght == 0)
			{
				int len = in.getInt();
				if (len > MQ.MAX_MESSAGE_SIZE)
				{
					throw new IllegalArgumentException("Illegal message size!! The maximum allowed message size is " + MQ.MAX_MESSAGE_SIZE  + " bytes.");
				}
				readBuffer.lenght = len;
				readBuffer.allocate();
			}

			//Keep dumping bytes into our holder until we reach the end of the message body
			if (readBuffer.readLenght < readBuffer.lenght)
			{
				int diff = readBuffer.lenght - readBuffer.readLenght;
				int readSize = Math.min(in.remaining(), diff);
				if (readSize > 0)
				{
					byte[] read_packet = new byte[readSize];
					in.get(read_packet);
					readBuffer.readLenght += readSize;
					readBuffer.packetbuf.write(read_packet);
				}
			}

			//We have the all message body,  unmarshal the gathered bytes and forward the Soap message. 
			if ((readBuffer.readLenght > 0) && (readBuffer.readLenght == readBuffer.lenght))
			{
				byte[] packet = readBuffer.packetbuf.toByteArray();
				readBuffer.lenght = 0;
				readBuffer.readLenght = 0;

				ByteArrayInputStream bin = new ByteArrayInputStream(packet);
				SoapEnvelope msg = SoapSerializer.FromXml(bin);
				out.write(msg);
				packet = null;
				bin = null;
			}
		}
		catch (Throwable e)
		{
			throw new RuntimeException(e);
		}
		return true;
	}

	@Override
	public void dispose(IoSession session) throws Exception
	{
		ReadBuffer buf = (ReadBuffer) session.getAttribute(BUFFER);
		if (buf != null)
		{
			buf = null;
			session.removeAttribute(BUFFER);
		}
		super.dispose(session);
	}
}
