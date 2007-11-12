package pt.com.broker.net.codec;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.caudexorigo.io.UnsynchByteArrayOutputStream;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapSerializer;
import pt.com.gcs.net.codec.SimpleFramingEncoder;

public class SoapEncoder extends SimpleFramingEncoder
{

	@Override
	public byte[] processBody(Object message)
	{
		UnsynchByteArrayOutputStream holder = new UnsynchByteArrayOutputStream();
		SoapSerializer.ToXml((SoapEnvelope) message, holder);
		return holder.toByteArray();
	}

	@Override
	public void processBody(Object message, ProtocolEncoderOutput pout)
	{
		IoBuffer wbuf = IoBuffer.allocate(2048, false);
		wbuf.setAutoExpand(true);
		wbuf.putInt(0);
		SoapSerializer.ToXml((SoapEnvelope) message, wbuf.asOutputStream());
		wbuf.putInt(0, wbuf.position() - 4);

		wbuf.flip();

		pout.write(wbuf);
	}

}