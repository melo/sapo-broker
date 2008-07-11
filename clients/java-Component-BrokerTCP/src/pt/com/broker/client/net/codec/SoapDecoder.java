package pt.com.broker.client.net.codec;

import org.apache.mina.common.IoBuffer;
import org.caudexorigo.io.UnsynchByteArrayInputStream;

import pt.com.broker.client.xml.SoapEnvelope;
import pt.com.broker.client.xml.SoapSerializer;

public class SoapDecoder extends SimpleFramingDecoder
{

	@Override
	public Object processBody(byte[] packet)
	{
		UnsynchByteArrayInputStream bin = new UnsynchByteArrayInputStream(packet);
		SoapEnvelope msg = SoapSerializer.FromXml(bin);
		return msg;
	}

	public Object processBody(IoBuffer iob)
	{

		SoapEnvelope msg = SoapSerializer.FromXml(iob.asInputStream());
		return msg;
	}
}
