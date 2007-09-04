package pt.com.broker.net.codec;

import org.caudexorigo.io.UnsynchByteArrayInputStream;

import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapSerializer;
import pt.com.gcs.net.codec.SimpleFramingDecoder;

public class SoapDecoder extends SimpleFramingDecoder
{
	public SoapDecoder(int max_message_size)
	{
		super(max_message_size);
	}

	@Override
	public Object processBody(byte[] packet)
	{
		UnsynchByteArrayInputStream bin = new UnsynchByteArrayInputStream(packet);
		SoapEnvelope msg = SoapSerializer.FromXml(bin);
		return msg;
	}

}
