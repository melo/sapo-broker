package pt.com.broker.net.codec;

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

}