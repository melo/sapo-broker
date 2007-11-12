package pt.com.broker.net.codec;

import org.apache.mina.common.IoBuffer;
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

	public Object processBody(IoBuffer iob)
	{

		SoapEnvelope msg = SoapSerializer.FromXml(iob.asInputStream());
		return msg;
		// String message = slurp(iob.asInputStream());
		// System.out.println("message: " + message);
		// return new SoapEnvelope();
	}

	// public static String slurp(InputStream in)
	// {
	// StringBuilder out = new StringBuilder();
	// byte[] b = new byte[4096];
	// try
	// {
	// for (int n; (n = in.read(b)) != -1;)
	// {
	// out.append(new String(b, 0, n));
	// }
	// }
	// catch (IOException e)
	// {
	// throw new RuntimeException(e);
	// }
	// return out.toString();
	// }

}
