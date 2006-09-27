package pt.com.codec;

import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;


/**
 * The network protocol is as simple as could be: 
 * 
 * ----------- 
 * | Length  | -> integer in network order
 * -----------
 * | Payload | -> Xml Soap Message
 * -----------
 * 
 * This applies to both input and ouput messages.
 */

public class SoapCodecFactory implements ProtocolCodecFactory
{

	private SoapEncoder encoder;

	private SoapDecoder decoder;

	public SoapCodecFactory()
	{

		encoder = new SoapEncoder();
		decoder = new SoapDecoder();
	}

	public ProtocolEncoder getEncoder()
	{
		return encoder;
	}

	public ProtocolDecoder getDecoder()
	{
		return decoder;
	}
}
