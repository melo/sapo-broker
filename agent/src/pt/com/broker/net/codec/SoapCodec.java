package pt.com.broker.net.codec;

import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;

import pt.com.broker.messaging.MQ;

/**
 * The network protocol is as simple as could be:
 * 
 * <pre>
 *  ----------- 
 *  | Length  | -&gt; integer in network order
 *  -----------
 *  | Payload | -&gt; Xml Soap Message
 *  -----------
 * </pre>
 * 
 * This applies to both input and ouput messages.
 */
public class SoapCodec implements ProtocolCodecFactory
{

	public static final int HEADER_LENGTH = 4;
	
	private SoapEncoder encoder;

	private SoapDecoder decoder;

	public SoapCodec()
	{
		encoder = new SoapEncoder();
		decoder = new SoapDecoder(MQ.MAX_MESSAGE_SIZE);
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
