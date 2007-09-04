package pt.com.gcs.net.codec;

import org.apache.mina.filter.codec.ProtocolCodecFactory;
import org.apache.mina.filter.codec.ProtocolDecoder;
import org.apache.mina.filter.codec.ProtocolEncoder;


/**
 * The network protocol is as simple as could be:
 * 
 * <pre>
 *  ----------- 
 *  | Length  | -&gt; integer in network order: message:length
 *  -----------
 *  | Payload | -&gt; message payload
 *  -----------
 * </pre>
 * 
 * This applies to both input and ouput messages.
 */
public class GcsCodec implements ProtocolCodecFactory
{

	public static final int HEADER_LENGTH = 8;
	
	public static final int DEFAULT_MAX_MESSAGE_SIZE = 256 * 1024;
	
	private GcsEncoder encoder;

	private GcsDecoder decoder;

	public GcsCodec()
	{
		encoder = new GcsEncoder();
		decoder = new GcsDecoder(DEFAULT_MAX_MESSAGE_SIZE);
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
