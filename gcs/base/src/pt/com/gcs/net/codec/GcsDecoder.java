package pt.com.gcs.net.codec;

import org.caudexorigo.io.UnsynchByteArrayInputStream;

import pt.com.gcs.io.SerializerHelper;

public class GcsDecoder extends SimpleFramingDecoder
{
	public GcsDecoder(int max_message_size)
	{
		super(max_message_size);
	}

	@Override
	public Object processBody(byte[] packet)
	{
		UnsynchByteArrayInputStream bin = new UnsynchByteArrayInputStream(packet);
		Object msg = SerializerHelper.fromStream(bin);
		return msg;
	}

}
