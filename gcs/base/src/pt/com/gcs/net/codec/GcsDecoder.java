package pt.com.gcs.net.codec;

import org.apache.mina.core.buffer.IoBuffer;
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

	@Override
	public Object processBody(IoBuffer in)
	{
		Object msg = SerializerHelper.fromStream(in.asInputStream());
		return msg;
	}

}
