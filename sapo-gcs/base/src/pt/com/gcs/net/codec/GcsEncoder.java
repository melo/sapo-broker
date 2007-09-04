package pt.com.gcs.net.codec;

import pt.com.gcs.io.SerializerHelper;
import pt.com.gcs.messaging.Message;
import org.caudexorigo.io.UnsynchByteArrayOutputStream;

public class GcsEncoder extends SimpleFramingEncoder
{
	@Override
	public byte[] processBody(Object message)
	{
		UnsynchByteArrayOutputStream holder = new UnsynchByteArrayOutputStream();
		SerializerHelper.toStream((Message) message, holder);
		return holder.toByteArray();
	}
}