package pt.com.gcs.net.codec;

import org.apache.mina.common.IoBuffer;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;
import org.caudexorigo.io.UnsynchByteArrayOutputStream;

import pt.com.gcs.io.SerializerHelper;
import pt.com.gcs.messaging.Message;

public class GcsEncoder extends SimpleFramingEncoder
{
	@Override
	public byte[] processBody(Object message)
	{
		UnsynchByteArrayOutputStream holder = new UnsynchByteArrayOutputStream();
		SerializerHelper.toStream((Message) message, holder);
		return holder.toByteArray();
	}

	@Override
	public void processBody(Object message, ProtocolEncoderOutput pout)
	{
		IoBuffer wbuf = IoBuffer.allocate(2048, false);
		wbuf.setAutoExpand(true);
		wbuf.putInt(0);
		SerializerHelper.toStream((Message) message, wbuf.asOutputStream());
		int msize = wbuf.position() - 4;
		wbuf.putInt(0, msize);

		wbuf.flip();

		pout.write(wbuf);		
	}
}