package pt.com.gcs.net.codec;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

public abstract class SimpleFramingEncoder extends ProtocolEncoderAdapter
{
	public void encode(IoSession session, Object message, ProtocolEncoderOutput pout) throws Exception
	{
		byte[] abuf = processBody(message);

		int msg_len = abuf.length;

		ByteBuffer wbuf = ByteBuffer.allocate(msg_len + GcsCodec.HEADER_LENGTH, false);

		wbuf.putInt(msg_len);
		wbuf.put(abuf);
		wbuf.flip();

		pout.write(wbuf);
		abuf = null;
	}

	public abstract byte[] processBody(Object message);

}
