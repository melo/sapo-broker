package pt.com.codec;

import org.apache.mina.common.ByteBuffer;
import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

public class SoapEncoder extends ProtocolEncoderAdapter
{
	public SoapEncoder()
	{
	}

	public void encode(IoSession session, Object message, ProtocolEncoderOutput pout) throws Exception
	{
		if (!(message instanceof byte[]))
		{
			return;
		}

		byte[] abuf = (byte[]) message;
		int msg_len = abuf.length;

		ByteBuffer wbuf = ByteBuffer.allocate(msg_len + 4, false);

		wbuf.putInt(msg_len);
		wbuf.put(abuf);
		wbuf.flip();

		pout.write(wbuf);
		abuf = null;
	}

	public void dispose() throws Exception
	{
	}
}