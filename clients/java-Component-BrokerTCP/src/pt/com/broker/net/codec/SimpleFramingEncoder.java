package pt.com.broker.net.codec;

import org.apache.mina.common.IoSession;
import org.apache.mina.filter.codec.ProtocolEncoderAdapter;
import org.apache.mina.filter.codec.ProtocolEncoderOutput;

public abstract class SimpleFramingEncoder extends ProtocolEncoderAdapter
{
	public void encode(IoSession session, Object message, ProtocolEncoderOutput pout) throws Exception
	{
		processBody(message, pout);
	}

	public abstract byte[] processBody(Object message);

	public abstract void processBody(Object message, ProtocolEncoderOutput pout);

}
