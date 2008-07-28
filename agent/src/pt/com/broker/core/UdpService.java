package pt.com.broker.core;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;

import org.caudexorigo.concurrent.CustomExecutors;
import org.caudexorigo.io.UnsynchByteArrayInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.messaging.BrokerProducer;
import pt.com.broker.messaging.MQ;
import pt.com.broker.xml.SoapEnvelope;
import pt.com.broker.xml.SoapSerializer;
import pt.com.gcs.conf.GcsInfo;

public class UdpService
{
	private static final Logger log = LoggerFactory.getLogger(UdpService.class);

	private static final ExecutorService exec = CustomExecutors.newThreadPool(4, "BrokerUdp");

	private static final BrokerProducer _brokerProducer = BrokerProducer.getInstance();

	public UdpService()
	{
		log.info("");
	}

	public void start()
	{
		DatagramSocket socket;
		try
		{
			InetAddress inet = InetAddress.getByName("0.0.0.0");
			int port = GcsInfo.getBrokerUdpPort();
			socket = new DatagramSocket(port, inet);
			socket.setReceiveBufferSize(4 * 1024 * 1024);
			
			log.info("Starting UdpService. Listen Port: {}. ReceiveBufferSize: {}", port, socket.getReceiveBufferSize());
		}
		catch (Exception error)
		{
			log.error("Error creating UDP Endpoint", error);
			return;
		}

		int mSize = (int) Math.pow(2, 16);

		DatagramPacket packet = new DatagramPacket(new byte[mSize], mSize);
		log.info("UdpService started and listening for packets.");

		while (true)
		{
			try
			{
				socket.receive(packet);
				byte[] receivedData = packet.getData();
				int len = packet.getLength();
				byte[] messageData = new byte[len];
				System.arraycopy(receivedData, 0, messageData, 0, len);
				exec.execute(new UdpPacketProcessor(messageData));
			}
			catch (Throwable error)
			{
				log.error(error.getMessage(), error);
			}
		}
	}

	final static class UdpPacketProcessor implements Runnable
	{
		final byte[] _messageData;

		UdpPacketProcessor(byte[] messageData)
		{
			_messageData = messageData;
		}

		@Override
		public void run()
		{
			try
			{
				SoapEnvelope soap = SoapSerializer.FromXml(new UnsynchByteArrayInputStream(_messageData));

				final String requestSource = MQ.requestSource(soap);

				if (soap.body.publish != null)
				{
					_brokerProducer.publishMessage(soap.body.publish, requestSource);
				}
				else if (soap.body.enqueue != null)
				{
					_brokerProducer.enqueueMessage(soap.body.enqueue, requestSource);
				}
			}
			catch (Throwable error)
			{
				log.error(error.getMessage(), error);
			}

		}
	}

}
