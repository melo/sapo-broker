package pt.com.manta.bio;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.manta.Env;

public class MantaBioServer
{
	protected static final Logger log = LoggerFactory.getLogger(MantaBioServer.class);

	protected static final ConcurrentMap<String, BioBroker> _brokers = new ConcurrentHashMap<String, BioBroker>();

	/* max # worker threads */
	protected static final int workers = 5;

	private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(workers, workers, 0L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
	
	protected static final int MAX_MESSAGE_SIZE = 256 * 1024;

	/* timeout on client connections */
	protected static final int timeout = 0;

	public static void main(String[] a) throws Exception
	{
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		executor.prestartAllCoreThreads();

		int port = Env.portFromSys("bus_port");

		ServerSocket ss = new ServerSocket(port);
		while (true)
		{
			Socket s = ss.accept();

			executor.execute(new Worker(s));
		}
	}
}

class Worker extends MantaBioServer implements Runnable
{

	/* Socket to client we're handling */
	private Socket _socket;
	
	private String _addr;

	private DataOutputStream out;

	private DataInputStream in;

	Worker(Socket s)
	{
		_socket = s;
		
		/*
		 * we will only block in read for this many milliseconds before we fail
		 * with java.io.InterruptedIOException, at which point we will abandon
		 * the connection.
		 */
		try
		{
			out = new DataOutputStream(_socket.getOutputStream());
			in = new DataInputStream(_socket.getInputStream());
			_socket.setSoTimeout(MantaBioServer.timeout);
			_socket.setTcpNoDelay(true);
			_addr =  _socket.getRemoteSocketAddress().toString();
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		_brokers.putIfAbsent(_addr, new BioBroker(_socket));
	}

	public void run()
	{
		while (true)
		{
			try
			{
				if (!_socket.isClosed())
				{
					handleClient();
				}
				else
					return;
			}
			catch (Exception e)
			{
				log.error(e.getMessage(), e);
			}
		}
	}

	void handleClose()
	{
		log.info("closing socket:" + _addr);
		try
		{
			BioBroker broker = MantaBioServer._brokers.remove(_addr.toString());
			broker.close();
			_socket.close();
		}
		catch (IOException e)
		{
			MantaBioServer.log.error(e.getMessage(), e);
		}
	}

	void handleClient() throws IOException
	{
		try
		{
			int len;
			byte[] buf;
			try
			{
				len = in.readInt();
				if (len>MAX_MESSAGE_SIZE)
				{
					in.skip(in.available());
					System.out.println("len>MAX_MESSAGE_SIZE:" + len);
					handleClose();
					return;
				}
				
				
				buf = new byte[len];
				in.readFully(buf);
			}
			catch (EOFException e)
			{
				handleClose();
				// log.error(e.getMessage(), e);
				return;
			}
			catch (SocketException e)
			{
				handleClose();
				// log.error(e.getMessage(), e);
				return;
			}

			// System.out.println("len:" + len);
			System.out.println(new String(buf));

//			ByteArrayInputStream bin = new ByteArrayInputStream(buf);
//			SoapEnvelope soap_msg = SoapSerializer.FromXml(bin);
//
//			BioBroker broker = _brokers.get(_addr);
//
//			if (soap_msg.body.notificationRequest != null)
//			{
//				NotificationRequest sb = soap_msg.body.notificationRequest;
//				if (sb.destinationType.equals("TOPIC"))
//				{
//					broker.subscribe(sb);
//				}
//				else if (sb.destinationType.equals("QUEUE"))
//				{
//					broker.listen(sb);
//				}
//				return;
//			}
//			else if (soap_msg.body.publishRequest != null)
//			{
//				broker.publishMessage(soap_msg.body.publishRequest);
//				return;
//
//			}
//			else if (soap_msg.body.enqueueRequest != null)
//			{
//				broker.enqueueMessage(soap_msg.body.enqueueRequest);
//				return;
//			}
//			else if (soap_msg.body.denqueueRequest != null)
//			{
//				BrokerMessage bkrmsg = broker.denqueueMessage(soap_msg.body.denqueueRequest).brokerMessage;
//
//				DenqueueResult dresult = new DenqueueResult();
//				dresult.brokerMessage = bkrmsg;
//				SoapEnvelope ret_env = new SoapEnvelope();
//				ret_env.body.denqueueResult = dresult;
//
//				ByteArrayOutputStream holder = new ByteArrayOutputStream(512);
//				SoapSerializer.ToXml(ret_env, holder);
//
//				out.write(holder.toByteArray());
//				out.flush();
//				return;
//			}
//			else if (soap_msg.body.acknowledgeRequest != null)
//			{
//				broker.acknowledge(soap_msg.body.acknowledgeRequest);
//				return;
//			}
//			else
//			{
//				throw new IllegalArgumentException("Not a valid soap_msg");
//			}

		}
		catch (Exception e)
		{
			log.error(e.getMessage(), e);
		}
	}
}