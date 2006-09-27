package pt.com.test.sapo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.Socket;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import pt.com.broker.BrokerMessage;
import pt.com.broker.Notify;
import pt.com.xml.SoapEnvelope;
import pt.com.xml.SoapSerializer;

public class PresenceConsumer
{

	static Socket client;

	static DataOutputStream raw;

	static DataInputStream rawi;

	private static final String topicPrefix = "/sapo/messenger/presence/";

	private static final String[] presenceTopics = { "sapo", "netcabo", "telepac", "outros" };

	static XmlPullParser xpp;

	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");

		client = new Socket("transports.m3.bk.sapo.pt", 2222);

		XmlPullParserFactory factory = XmlPullParserFactory.newInstance(System.getProperty(XmlPullParserFactory.PROPERTY_NAME), null);
		factory.setNamespaceAware(true);
		factory.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

		xpp = factory.newPullParser();

		raw = new DataOutputStream(client.getOutputStream());
		rawi = new DataInputStream(client.getInputStream());

		subscribePresences(raw);
		raw.flush();

		System.out.println("waiting notification");
		while (true)
		{
			int len = rawi.readInt();
			// System.out.print(len + "\n");
			byte[] b = new byte[len];
			rawi.readFully(b);
			BrokerMessage msg = SoapSerializer.FromXml(new ByteArrayInputStream(b)).body.notification.brokerMessage;
			try
			{
				//System.out.println("[" + extractPresenceInfo(msg.textPayload) + "]");
				System.out.println(extractPresenceInfo(msg.textPayload));
			}
			catch (XmlPullParserException e)
			{
				System.out.println("ERROR::" + e.getMessage());
				System.out.println(msg.textPayload);
				// System.exit(-1);
			}

			// System.out.println(new String(b));
		}
	}

	static void subscribePresences(DataOutputStream sout) throws Exception
	{
		for (int i = 0; i < presenceTopics.length; i++)
		{
			String topicName = topicPrefix + presenceTopics[i];
			System.out.println("topicName:" + topicName);
			byte[] subMessage = getMesssage(topicName);
			sout.writeInt(subMessage.length);
			sout.write(subMessage);
		}

		// byte[] subMessage = getMesssage("/test/performance");
		// sout.writeInt(subMessage.length);
		// sout.write(subMessage);
	}

	static byte[] getMesssage(String topicName) throws UnsupportedEncodingException
	{
		SoapEnvelope soap = new SoapEnvelope();
		soap.body.notify = new Notify();
		soap.body.notify.destinationName = topicName;
		soap.body.notify.destinationType = "TOPIC";
		ByteArrayOutputStream bout = new ByteArrayOutputStream(2500);
		SoapSerializer.ToXml(soap, bout);
		return bout.toByteArray();
	}

	static String extractPresenceInfo(String xml) throws XmlPullParserException, IOException
	{
		xpp.setInput(new StringReader(xml));
		int eventType = xpp.getEventType();
		String jid = "";
		String show = "";
		boolean fJid = false;
		boolean fShow = false;

		do
		{
			if (eventType == XmlPullParser.START_TAG)
			{
				String name = xpp.getName();
				String uri = xpp.getNamespace();
				if (name.equals("jid") && uri.equals("http://uri.sapo.pt/schemas/messenger/presence.xsd"))
				{
					xpp.next();
					jid = xpp.getText();
					fJid = true;
				}
				if (name.equals("show") && uri.equals("http://uri.sapo.pt/schemas/messenger/presence.xsd"))
				{
					xpp.next();
					show = xpp.getText();
					fShow = true;
				}
			}
			eventType = xpp.next();
		}
		while ((!fJid && !fShow) || (eventType != XmlPullParser.END_DOCUMENT));

		String[] ret = jid.split("@");
		String domain = ret[1].split("/")[0];
		jid = ret[0];

		//return "jid:" + jid + ", domain:" + domain + ", status:" + show;
		if (show.equalsIgnoreCase("unavailable"))
		{
			return "";
		}
		return jid + "@" + domain;
	}

}
