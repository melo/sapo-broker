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

public class SearchQueriesConsumer
{

	static Socket client;

	static DataOutputStream raw;

	static DataInputStream rawi;

	private static final String topicName = "/sapo/pesquisa/queries";

	static XmlPullParser xpp;

	public static void main(String[] args) throws Exception
	{
		System.setProperty("file.encoding", "UTF-8");

		client = new Socket("10.135.33.21", 2222);

		XmlPullParserFactory factory = XmlPullParserFactory.newInstance(System.getProperty(XmlPullParserFactory.PROPERTY_NAME), null);
		factory.setNamespaceAware(true);
		factory.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

		xpp = factory.newPullParser();

		raw = new DataOutputStream(client.getOutputStream());
		rawi = new DataInputStream(client.getInputStream());

		subscribePresences(raw);

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
				System.out.println(extractSearchInfo(msg.textPayload));
			}
			catch (XmlPullParserException e)
			{
				System.out.println("ERROR::" + e.getMessage());
				System.out.println(msg.textPayload);
				// System.exit(-1);
			}
		}
	}

	static void subscribePresences(DataOutputStream sout) throws Exception
	{
		System.out.println("topicName:" + topicName);
		byte[] subMessage = getMesssage(topicName);
		sout.writeInt(subMessage.length);
		sout.write(subMessage);
		sout.flush();
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

	static String extractSearchInfo(String xml) throws XmlPullParserException, IOException
	{
		xpp.setInput(new StringReader(xml));
		int eventType = xpp.getEventType();

		String keywords = "";

		do
		{
			if (eventType == XmlPullParser.START_TAG)
			{
				String name = xpp.getName();
				String uri = xpp.getNamespace();
				if (name.equals("keywords") && uri.equals("http://uri.sapo.pt/schemas/pesquisa/query.xsd"))
				{
					xpp.next();
					keywords = xpp.getText();
					break;
				}
			}
			eventType = xpp.next();
		}
		while (eventType != XmlPullParser.END_DOCUMENT);

		return keywords;
	}
}
