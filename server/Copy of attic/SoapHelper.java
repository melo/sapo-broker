package pt.com.xml;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.wrapper.XmlPullParserWrapper;
import org.xmlpull.v1.wrapper.XmlPullWrapperFactory;
import org.xmlpull.v1.wrapper.XmlSerializerWrapper;

public class SoapHelper
{	
	private SoapHelper()
	{
	}
	
	public static void extractSoapBody(InputStream in, OutputStream out) throws Exception
	{
		XmlPullWrapperFactory wp = XmlPullWrapperFactory.newInstance();
		wp.setNamespaceAware(true);
		XmlSerializerWrapper ser = wp.newSerializerWrapper();
		XmlPullParserWrapper pw = wp.newPullParserWrapper();
		
		ser.setOutput(new OutputStreamWriter(out));
		pw.setInput(new InputStreamReader(in));

		int event = pw.getEventType();
		while (event != XmlPullParser.END_DOCUMENT)
		{
			event = pw.next();

			if (event == XmlPullParser.START_TAG && pw.getName().equals("Body"))
			{
				while (true)
				{
					event = pw.next();
					if (event == XmlPullParser.END_TAG && pw.getName().equals("Body"))
					{
						ser.flush();
						return;
					}
					else
					{
						ser.event(pw);
					}
				}
			}
		}
	}

}
