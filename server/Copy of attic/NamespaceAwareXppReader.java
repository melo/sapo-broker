package pt.com.xml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

import org.xmlpull.mxp1.MXParser;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.thoughtworks.xstream.converters.ErrorWriter;
import com.thoughtworks.xstream.io.StreamException;
import com.thoughtworks.xstream.io.xml.AbstractPullReader;

public class NamespaceAwareXppReader extends AbstractPullReader
{

	private final XmlPullParser parser;

	private final BufferedReader reader;

	private static final XmlPullParserFactory factory;

	static
	{
		factory = SafeFactoryBuilder.createParserFactory();
		factory.setNamespaceAware(true);
	}

	public NamespaceAwareXppReader(Reader reader)
	{
		try
		{
			parser = createParser();
			this.reader = new BufferedReader(reader);
			parser.setInput(this.reader);
			moveDown();
		}
		catch (XmlPullParserException e)
		{
			throw new StreamException(e);
		}
	}

	/**
	 * To use another implementation of org.xmlpull.v1.XmlPullParser, override this method.
	 */
	protected XmlPullParser createParser()
	{
		try
		{
			return factory.newPullParser();
		}
		catch (Exception e)
		{
			return new MXParser();
		}
	}

	protected int pullNextEvent()
	{
		try
		{
			switch (parser.next())
			{
			case XmlPullParser.START_DOCUMENT:
			case XmlPullParser.START_TAG:
				return START_NODE;
			case XmlPullParser.END_DOCUMENT:
			case XmlPullParser.END_TAG:
				return END_NODE;
			case XmlPullParser.TEXT:
				return TEXT;
			case XmlPullParser.COMMENT:
				return COMMENT;
			default:
				return OTHER;
			}
		}
		catch (XmlPullParserException e)
		{
			throw new StreamException(e);
		}
		catch (IOException e)
		{
			throw new StreamException(e);
		}
	}

	protected String pullElementName()
	{
		return parser.getName();
	}

	protected String pullText()
	{
		return parser.getText();
	}

	public String getAttribute(String name)
	{
		return parser.getAttributeValue(null, name);
	}

	public String getAttribute(int index)
	{
		return parser.getAttributeValue(index);
	}

	public int getAttributeCount()
	{
		return parser.getAttributeCount();
	}

	public String getAttributeName(int index)
	{
		return parser.getAttributeName(index);
	}

	public void appendErrors(ErrorWriter errorWriter)
	{
		errorWriter.add("line number", String.valueOf(parser.getLineNumber()));
	}

	public void close()
	{
		try
		{
			reader.close();
		}
		catch (IOException e)
		{
			throw new StreamException(e);
		}
	}

}

final class SafeFactoryBuilder
{
	protected static XmlPullParserFactory createParserFactory()
	{
		try
		{
			return XmlPullParserFactory.newInstance();
		}
		catch (XmlPullParserException e)
		{
			throw new RuntimeException(e);
		}

	}
}