package pt.com.xml;

import java.io.InputStream;
import java.io.OutputStream;

import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;


public class SoapSerializer
{

	private static final SoapSerializer instance = new SoapSerializer();

	private IBindingFactory bfact;

	private SoapSerializer()
	{

		try
		{
			bfact = BindingDirectory.getFactory(pt.com.xml.SoapEnvelope.class);
		}
		catch (JiBXException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public static void ToXml(SoapEnvelope soapEnv, OutputStream out)
	{
		try
		{
			IMarshallingContext mctx = instance.bfact.createMarshallingContext();

			mctx.marshalDocument(soapEnv, "UTF-8", null, out);
		}
		catch (JiBXException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public static SoapEnvelope FromXml(InputStream in)
	{
		try
		{
			IUnmarshallingContext uctx = instance.bfact.createUnmarshallingContext();

			Object o = uctx.unmarshalDocument(in, "UTF-8");
			if (o instanceof SoapEnvelope)
			{
				return (SoapEnvelope) o;

			}
			else
				return new SoapEnvelope();

		}
		catch (JiBXException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
