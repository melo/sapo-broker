package pt.com.broker.xml;

import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pt.com.broker.Start;

public class JibxActors
{
	private static final Logger log = LoggerFactory.getLogger(JibxActors.class);

	private static final JibxActors instance = new JibxActors();

	private IBindingFactory bfact;

	private JibxActors()
	{
		try
		{
			bfact = BindingDirectory.getFactory(pt.com.broker.xml.SoapEnvelope.class);
		}
		catch (JiBXException e)
		{
			log.error(e.getMessage(), e);
			Start.shutdown();
		}
	}

	private static final ThreadLocal<IMarshallingContext> mctx = new ThreadLocal<IMarshallingContext>()
	{
		@Override
		protected IMarshallingContext initialValue()
		{
			IMarshallingContext _mctx;
			try
			{
				_mctx = instance.bfact.createMarshallingContext();
				return _mctx;
			}
			catch (JiBXException e)
			{
				throw new RuntimeException(e);
			}

		}
	};

	private static final ThreadLocal<IUnmarshallingContext> uctx = new ThreadLocal<IUnmarshallingContext>()
	{
		@Override
		protected IUnmarshallingContext initialValue()
		{
			IUnmarshallingContext _uctx;
			try
			{
				_uctx = instance.bfact.createUnmarshallingContext();
				return _uctx;
			}
			catch (JiBXException e)
			{
				throw new RuntimeException(e);
			}

		}
	};

	public static IMarshallingContext getMarshallingContext()
	{
		return mctx.get();
	}

	public static IUnmarshallingContext getUnmarshallingContext()
	{
		return uctx.get();
	}
	
	public static void reload()
	{
		mctx.remove();
		uctx.remove();
	}
}
