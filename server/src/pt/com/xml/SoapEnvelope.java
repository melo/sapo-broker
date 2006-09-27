package pt.com.xml;

public class SoapEnvelope
{
	public SoapBody body;
	
	public SoapHeader header;
	
	public SoapEnvelope()
	{
		body = new SoapBody();
		header = new SoapHeader();
	}
}
