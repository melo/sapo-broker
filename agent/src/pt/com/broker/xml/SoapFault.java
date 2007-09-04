package pt.com.broker.xml;


public class SoapFault
{
	public FaultCode faultCode;

	public FaultReason faultReason;

	public String detail = "";
	
	public SoapFault()
	{
		detail = "";
		faultCode = new FaultCode();
		faultReason = new FaultReason();
	}
}
