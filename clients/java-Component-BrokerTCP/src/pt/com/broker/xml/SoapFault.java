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

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("\n");
		sb.append("faultCode:" + faultCode.value);
		sb.append("\n");
		sb.append("faultReason:" + faultReason.text);
		sb.append("\n");
		sb.append("faultDetail:\n" + detail);
		sb.append("\n");
		return sb.toString();
	}
	
	
}
