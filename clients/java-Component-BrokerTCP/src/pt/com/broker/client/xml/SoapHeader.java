package pt.com.broker.client.xml;

public class SoapHeader
{

	// wsa* -> ws-addressing fields;
	
	public String wsaMessageID;

	public String wsaRelatesTo;

	public String wsaTo;

	public String wsaAction;

	public EndPointReference wsaFrom;

	public EndPointReference wsaReplyTo;

	public EndPointReference wsaFaultTo;

}
