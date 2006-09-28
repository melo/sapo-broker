using System;
using System.Xml;
using System.Xml.Serialization;



namespace PTCom.ApplicationBlocks.Messaging.Soap
{
	[XmlRoot(ElementName = "Envelope", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
	public class SoapEnvelope
	{
		[XmlElement(ElementName = "Header", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public SoapHeader Header = new SoapHeader();

		[XmlElement(ElementName = "Body", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public SoapBody Body = new SoapBody();
	}

	public class SoapHeader
	{
		// wsa* -> ws-addressing fields;

		[XmlElement(ElementName = "MessageID", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public string WsaMessageID;

		[XmlElement(ElementName = "RelatesTo", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public string wsaRelatesTo;

		[XmlElement(ElementName = "To", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public string WsaTo;

		[XmlElement(ElementName = "Action", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public string WsaAction;

		[XmlElement(ElementName = "From", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public EndPointReference WsaFrom;

		[XmlElement(ElementName = "ReplyTo", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public EndPointReference WsaReplyTo;

		[XmlElement(ElementName = "FaultTo", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public EndPointReference WsaFaultTo;
	}

	public class SoapBody
	{
		[XmlElement(ElementName = "Notify", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public Notify Notify;

		[XmlElement(ElementName = "Acknowledge", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public Acknowledge Acknowledge;

		[XmlElement(ElementName = "BrokerMessage", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public BrokerMessage BrokerMessage;


		[XmlElement(ElementName = "Denqueue", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public Denqueue Denqueue;

		[XmlElement(ElementName = "DenqueueResponse", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public DenqueueResponse DenqueueResponse;


		[XmlElement(ElementName = "Enqueue", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public Enqueue Enqueue;

		[XmlElement(ElementName = "Notification", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public Notification Notification;


		[XmlElement(ElementName = "Publish", Namespace = "http://services.sapo.pt/broker", IsNullable = false)]
		public Publish Publish;

		[XmlElement(ElementName = "Fault", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public SoapFault Fault;
	}

	public class SoapFault
	{
		[XmlElement(ElementName = "Code", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public SoapFaultCode Code = new SoapFaultCode();

		[XmlElement(ElementName = "Reason", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public SoapFaultReason Reason = new SoapFaultReason();

		[XmlElement(ElementName = "Detail", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public string Detail = "";
	}

	public class EndPointReference
	{
		[XmlElement(ElementName = "Address", Namespace = "http://www.w3.org/2005/08/addressing", IsNullable = false)]
		public String Address;
	}

	public class SoapFaultCode
	{
		[XmlElement(ElementName = "Value", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public string Value = "";
	}

	public class SoapFaultReason
	{
		[XmlElement(ElementName = "Text", Namespace = "http://www.w3.org/2003/05/soap-envelope")]
		public string Text = "";
	}
}
