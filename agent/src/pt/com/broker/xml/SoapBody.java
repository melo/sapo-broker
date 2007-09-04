package pt.com.broker.xml;

import pt.com.broker.messaging.Acknowledge;
import pt.com.broker.messaging.Enqueue;
import pt.com.broker.messaging.Notification;
import pt.com.broker.messaging.Notify;
import pt.com.broker.messaging.Publish;
import pt.com.broker.messaging.Unsubscribe;

public class SoapBody
{
	public SoapFault fault;
	
	public Notify notify;

	public Acknowledge acknowledge;

	public Unsubscribe unsubscribe;
	
	public Enqueue enqueue;

	public Notification notification;

	public Publish publish;
}
