package pt.com.broker.client.xml;

import pt.com.broker.client.messaging.Accepted;
import pt.com.broker.client.messaging.Acknowledge;
import pt.com.broker.client.messaging.CheckStatus;
import pt.com.broker.client.messaging.Enqueue;
import pt.com.broker.client.messaging.Notification;
import pt.com.broker.client.messaging.Notify;
import pt.com.broker.client.messaging.Poll;
import pt.com.broker.client.messaging.Publish;
import pt.com.broker.client.messaging.Status;
import pt.com.broker.client.messaging.Unsubscribe;

public class SoapBody
{
	public SoapFault fault;
	
	public Notify notify;

	public Acknowledge acknowledge;

	public Unsubscribe unsubscribe;
	
	public Enqueue enqueue;
	
	public Poll poll;

	public Notification notification;

	public Publish publish;
	
	public CheckStatus checkStatus;
	
	public Status status;
	
	public Accepted accepted;
	
}
