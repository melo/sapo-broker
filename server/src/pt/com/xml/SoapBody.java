package pt.com.xml;

import pt.com.broker.Acknowledge;
import pt.com.broker.Denqueue;
import pt.com.broker.DenqueueResponse;
import pt.com.broker.Enqueue;
import pt.com.broker.Notification;
import pt.com.broker.Notify;
import pt.com.broker.Publish;

public class SoapBody
{

	public Notify notify;

	public Acknowledge acknowledge;

	public Denqueue denqueue;

	public DenqueueResponse denqueueResponse;

	public Enqueue enqueue;

	public Notification notification;

	public Publish publish;

}
