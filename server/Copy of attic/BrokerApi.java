package pt.com.broker;

import java.rmi.RemoteException;

public interface BrokerApi
{
	public void acknowledge(Acknowledge ackReq) throws RemoteException;
	
	public abstract void enqueueMessage(Enqueue enqreq) throws RemoteException;

	public abstract DenqueueResultWrapper denqueueMessage(Denqueue denqreq) throws RemoteException;

	public abstract void publishMessage(Publish pubreq) throws RemoteException;

	public abstract void subscribe(Notify notfreq) throws RemoteException;
	
	public abstract void listen(Notify notfreq) throws RemoteException;

}