using System;
using System.Threading;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Collections.Generic;
using PTCom.ApplicationBlocks.Messaging.Util;
using PTCom.ApplicationBlocks.Messaging.Soap;
using PTCom.ApplicationBlocks.Messaging.Network;

namespace PTCom.ApplicationBlocks.Messaging
{
    public class BrokerClient
    {
        public static readonly Encoding ENCODING = new UTF8Encoding(false);

        private string _host;
        private int _portNumber;
        private SocketClient _skClient;
		private string _appName;

        public BrokerClient(string host, int portNumber, string appName)
        {
            _host = host;
            _portNumber = portNumber;
			_appName = appName;
            _skClient = new SocketClient(host, portNumber, this);

        }



        public void Acknowledge(Acknowledge ackReq)
        {
            if ((ackReq != null) && (ackReq.MessageId != null) && (!IsBlank(ackReq.MessageId)))
            {
				SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/acknowledge");
                soap.Body.Acknowledge = ackReq;
                _skClient.SendMessage(soap, false);
            }
            else
            {
                throw new ArgumentException("Mal-formed AcknowledgeRequest object");
            }
        }

        public void EnqueueMessage(Enqueue enqreq)
        {
            if ((enqreq != null) && (enqreq.BrokerMessage != null) && (!IsBlank(enqreq.BrokerMessage.DestinationName)))
            {
				SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/enqueue");
                soap.Body.Enqueue = enqreq;
                _skClient.SendMessage(soap, false);
            }
            else
            {
                throw new ArgumentException("Mal-formed EnqueueRequest object");
            }
        }


        public DenqueueResponse DenqueueMessage(Denqueue denqreq)
        {
            if ((denqreq != null) && (!IsBlank(denqreq.DestinationName)))
            {
				SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/denqueue");
                soap.Body.Denqueue = denqreq;
                _skClient.SendMessage(soap, true);

                return DenqueResponseHolder.GetMessageFromQueue(denqreq.DestinationName);
            }
            else
            {
                throw new ArgumentException("Mal-formed DenqueueRequest object");
            }
        }

        public void PublishMessage(Publish pubreq)
        {
            if ((pubreq != null) && (pubreq.BrokerMessage != null) && (!IsBlank(pubreq.BrokerMessage.DestinationName)))
            {
				SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/publish");
                soap.Body.Publish = pubreq;
                _skClient.SendMessage(soap, false);
            }
            else
            {
                throw new ArgumentException("Mal-formed PublishRequest object");
            }
        }

        public void Subscribe(Notify subreq, Listener listener)
        {
            if ((subreq != null) && (!IsBlank(subreq.DestinationName)) && (subreq.DestinationType == DestinationType.TOPIC))
            {
				SetAsyncConsumer(subreq, listener, "http://services.sapo.pt/broker/subscribe");
            }
            else
            {
                throw new ArgumentException("Mal-formed NotificationRequest object");
            }
        }


        public void Listen(Notify subreq, Listener listener)
        {
            if ((subreq != null) && (!IsBlank(subreq.DestinationName)) && (subreq.DestinationType == DestinationType.QUEUE))
            {
				SetAsyncConsumer(subreq, listener, "http://services.sapo.pt/broker/listen");
            }
            else
            {
                throw new ArgumentException("Mal-formed NotificationRequest object");
            }
        }


        private void SetAsyncConsumer(Notify notify, Listener listener, string action)
        {
			SoapEnvelope soap = BuildSoapEnvelope(action);
            soap.Body.Notify = notify;
            _skClient.SendMessage(soap, true);
            _skClient.SetListener(listener);
        }

        public void Shutdown()
        {
            _skClient.Disconnect();
        }

        internal void ExceptionCaught(Exception ex)
        {
            //TODO: not sure what do here, for now just rethrow the exception.
			throw ex;
        }

		private SoapEnvelope BuildSoapEnvelope(string action)
		{
			SoapEnvelope soap = new SoapEnvelope();			
			soap.Header.WsaAction = action;
			soap.Header.WsaFrom = new EndPointReference();
			soap.Header.WsaFrom.Address = _appName;
			return soap;
		}

        private static bool IsBlank(string str)
        {
            int strLen;
            if (str == null || (strLen = str.Trim().Length) == 0)
            {
                return true;
            }
            char[] str_arr = str.ToCharArray();
            for (int i = 0; i < str_arr.Length; i++)
            {
                if ((char.IsWhiteSpace(str_arr[i]) == false))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
