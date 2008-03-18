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
        private SocketClient _skClient;
        private string _appName;

        public BrokerClient(string host, int portNumber, string appName)
        {
            _appName = appName;
            _skClient = new SocketClient(host, portNumber, this);
        }

        public void Acknowledge(BrokerMessage brkmsg)
        {
            if ((brkmsg != null) && (!IsBlank(brkmsg.MessageId)))
            {
                Acknowledge ack = new Acknowledge();
                ack.MessageId = brkmsg.MessageId;

                SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/acknowledge");
                soap.Body.Acknowledge = ack;
                _skClient.SendMessage(soap, false);
            }
            else
            {
                throw new ArgumentException("Can't acknowledge invalid message.");
            }
        }

        public void EnqueueMessage(BrokerMessage brkmsg)
        {
            if ((brkmsg != null) && (!IsBlank(brkmsg.DestinationName)))
            {
                Enqueue enqreq = new Enqueue();
                enqreq.BrokerMessage = brkmsg;
                SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/enqueue");
                soap.Body.Enqueue = enqreq;
                _skClient.SendMessage(soap, false);
            }
            else
            {
                throw new ArgumentException("Mal-formed EnqueueRequest object");
            }
        }


        public void PublishMessage(BrokerMessage brkmsg)
        {
            if ((brkmsg != null) && (!IsBlank(brkmsg.DestinationName)))
            {
                Publish pubreq = new Publish();
                pubreq.BrokerMessage = brkmsg;
                SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/publish");
                soap.Body.Publish = pubreq;
                _skClient.SendMessage(soap, false);
            }
            else
            {
                throw new ArgumentException("Mal-formed PublishRequest object");
            }
        }

        public void SetAsyncConsumer(Notify notify, Listener listener)
        {
            if ((notify != null) && (!IsBlank(notify.DestinationName)))
            {
                string action = "";
                if (notify.DestinationType == DestinationType.QUEUE)
                {
                    action = "http://services.sapo.pt/broker/listen";
                }
                else if ((notify.DestinationType == DestinationType.TOPIC) || (notify.DestinationType == DestinationType.TOPIC_AS_QUEUE))
                {
                    action = "http://services.sapo.pt/broker/subscribe";
                }
                else
                {
                    throw new ArgumentException("Mal-formed NotificationRequest object");
                }
                SoapEnvelope soap = BuildSoapEnvelope(action);
                soap.Body.Notify = notify;
                _skClient.SendMessage(soap, true);
                _skClient.SetListener(listener);
            }
            else
            {
                throw new ArgumentException("Mal-formed NotificationRequest object");
            }
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
            return String.IsNullOrEmpty(str);
        }
    }
}
