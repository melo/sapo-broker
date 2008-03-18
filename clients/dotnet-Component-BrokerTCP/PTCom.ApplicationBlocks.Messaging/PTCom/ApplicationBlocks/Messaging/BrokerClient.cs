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
using System.Diagnostics;

namespace PTCom.ApplicationBlocks.Messaging
{
    public enum ExceptionType
    {
        Fatal,
        Recovered
    }

    public delegate bool ExceptionHandler(ExceptionType type, Exception exception, BrokerClient broker);

    public class BrokerClient : IDisposable
    {
        public static readonly Encoding ENCODING = new UTF8Encoding(false);

        private string _host;
        private int _portNumber;
        private SocketClient _skClient;
        private string _appName;
        private bool _isDisposed;
        private Notify _notify;

        public Notify Notify
        {
            get { return _notify; }
        }

        private ExceptionHandler _exceptionRaisedHandler;
        public event ExceptionHandler ExceptionRaised
        {
            add
            {
                _exceptionRaisedHandler += value;
            }
            remove 
            {
                _exceptionRaisedHandler -= value;
            }
        }

        public BrokerClient(string host, int portNumber, string appName)
        {
            _host = host;
            _portNumber = portNumber;
            _appName = appName;
            _isDisposed = false;

            //CreateSocket();
            _skClient = new SocketClient(_host, _portNumber, this);
        }

        public void Acknowledge(BrokerMessage brkmsg)
        {
            if ((brkmsg != null) && (!IsBlank(brkmsg.MessageId)))
            {
                Acknowledge ack = new Acknowledge();
                ack.MessageId = brkmsg.MessageId;

                SoapEnvelope soap = BuildSoapEnvelope("http://services.sapo.pt/broker/acknowledge");
                soap.Body.Acknowledge = ack;
                _skClient.SendMessageAsync(soap, false);
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
                _skClient.SendMessageAsync(soap, false);
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
                _skClient.SendMessageAsync(soap, false);
            }
            else
            {
                throw new ArgumentException("Mal-formed PublishRequest object");
            }
        }

        public bool HasConsumers()
        {
            return _skClient.HasListeners();
        }

        public IEnumerable<IListener> GetConsumers()
        {
            if (_skClient != null)
            {
                foreach (BrokerHandler handler in _skClient.GetHandlers())
                {
                    IListener listener = handler.Target as IListener;
                    if (listener != null)
                    {
                        yield return listener;
                    }
                }
            }
        }

        public void RemoveAsyncConsumer(IListener listener)
        {
            _skClient.Listeners -= new BrokerHandler(listener.OnMessage);
        }

        public void AddAsyncConsumer(Notify notify, IListener listener)
        {
            if ((notify != null) && (!IsBlank(notify.DestinationName)))
            {
                if (!_skClient.HasListeners())
                {
                    InitiateSocketToReceive(notify);
                }
                _skClient.Listeners += new BrokerHandler(listener.OnMessage);
            }
            else
            {
                throw new ArgumentException("Mal-formed NotificationRequest object");
            }
        }

        private void InitiateSocketToReceive(Notify notify)
        {
            string action = "";
            if (notify.DestinationType == DestinationType.QUEUE)
            {
                action = "http://services.sapo.pt/broker/listen";
            }
            else if ((notify.DestinationType == DestinationType.TOPIC) || 
                (notify.DestinationType == DestinationType.TOPIC_AS_QUEUE))
            {
                action = "http://services.sapo.pt/broker/subscribe";
            }
            else
            {
                throw new ArgumentException("Mal-formed NotificationRequest object");
            }

            //CreateSocket();

            SoapEnvelope soap = BuildSoapEnvelope(action);
            soap.Body.Notify = notify;
            _notify = notify;
            _skClient.SendMessageAsync(soap, true);
            //_skClient.SetListener(listener);
        }

        //private void CreateSocket()
        //{
        //    _skClient = new SocketClient(_host, _portNumber, this);
        //}

        private void Shutdown()
        {
            if (_skClient.IsConnected())
            {
                _skClient.Disconnect();
                //_skClient = null;
            }
            _isDisposed = true;
        }

        internal void ExceptionCaught(Exception exception)
        {
            try
            {
                Dispose();

                lock (this)
                {
                    Trace.TraceError(string.Format("BrokerClient exception: {0}{1}{2}",
                        exception.Message, Environment.NewLine, exception.StackTrace));

                    if (_exceptionRaisedHandler != null)
                    {
                        bool handled = false;
                        foreach (ExceptionHandler handler in _exceptionRaisedHandler.GetInvocationList())
                        {
                            if (handler(ExceptionType.Fatal, exception, this))
                            {
                                handled = true;
                            }
                        }
                        if (!handled)
                        {
                            throw exception;
                        }
                        _exceptionRaisedHandler = null;
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Error in BrokerClient.ExceptionCaught: {0}{1}{2}",
                    ex.Message, Environment.NewLine, ex.StackTrace);
            }
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

        #region IDisposable Members

        public void Dispose()
        {
            if (!_isDisposed)
            {
                Shutdown();
            }
        }

        #endregion
    }
}
