
using System;
using System.Net;
using System.Threading;
using System.Net.Sockets;
using PTCom.ApplicationBlocks.Messaging.Soap;
using PTCom.ApplicationBlocks.Messaging.Util;
using System.Runtime.CompilerServices;
using System.Diagnostics;


namespace PTCom.ApplicationBlocks.Messaging.Network
{
    public delegate void BrokerHandler(BrokerMessage message);

    /// <summary>
    /// Description of SocketClient.	
    /// </summary>
    public class SocketClient
    {
        private string _host;
        private int _port;
        private BrokerClient _bkClient;
        private Socket _clientSocket;
        private Boolean _isWaitingMessage;
        private IAsyncResult _pendingAsyncResult;
        private bool _isDisposed;

        private BrokerHandler _brokerHandlerDelegate;

        public event BrokerHandler Listeners
        {
            add
            {
                AddListener(value);
            }
            remove
            {
                RemoveListener(value);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void RemoveListener(BrokerHandler value)
        {
            _brokerHandlerDelegate -= value;
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void AddListener(BrokerHandler value)
        {
            _brokerHandlerDelegate += value;
        }

        public BrokerHandler[] GetHandlers()
        {
            if (_brokerHandlerDelegate != null)
            {
                return _brokerHandlerDelegate.GetInvocationList() as BrokerHandler[];
            }
            return new BrokerHandler[] { };
        }

        public SocketClient(string host, int port, BrokerClient bkClient)
        {
            _isDisposed = false;
            _isWaitingMessage = false;
            _host = host;
            _port = port;
            _bkClient = bkClient;

            // Create the socket instance
            _clientSocket = new Socket(
                AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _clientSocket.Connect(_host, _port);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void SendMessageAsync(SoapEnvelope soap, bool expectResponse)
        {
            try
            {
                byte[] dataArray = SerializationHelper.SerializeObject(soap);
                // get array length
                int reqLen = dataArray.Length;

                // convert length value to network order
                int reqLenH2N = IPAddress.HostToNetworkOrder(reqLen);

                // get length value into a byte array -- for use with
                // Socket.Send
                byte[] reqLenArray = BitConverter.GetBytes(reqLenH2N);

                // send the length value
                _clientSocket.Send(reqLenArray, 4, System.Net.Sockets.SocketFlags.None);

                // send the byte array
                _clientSocket.Send(dataArray, reqLen, System.Net.Sockets.SocketFlags.None);
                if (!_isWaitingMessage && expectResponse)
                {
                    _isWaitingMessage = expectResponse;
                    if (_clientSocket.Connected)
                    {
                        //Wait for data asynchronously 
                        WaitForHeader();
                    }
                }
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }

        private class BrokerPacket
        {
            public System.Net.Sockets.Socket thisSocket;
            public byte[] msgLengthHolder = new byte[4];
            public byte[] msgBodyHolder;
            public int totalHeaderBytesReceived = 0;
            public int totalBodyBytesReceived = 0;
        }
        
        [MethodImpl(MethodImplOptions.Synchronized)]
        private void WaitForHeader()
        {
            if (_isDisposed)
            {
                return;
            }
            try
            {
                BrokerPacket messagePacket = new BrokerPacket();
                messagePacket.thisSocket = _clientSocket;
                // Start listening to the data asynchronously
                _pendingAsyncResult = _clientSocket.BeginReceive(messagePacket.msgLengthHolder, 0, 
                    messagePacket.msgLengthHolder.Length, SocketFlags.None, 
                    new AsyncCallback(OnHeaderReceived), messagePacket);
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void AcumulateMessageHeader(BrokerPacket messagePacket, int messageLenght, int offset)
        {
            messagePacket.thisSocket = _clientSocket;
            // Start listening to the data asynchronously
            _pendingAsyncResult = _clientSocket.BeginReceive(messagePacket.msgLengthHolder, offset,
                messageLenght, SocketFlags.None, new AsyncCallback(OnHeaderReceived), messagePacket);
        }


        [MethodImpl(MethodImplOptions.Synchronized)]
        private void OnHeaderReceived(IAsyncResult asyn)
        {
            if (_isDisposed)
            {
                return;
            }
            try
            {
                BrokerPacket messagePacket = (BrokerPacket)asyn.AsyncState;
                int bytesReceived = messagePacket.thisSocket.EndReceive(asyn);
                messagePacket.totalHeaderBytesReceived += bytesReceived;
                if (messagePacket.totalHeaderBytesReceived < 4)
                {
                    // Incomplete message header,  cumulate remainder
                    AcumulateMessageHeader(
                        messagePacket, 4 - messagePacket.totalHeaderBytesReceived, 
                        messagePacket.totalHeaderBytesReceived);
                    return;
                }
                int dataSize = BitConverter.ToInt32(messagePacket.msgLengthHolder, 0);
                dataSize = IPAddress.NetworkToHostOrder(dataSize);

                WaitForMessageBody(messagePacket, dataSize);
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void WaitForMessageBody(BrokerPacket messagePacket, int messageLenght)
        {
            try
            {
                messagePacket.msgBodyHolder = new byte[messageLenght];
                // Start listening to the data asynchronously
                _pendingAsyncResult = _clientSocket.BeginReceive(
                    messagePacket.msgBodyHolder, 0, messageLenght, SocketFlags.None, 
                    new AsyncCallback(OnMessageBodyReceived), messagePacket);
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void AcumulateMessageBody(BrokerPacket messagePacket, int messageLenght, int offset)
        {
            _pendingAsyncResult = _clientSocket.BeginReceive(
                messagePacket.msgBodyHolder, offset, messageLenght, SocketFlags.None,
                new AsyncCallback(OnMessageBodyReceived), messagePacket);
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void OnMessageBodyReceived(IAsyncResult asyn)
        {
            if (_isDisposed)
            {
                return;
            }
            try
            {
                BrokerPacket messagePacket = (BrokerPacket)asyn.AsyncState;
                int bytesReceived = messagePacket.thisSocket.EndReceive(asyn);
                messagePacket.totalBodyBytesReceived += bytesReceived;

                if (messagePacket.totalBodyBytesReceived < messagePacket.msgBodyHolder.Length)
                {
                    // Incomplete message body, cumulate remainder
                    AcumulateMessageBody(messagePacket, 
                        messagePacket.msgBodyHolder.Length - messagePacket.totalBodyBytesReceived, 
                        messagePacket.totalBodyBytesReceived);
                    return;
                }

                SoapEnvelope sp = (SoapEnvelope)SerializationHelper.DeserializeObject(
                    messagePacket.msgBodyHolder, typeof(SoapEnvelope));

                if (sp.Body.Fault != null)
                {
                    SoapFault fault = sp.Body.Fault;
                    Exception ex = new Exception(fault.Reason.Text, new Exception(fault.Detail));
                    ex.Source = fault.Code.Value;
                    _bkClient.ExceptionCaught(ex);
                    return;
                }
                else if (sp.Body.Notification != null)
                {
                    NotifyListeners(sp.Body.Notification.BrokerMessage);
                }

                WaitForHeader();
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        private void NotifyListeners(BrokerMessage message)
        {
            if (message != null)
            {
                // Notify clients
                //  queue clients notification for execution, so that this thread could 
                //  release the lock (clients notification could be lengthy)
                ThreadPool.QueueUserWorkItem(delegate(object state)
                {
                    lock (this)
                    {
                        if (_brokerHandlerDelegate != null)
                        {
                            foreach (BrokerHandler handler in _brokerHandlerDelegate.GetInvocationList())
                            {
                                try
                                {
                                    handler(message);
                                }
                                catch { }
                            }
                        }
                    }
                    //_brokerHandlerDelegate(sp.Body.Notification.BrokerMessage);
                });
            }
        }
        
        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Disconnect()
        {
            try
            {
                if (_clientSocket != null && !_isDisposed)
                {
                    _clientSocket.Shutdown(SocketShutdown.Both);
                    _clientSocket.Close();
                }
                _isDisposed = true;
            }
            catch (Exception exception)
            {
                Trace.TraceError("Error while disconnecting socket: {0}{1}{2}",
                    exception.Message, Environment.NewLine, exception.StackTrace);
            }
        }

        internal bool HasListeners()
        {
            return _brokerHandlerDelegate != null;
        }

        public bool IsConnected()
        {
            if (_clientSocket != null)
            {
                return _clientSocket.Connected;  
            }
            return false; 
        }
    }
}
