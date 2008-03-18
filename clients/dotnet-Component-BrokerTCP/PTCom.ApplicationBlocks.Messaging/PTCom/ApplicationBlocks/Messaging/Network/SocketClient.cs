
using System;
using System.Net;
using System.Threading;
using System.Net.Sockets;
using PTCom.ApplicationBlocks.Messaging.Soap;
using PTCom.ApplicationBlocks.Messaging.Util;


namespace PTCom.ApplicationBlocks.Messaging.Network
{
    /// <summary>
    /// Description of SocketClient.	
    /// </summary>
    public class SocketClient
    {
        private string _host;
        private int _port;
        private BrokerClient _bkClient;
        private Socket _clientSocket;
        private Boolean _isWaitingMessage = false;
        private Listener _listener;

        public SocketClient(string host, int port, BrokerClient bkClient)
        {
            _host = host;
            _port = port;
            _bkClient = bkClient;
            try
            {
                // Create the socket instance
                _clientSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                _clientSocket.Connect(_host, _port);
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }

        }

        public void SendMessage(SoapEnvelope soap, bool expectResponse)
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


        private void WaitForHeader()
        {
            try
            {
                BrokerPacket messagePacket = new BrokerPacket();
                messagePacket.thisSocket = _clientSocket;
                // Start listening to the data asynchronously
                _clientSocket.BeginReceive(messagePacket.msgLengthHolder, 0, messagePacket.msgLengthHolder.Length, SocketFlags.None, new AsyncCallback(OnHeaderReceived), messagePacket);
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }

        private void AcumulateMessageHeader(BrokerPacket messagePacket, int messageLenght, int offset)
        {
            messagePacket.thisSocket = _clientSocket;
            // Start listening to the data asynchronously
            _clientSocket.BeginReceive(messagePacket.msgLengthHolder, offset, messageLenght, SocketFlags.None, new AsyncCallback(OnHeaderReceived), messagePacket);
        }


        private void OnHeaderReceived(IAsyncResult asyn)
        {
            try
            {
                BrokerPacket messagePacket = (BrokerPacket)asyn.AsyncState;
                int bytesReceived = messagePacket.thisSocket.EndReceive(asyn);
                messagePacket.totalHeaderBytesReceived += bytesReceived;
                if (messagePacket.totalHeaderBytesReceived < 4)
                {
                    // Incomplete message header,  cumulate remainder
                    AcumulateMessageHeader(messagePacket, 4 - messagePacket.totalHeaderBytesReceived, messagePacket.totalHeaderBytesReceived);
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

        private void WaitForMessageBody(BrokerPacket messagePacket, int messageLenght)
        {
            try
            {
                messagePacket.msgBodyHolder = new byte[messageLenght];
                // Start listening to the data asynchronously
                _clientSocket.BeginReceive(messagePacket.msgBodyHolder, 0, messageLenght, SocketFlags.None, new AsyncCallback(OnMessageBodyReceived), messagePacket);
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }

        private void AcumulateMessageBody(BrokerPacket messagePacket, int messageLenght, int offset)
        {
            _clientSocket.BeginReceive(messagePacket.msgBodyHolder, offset, messageLenght, SocketFlags.None, new AsyncCallback(OnMessageBodyReceived), messagePacket);
        }

        private void OnMessageBodyReceived(IAsyncResult asyn)
        {
            try
            {
                BrokerPacket messagePacket = (BrokerPacket)asyn.AsyncState;
                int bytesReceived = messagePacket.thisSocket.EndReceive(asyn);
                messagePacket.totalBodyBytesReceived += bytesReceived;

                if (messagePacket.totalBodyBytesReceived < messagePacket.msgBodyHolder.Length)
                {
                    // Incomplete message body, cumulate remainder
                    AcumulateMessageBody(messagePacket, messagePacket.msgBodyHolder.Length - messagePacket.totalBodyBytesReceived, messagePacket.totalBodyBytesReceived);
                    return;
                }

                SoapEnvelope sp = (SoapEnvelope)SerializationHelper.DeserializeObject(messagePacket.msgBodyHolder, typeof(SoapEnvelope));

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
                    if (_listener != null)
                    {
                        _listener.OnMessage(sp.Body.Notification.BrokerMessage);
                    }
				}
                
                WaitForHeader();
            }
            catch (Exception ex)
            {
                _bkClient.ExceptionCaught(ex);
            }
        }


        public void Disconnect()
        {
            if (_clientSocket != null)
            {
                _clientSocket.Close();
                _clientSocket = null;
            }
        }

        internal void SetListener(Listener listener)
        {
            _listener = listener;
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
