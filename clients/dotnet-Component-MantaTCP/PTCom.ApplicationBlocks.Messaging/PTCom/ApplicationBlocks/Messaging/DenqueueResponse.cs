using System;

namespace PTCom.ApplicationBlocks.Messaging
{

    public class DenqueueResponse
    {
        private BrokerMessage brokerMessage;

        public BrokerMessage BrokerMessage
        {
            get
            {
                return this.brokerMessage;
            }
            set
            {
                this.brokerMessage = value;
            }
        }
    }
}