using System;

namespace PTCom.ApplicationBlocks.Messaging
{
    public class Notification
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