using System;

namespace PTCom.ApplicationBlocks.Messaging
{
    public class Publish
    {
        private BrokerMessage brokerMessage;

        public Publish()
        {
        }

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