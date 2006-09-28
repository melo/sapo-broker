using System;

namespace PTCom.ApplicationBlocks.Messaging
{

    public class Enqueue
    {
        private BrokerMessage brokerMessage;

        public Enqueue()
        {
            brokerMessage = new BrokerMessage();
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