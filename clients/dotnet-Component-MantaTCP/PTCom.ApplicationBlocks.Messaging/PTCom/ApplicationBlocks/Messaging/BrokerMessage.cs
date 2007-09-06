using System;

namespace PTCom.ApplicationBlocks.Messaging
{

    public class BrokerMessage
    {

        private DeliveryMode deliveryMode;

        private int priority;

        private string messageId;

        private string correlationId;

        private string timestamp;

        private string expiration;

        private string destinationName;

        private string textPayload;

        public BrokerMessage()
        {
            deliveryMode = DeliveryMode.TRANSIENT;
            priority = 4;
            messageId = "";
            correlationId = "";
            timestamp = "";
            expiration = "";
            destinationName = "";
            textPayload = "";
        }

        public DeliveryMode DeliveryMode
        {
            get
            {
                return this.deliveryMode;
            }
            set
            {
                this.deliveryMode = value;
            }
        }

        public int Priority
        {
            get
            {
                return this.priority;
            }
            set
            {
                this.priority = value;
            }
        }

        public string MessageId
        {
            get
            {
                return this.messageId;
            }
            set
            {
                this.messageId = value;
            }
        }

        public string CorrelationId
        {
            get
            {
                return this.correlationId;
            }
            set
            {
                this.correlationId = value;
            }
        }

        public string Timestamp
        {
            get
            {
                return this.timestamp;
            }
            set
            {
                this.timestamp = value;
            }
        }

        public string Expiration
        {
            get
            {
                return this.expiration;
            }
            set
            {
                this.expiration = value;
            }
        }

        public string DestinationName
        {
            get
            {
                return this.destinationName;
            }
            set
            {
                this.destinationName = value;
            }
        }

        public string TextPayload
        {
            get
            {
                return this.textPayload;
            }
            set
            {
                this.textPayload = value;
            }
        }
    }
}
