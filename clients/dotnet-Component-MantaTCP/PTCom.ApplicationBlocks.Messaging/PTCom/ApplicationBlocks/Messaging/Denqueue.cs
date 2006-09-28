using System;

namespace PTCom.ApplicationBlocks.Messaging
{
    public class Denqueue
    {
        private string destinationName;

        private long timeOut;

        private AcknowledgeMode ackMode;

        public Denqueue()
        {
            destinationName = "";
            timeOut = 0;
            ackMode = AcknowledgeMode.AUTO;
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

        public long TimeOut
        {
            get
            {
                return this.timeOut;
            }
            set
            {
                this.timeOut = value;
            }
        }

        public AcknowledgeMode AcknowledgeMode
        {
            get
            {
                return this.ackMode;
            }
            set
            {
                this.ackMode = value;
            }
        }
    }
}