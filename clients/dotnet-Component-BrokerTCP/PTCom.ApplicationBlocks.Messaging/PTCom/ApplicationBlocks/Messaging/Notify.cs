using System;

namespace PTCom.ApplicationBlocks.Messaging
{
    public class Notify
    {
        private string destinationName;

        private DestinationType destinationType;


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

        public DestinationType DestinationType
        {
            get
            {
                return this.destinationType;
            }
            set
            {
                this.destinationType = value;
            }
        }
    }
}