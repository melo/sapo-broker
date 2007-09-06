using System;

namespace PTCom.ApplicationBlocks.Messaging
{
    public abstract class Listener
    {
        public abstract void OnMessage(BrokerMessage message);

        public override string ToString()
        {
            return base.ToString() + "#" + Guid.NewGuid().ToString();
        }
    }
}
