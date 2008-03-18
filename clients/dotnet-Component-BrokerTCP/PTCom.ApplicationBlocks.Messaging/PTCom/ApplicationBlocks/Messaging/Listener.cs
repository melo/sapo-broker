using System;

namespace PTCom.ApplicationBlocks.Messaging
{
    public interface IListener
    {
        void OnMessage(BrokerMessage message);
        void OnConnectionClosed();
    }

    public abstract class Listener : IListener
    {
        public override string ToString()
        {
            return base.ToString() + "#" + Guid.NewGuid().ToString();
        }

        #region IListener Members

        public abstract void OnMessage(BrokerMessage message);
        public abstract void OnConnectionClosed();

        #endregion
    }
}
