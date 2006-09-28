using System;
using PTCom.ApplicationBlocks.Messaging;

namespace PTCom.ApplicationBlocks.Messaging.Sample
{
    public class MQConsumer : Listener
    {
        public MQConsumer()
        {
        }

        public override void OnMessage(BrokerMessage message)
        {
            Console.WriteLine(DateTime.Now.Ticks + "::" + message.TextPayload);
        }

        static void Main(string[] args)
        {
            MQConsumer consumer = new MQConsumer();

			BrokerClient bc = new BrokerClient("localhost", 2222, "tcp://mycompany.com/mysniffer");

            Notify nreq1 = new Notify();
            nreq1.AcknowledgeMode = AcknowledgeMode.AUTO;
            nreq1.DestinationName = "sample_topic1";
            nreq1.DestinationType = DestinationType.TOPIC;

            bc.Subscribe(nreq1, consumer);

            while (true)
            {
                if (Console.ReadLine().Equals("exit"))
                {
                    //BrokerClient.Shutdown();
                    Environment.Exit(0);
                }
            }

        }
    }
}
