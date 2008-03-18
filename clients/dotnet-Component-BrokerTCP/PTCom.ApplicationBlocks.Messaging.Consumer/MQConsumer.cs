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
		
		public override void  OnConnectionClosed()
        {
            Console.WriteLine("ConnectionClosed()");
        }

        static void Main(string[] args)
        {
            MQConsumer consumer = new MQConsumer();

            BrokerClient bc = new BrokerClient("10.135.5.86", 3322, "tcp://mycompany.com/mysniffer");

            Notify nreq1 = new Notify();
            nreq1.DestinationName = "/sapo/webanalytics/pageviews";
            nreq1.DestinationType = DestinationType.TOPIC;

			bc.AddAsyncConsumer(nreq1, consumer);
            Console.WriteLine("listening... ");

            while (true)
            {			
                if (Console.ReadLine().Equals("exit"))
                {
                    bc.Dispose();
                    Environment.Exit(0);
                }
            }
        }
    }
}