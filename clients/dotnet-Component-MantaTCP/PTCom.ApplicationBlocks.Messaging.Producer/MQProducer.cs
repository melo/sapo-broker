using System;
using System.Text;
using PTCom.ApplicationBlocks.Messaging;

namespace PTCom.ApplicationBlocks.Messaging.Sample
{
	class MQProducer
	{
		private static Random rnd = new Random();

		private static string RandomString(int size)
		{
			StringBuilder builder = new StringBuilder();
			char ch;
			for (int i = 0; i < size; i++)
			{
				ch = Convert.ToChar(Convert.ToInt32(Math.Floor(26 * rnd.NextDouble() + 65)));
				builder.Append(ch);
			}
			return builder.ToString();
		}

		static void Main(string[] args)
		{

			BrokerClient bk = new BrokerClient("localhost", 2222, "tcp://mycompany.com/mypublisher");

			Console.WriteLine("Start sending");
			for (int i = 0; i < 100000; i++)
			{
				Console.WriteLine("message:" + i);
				Publish pub_req = new Publish();
				pub_req.BrokerMessage.TextPayload = RandomString(200);
				pub_req.BrokerMessage.DestinationName = "sample_topic1";
				bk.PublishMessage(pub_req);
				System.Threading.Thread.Sleep(500);
			}

		}
	}
}
