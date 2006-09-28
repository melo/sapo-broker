using System;
using System.Collections.Generic;
using System.Text;
using PTCom.ApplicationBlocks.Messaging.Util;

namespace PTCom.ApplicationBlocks.Messaging
{
	internal class DenqueResponseHolder
	{
		private static Dictionary<string, BlockingQueue<DenqueueResponse>> _syncResponses = new Dictionary<string, BlockingQueue<DenqueueResponse>>();

		private static BlockingQueue<DenqueueResponse> GetMessageHolder(string queueName)
		{
			lock (_syncResponses)
			{
				string dkey = "QUEUE#" + queueName;

				BlockingQueue<DenqueueResponse> bqR;

				try
				{
					bqR = _syncResponses[dkey];
					if (bqR == null)
					{
						bqR = populateMap(dkey);
					}
				}
				catch (KeyNotFoundException)
				{
					bqR = populateMap(dkey);
				}

				return bqR;
			}
		}

		private static BlockingQueue<DenqueueResponse> populateMap(string dictKey)
		{
			BlockingQueue<DenqueueResponse> bqR = new BlockingQueue<DenqueueResponse>();
			_syncResponses[dictKey] = bqR;
			return bqR;
		}

		public static DenqueueResponse GetMessageFromQueue(string queueName)
		{
			BlockingQueue<DenqueueResponse> bqR = GetMessageHolder(queueName);
			return bqR.Dequeue();
		}

		public static void AddMessageToQueue(DenqueueResponse dqResponse)
		{
			BlockingQueue<DenqueueResponse> bqR = GetMessageHolder(dqResponse.BrokerMessage.DestinationName);
			bqR.Enqueue(dqResponse);
		}
	}
}
