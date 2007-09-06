using System;
using System.Collections.Generic;
using System.Xml;
using System.Xml.Serialization;
using System.IO;
using System.Text;

namespace PTCom.ApplicationBlocks.Messaging.Util
{
    public class SerializationHelper
    {
        public static byte[] SerializeObject(Object pObject)
        {
            MemoryStream memoryStream = new MemoryStream();
            XmlSerializer xs = new XmlSerializer(pObject.GetType());
            XmlTextWriter xmlTextWriter = new XmlTextWriter(memoryStream, BrokerClient.ENCODING);
            xs.Serialize(xmlTextWriter, pObject);
            memoryStream = (MemoryStream)xmlTextWriter.BaseStream;
            return memoryStream.ToArray();
        }

        public static Object DeserializeObject(byte[] data, Type type)
        {
            XmlSerializer xs = new XmlSerializer(type);
            MemoryStream memoryStream = new MemoryStream(data);
            XmlTextWriter xmlTextWriter = new XmlTextWriter(memoryStream, BrokerClient.ENCODING);
            return xs.Deserialize(memoryStream);
        }
    }
}
