using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Google.Protobuf;

namespace Fluffy.Kafka.Proto.Serialization
{
    /// <summary>
    /// Used for serializing Protobuf messages for Kafka producers.
    /// </summary>
    /// <typeparam name="TMessage">The generated Protobuf class, must implement Google.Protobuf.IMessage</typeparam>
    public class ProtobufSerializer<TMessage> : ISerializer<TMessage> 
        where TMessage : IMessage<TMessage>
    {
        /// <summary>
        /// Releases any unmanaged resources owned by the object.
        /// </summary>
        public void Dispose() { }

        /// <summary>
        /// Serializes a <typeparamref name="TMessage"/> object into a byte array.
        /// </summary>
        /// <param name="topic">The topic associated with the data (ignored by this serializer).</param>
        /// <param name="data">The object to serialize</param>
        /// <returns><paramref name="data" /> encoded in a byte array (or null if <paramref name="data" /> is null).</returns>
        public byte[] Serialize(string topic, TMessage data)
        {
            return data?.ToByteArray();
        }

        /// <inheritdoc />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}
