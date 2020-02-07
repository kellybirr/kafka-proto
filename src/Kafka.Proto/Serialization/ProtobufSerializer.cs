using Confluent.Kafka;
using Google.Protobuf;

namespace Coderz.Kafka.Proto.Serialization
{
    /// <summary>
    /// Used for serializing Protobuf messages for Kafka producers.
    /// </summary>
    /// <typeparam name="TMessage">The generated Protobuf class, must implement Google.Protobuf.IMessage</typeparam>
    public class ProtobufSerializer<TMessage> : ISerializer<TMessage> 
        where TMessage : IMessage<TMessage>
    {
        /// <summary>
        /// Serializes a <typeparamref name="TMessage"/> object into a byte array.
        /// </summary>
        /// <param name="data">The object to serialize</param>
        /// <param name="context">The serialization context</param>
        /// <returns><paramref name="data" /> encoded in a byte array (or null if <paramref name="data" /> is null).</returns>

        public byte[] Serialize(TMessage data, SerializationContext context)
        {
            return data?.ToByteArray();
        }
    }

    /// <summary>
    /// Used for serializing Protobuf messages for Kafka producers.
    /// </summary>
    public class ProtobufSerializer : ISerializer<IMessage>
    {
        /// <summary>
        /// Serializes any <see cref="Google.Protobuf.IMessage"/> object into a byte array.
        /// </summary>
        /// <param name="data">The object to serialize</param>
        /// <param name="context">The serialization context</param>
        /// <returns><paramref name="data" /> encoded in a byte array (or null if <paramref name="data" /> is null).</returns>
        public byte[] Serialize(IMessage data, SerializationContext context)
        {
            return data?.ToByteArray();
        }
    }

}
