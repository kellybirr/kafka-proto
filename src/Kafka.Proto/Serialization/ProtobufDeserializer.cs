using System;
using System.Collections.Generic;
using Confluent.Kafka;
using Google.Protobuf;

namespace Coderz.Kafka.Proto.Serialization
{
    /// <summary>
    /// Used for deserializing Protobuf messages from Kafka consumers
    /// </summary>
    /// <typeparam name="TMessage">The generated Protobuf class, must implement Google.Protobuf.IMessage</typeparam>
    public class ProtobufDeserializer<TMessage> : IDeserializer<TMessage> 
        where TMessage : IMessage<TMessage>, new()
    {
        private readonly MessageParser<TMessage> _parser;

        /// <summary>
        ///  Default Constructor with default ( new() ) factory.
        /// </summary>
        public ProtobufDeserializer() : this( () => new TMessage() )
        { }

        /// <summary>
        /// Constructor that allows overriding the default object factory
        /// </summary>
        /// <param name="factory">Factory code to create new instances of <typeparamref name="TMessage"/>.</param>
        public ProtobufDeserializer(Func<TMessage> factory)
        {
            _parser = new MessageParser<TMessage>(factory);
        }

        /// <summary>
        /// Deserializes a <typeparamref name="TMessage"/> value from a byte array.
        /// </summary>
        /// <param name="data">The data to deserialize.</param>
        /// <param name="isNull">is the data or value null</param>
        /// <param name="context">The serialization context</param>
        /// <returns><paramref name="data" /> deserialized to a <typeparamref name="TMessage"/> (or null if data is null).</returns>
        public TMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data == null) return default;
            return _parser.ParseFrom(data.ToArray());
        }
    }
}
