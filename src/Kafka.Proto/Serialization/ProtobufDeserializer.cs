using System;
using System.Collections.Generic;
using Confluent.Kafka.Serialization;
using Google.Protobuf;

namespace Fluffy.Kafka.Proto.Serialization
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
        /// Releases any unmanaged resources owned by the object.
        /// </summary>
        public void Dispose() { }

        /// <summary>
        /// Deserializes a <typeparamref name="TMessage"/> value from a byte array.
        /// </summary>
        /// <param name="topic">The topic associated with the data (ignored by this deserializer).</param>
        /// <param name="data">The data to deserialize.</param>
        /// <returns><paramref name="data" /> deserialized to a <typeparamref name="TMessage"/> (or null if data is null).</returns>
        public TMessage Deserialize(string topic, byte[] data)
        {
            if (data == null) return default;
            return _parser.ParseFrom(data);
        }

        /// <inheritdoc />
        public IEnumerable<KeyValuePair<string, object>> Configure(IEnumerable<KeyValuePair<string, object>> config, bool isKey)
        {
            return config;
        }
    }
}
