using System;
using System.Collections.Generic;
using Fluffy.Kafka.Proto.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Fluffy.Kafka.Proto.Test
{
    [TestClass]
    public class UnitTest1
    {
        private const string TOPIC = "myTopic";

        [TestMethod]
        public void Test_RoundTrip()
        {
            var rnd = new Random();
            var record = new TestRecord
            {
                Id = Guid.NewGuid().ToString(),
                Integer = DateTime.UtcNow.Millisecond,
                Floating = rnd.NextDouble(),
                EnumeratedValue = MyEnum.Val2,
                TextValues =
                {
                    rnd.Next().ToString(),
                    rnd.Next().ToString(),
                    rnd.Next().ToString(),
                    rnd.Next().ToString()
                }
            };

            byte[] bytes;   // serialize to bytes
            using (var ser = new ProtobufSerializer<TestRecord>())
                bytes = ser.Serialize(TOPIC, record);

            TestRecord testing; // back to object
            using (var deSer = new ProtobufDeserializer<TestRecord>())
                testing = deSer.Deserialize(TOPIC, bytes);

            Assert.AreEqual(record.Id, testing.Id);
            Assert.AreEqual(record.Integer, testing.Integer);
            Assert.AreEqual(record.Floating, testing.Floating);
            Assert.AreEqual(record.EnumeratedValue, testing.EnumeratedValue);

            Assert.AreEqual(record.TextValues.Count, testing.TextValues.Count);
            for (int i = 0; i < record.TextValues.Count; i++)
                Assert.AreEqual(record.TextValues[i], testing.TextValues[i]);
        }

        [TestMethod]
        public void Test_Untyped()
        {
            var rnd = new Random();
            var record = new TestRecord
            {
                Id = Guid.NewGuid().ToString(),
                Integer = DateTime.UtcNow.Millisecond,
                Floating = rnd.NextDouble(),
                EnumeratedValue = MyEnum.Val2,
                TextValues =
                {
                    rnd.Next().ToString(),
                    rnd.Next().ToString(),
                    rnd.Next().ToString(),
                    rnd.Next().ToString()
                }
            };

            byte[] bytes;   // serialize to bytes
            using (var ser = new ProtobufSerializer())
                bytes = ser.Serialize(TOPIC, record);

            TestRecord testing; // back to object
            using (var deSer = new ProtobufDeserializer<TestRecord>())
                testing = deSer.Deserialize(TOPIC, bytes);

            Assert.AreEqual(record.Id, testing.Id);
            Assert.AreEqual(record.Integer, testing.Integer);
            Assert.AreEqual(record.Floating, testing.Floating);
            Assert.AreEqual(record.EnumeratedValue, testing.EnumeratedValue);

            Assert.AreEqual(record.TextValues.Count, testing.TextValues.Count);
            for (int i = 0; i < record.TextValues.Count; i++)
                Assert.AreEqual(record.TextValues[i], testing.TextValues[i]);
        }

        [TestMethod]
        public void Test_Configs()
        {
            var startingConfig = new Dictionary<string, object> { { "bootstrap.servers", "127.0.0.1:9092" } };

            using (var ser = new ProtobufSerializer<TestRecord>())
            {
                var cfg1 = ser.Configure(startingConfig, true);
                Assert.AreEqual(startingConfig, cfg1);

                var cfg2 = ser.Configure(startingConfig, false);
                Assert.AreEqual(startingConfig, cfg2);
            }

            using (var deSer = new ProtobufDeserializer<TestRecord>())
            {
                var cfg3 = deSer.Configure(startingConfig, true);
                Assert.AreEqual(startingConfig, cfg3);

                var cfg4 = deSer.Configure(startingConfig, false);
                Assert.AreEqual(startingConfig, cfg4);
            }
        }

    }
}
