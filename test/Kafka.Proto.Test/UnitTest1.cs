using System;
using Coderz.Kafka.Proto.Serialization;
using Fluffy.Kafka.Proto.Test;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Coderz.Kafka.Proto.Test
{
    [TestClass]
    public class UnitTest1
    {
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
            var ser = new ProtobufSerializer<TestRecord>();
            bytes = ser.Serialize(record, default);

            TestRecord testing; // back to object
            var deSer = new ProtobufDeserializer<TestRecord>();
            testing = deSer.Deserialize(bytes, false,default);

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
            var ser = new ProtobufSerializer();
            bytes = ser.Serialize(record, default);

            TestRecord testing; // back to object
            var deSer = new ProtobufDeserializer<TestRecord>();
            testing = deSer.Deserialize(bytes, false, default);

            Assert.AreEqual(record.Id, testing.Id);
            Assert.AreEqual(record.Integer, testing.Integer);
            Assert.AreEqual(record.Floating, testing.Floating);
            Assert.AreEqual(record.EnumeratedValue, testing.EnumeratedValue);

            Assert.AreEqual(record.TextValues.Count, testing.TextValues.Count);
            for (int i = 0; i < record.TextValues.Count; i++)
                Assert.AreEqual(record.TextValues[i], testing.TextValues[i]);
        }
    }
}
