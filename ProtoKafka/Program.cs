using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Proto;
using Process = Proto.Process;

namespace ProtoKafka
{
    class Program
    {
        static void Main(string[] args)
        {
            var brokerList = "localhost:9092";
            var topic = "test1";
            var kp = StartKafkaProducerProcess(brokerList, topic);
            var kc = StartKafkaConsumer(brokerList, topic);
            
            var actor1 = Actor.Spawn(Actor.FromProducer(() => new MyActor(kp)));
            Console.WriteLine("Started.");

            while (true)
            {
                Console.ReadLine();
                actor1.Tell(new {Message = "hello"});
            }
        }

        private static Consumer<string, string> StartKafkaConsumer(string brokerList, string topic)
        {
            var config = new Dictionary<string, object>
            {
                ["bootstrap.servers"] = brokerList,
                ["group.id"] = "testconsumer1"
            };
            var sz = new StringDeserializer(Encoding.UTF8);
            var consumer = new Consumer<string, string>(config, sz, sz);
            consumer.Subscribe(topic);
            consumer.OnMessage += (sender, message) =>
            {
                var dsz = JsonConvert.DeserializeObject(message.Value);
                Console.WriteLine($"Kafka consumer: topic:{message.Topic} partition:{message.Partition} offset:{message.Offset} value:'{dsz}'");
            };
            Task.Run(() =>
            {
                while (true)
                {
                    consumer.Poll(100);
                }
            });
            return consumer;
        }

        private static PID StartKafkaProducerProcess(string brokerList, string topic)
        {
            Func<object, string> valueSerializer = JsonConvert.SerializeObject;
            var kp = new KafkaProducerProcess(brokerList, topic, null, valueSerializer);
            var id = ProcessRegistry.Instance.NextId();
            var (pid, ok) = ProcessRegistry.Instance.TryAdd(id, kp);
            if (ok) return pid;
            else throw new Exception("Failed to add KafkaProducer to ProcessRegistry.");
        }
    }

    internal class MyActor : IActor
    {
        private readonly PID _kafkaProducer;

        public MyActor(PID kafkaProducer)
        {
            _kafkaProducer = kafkaProducer;
        }

        public Task ReceiveAsync(IContext context)
        {
            switch (context.Message)
            {
                case Started _:
                    break;
                default:
                    Console.WriteLine($"Got message {context.Message}");
                    _kafkaProducer.Tell(context.Message);
                    break;
            }
            return Actor.Done;
        }
    }

    public class KafkaProducerProcess : Process
    {
        private readonly Producer<string, string> _producer;
        private readonly string _topic;
        private readonly Func<object, string> _keyConstructor;
        private readonly Func<object, string> _valueSerializer;

        public KafkaProducerProcess(string brokerList, string topic, Func<object, string> keyConstructor, Func<object, string> valueSerializer)
        {
            var config = new Dictionary<string, object>()
            {
                ["bootstrap.servers"] = brokerList
            };
            var sz = new StringSerializer(Encoding.UTF8);
            _producer = new Producer<string, string>(config, sz, sz);
            _topic = topic;
            _keyConstructor = keyConstructor;
            _valueSerializer = valueSerializer;
        }

        public override void SendUserMessage(PID pid, object message)
        {
            var key = _keyConstructor?.Invoke(message);
            var value = _valueSerializer(message);
            var deliveryReport = _producer.ProduceAsync(_topic, key, value).Result;
            Console.WriteLine($"Kafka producer: topic:{deliveryReport.Topic} partition:{deliveryReport.Partition} offset:{deliveryReport.Offset} value:{message}");
        }

        public override void SendSystemMessage(PID pid, object message)
        {
        }
    }
}