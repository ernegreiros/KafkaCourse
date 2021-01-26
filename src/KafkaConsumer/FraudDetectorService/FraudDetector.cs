using Confluent.Kafka;
using System;
using System.Threading;

namespace KafkaConsumer
{
    class FraudDetector
    {
        static void Main(string[] args)
        {
            var kafkaTopic = "ECOMMERCE_PURCHASE";

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "FraudDetectorService",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoOffsetStore = false
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            consumer.Subscribe(kafkaTopic);

            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume();

                    Console.WriteLine($"--------- Processing new order ---------");
                    Console.WriteLine($"{consumeResult.Offset}::{consumeResult.Partition}::");
                    Console.WriteLine($"--- Order ---");
                    Console.WriteLine($"{consumeResult.Message.Key}::{consumeResult.Message.Value}");
                    Console.WriteLine("Checking for fraud...");
                    Thread.Sleep(200);
                    Console.WriteLine("Order Processed!");

                    consumer.StoreOffset(consumeResult);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error Consuming message \n{ex.Message}");
                }
            }
        }
    }
}
