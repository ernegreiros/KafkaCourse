using Confluent.Kafka;
using System;

namespace LogService
{
    class Log
    {
        static void Main(string[] args)
        {
            var kafkaTopic = "^ECOMMERCE.*";

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "LogService",
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

                    Console.WriteLine($"--------- Log ---------");
                    Console.WriteLine($"Topic: {consumeResult.Topic}");
                    Console.WriteLine($"{consumeResult.Offset}::{consumeResult.Partition}::");
                    Console.WriteLine($"{consumeResult.Message.Key}::{consumeResult.Message.Value}");

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
