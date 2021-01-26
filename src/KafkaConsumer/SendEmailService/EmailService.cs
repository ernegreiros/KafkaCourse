using Confluent.Kafka;
using System;
using System.Threading;

namespace SendEmailService
{
    class EmailService
    {
        static void Main(string[] args)
        {
            var kafkaTopic = "ECOMMERCE_EMAIL";

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "SendEmailService",
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

                    Console.WriteLine($"--------- Sending new email ---------");
                    Console.WriteLine($"{consumeResult.Offset}::{consumeResult.Partition}::");
                    Console.WriteLine($"--- Email ---");
                    Console.WriteLine($"{consumeResult.Message.Key}::{consumeResult.Message.Value}");
                    Console.WriteLine("Sending...");
                    Thread.Sleep(200);
                    Console.WriteLine("Email sent!");

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
