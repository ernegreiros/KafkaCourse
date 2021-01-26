using Confluent.Kafka;
using System;
using System.Net;

namespace KafkaProducer
{
    class Producer
    {
        private static Random random = new Random();

        static void Main(string[] args)
        {
            var kafkaTopic = "ECOMMERCE_PURCHASE";
            var id = random.Next(0, 999).ToString();
            var name = Guid.NewGuid().ToString();
            var purchaseValue = random.Next(0, 9999).ToString();

            var configs = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };

            using var producer = new ProducerBuilder<string, string>(configs).Build();

            var message = new Message<string, string>
            {
                Key = id,
                Value = $"{id},{name},{purchaseValue}"
            };

            producer.Produce(topic: kafkaTopic,
                             message: message,
                             CallBack);
            producer.Flush();
        }

        private static void CallBack(DeliveryReport<string, string> deliveryReport)
        {
            if (deliveryReport.Error.Code == ErrorCode.NoError)
            {
                Console.WriteLine($"Message Successfully Sent!\n" +
                                  $"Offset:{deliveryReport.Offset}\n" +
                                  $"Key:{deliveryReport.Message.Key}\n" +
                                  $"Value:{deliveryReport.Message.Value}");
            }
            else
            {
                Console.WriteLine($"Error sending message!\n" +
                                  $"Error Code: {deliveryReport.Error.Code}\n" +
                                  $"isBrokerError: {deliveryReport.Error.IsBrokerError}\n" +
                                  $"isError: {deliveryReport.Error.IsError}\n" +
                                  $"isFatal: {deliveryReport.Error.IsFatal}\n" +
                                  $"isLocalError: {deliveryReport.Error.IsLocalError}\n" +
                                  $"Reason: {deliveryReport.Error.Reason}");
            }   
        }
    }
}
