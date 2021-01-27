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
            var orderTopic = "ECOMMERCE_PURCHASE";
            var emailTopic = "ECOMMERCE_EMAIL";

            

            var configs = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                ClientId = Dns.GetHostName()
            };

            using var producer = new ProducerBuilder<string, string>(configs).Build();

            /*
             * The topic ECOMMERCE_PURCHASE, has 3 partitions so running more than one of FraudDetectorService consumer
             * with same group id, will force kafka to rebalance between consumers so each one can be listening
             * to a specific partition of the topic and run in parallel
            */
            for (int i = 0; i < 50; i++)
            {
                var id = random.Next(0, 999).ToString();
                var name = Guid.NewGuid().ToString();
                var purchaseValue = random.Next(0, 9999).ToString();

                producer.Produce(topic: orderTopic,
                             message: CreateOrderMessage(id, name, purchaseValue),
                             CallBack);

                producer.Produce(topic: emailTopic,
                                 message: CreateEmailMessage(id, name),
                                 CallBack);
            }

            producer.Flush();
        }

        private static Message<string, string> CreateOrderMessage(string id, string name, string purchaseValue)
        {
            var message = $"{id},{name},{purchaseValue}";
            return CreateMessage(key: id, value: message);
        }

        private static Message<string, string> CreateEmailMessage(string id, string name)
        {
            var message = $"Hi! {name}, Thank you for purchasing!";
            return CreateMessage(key: id, value: message);
        }

        private static Message<string, string> CreateMessage(string key, string value)
        {
            var message = new Message<string, string>
            {
                Key = key,
                Value = value
            };

            return message;
        }

        private static void CallBack(DeliveryReport<string, string> deliveryReport)
        {
            if (deliveryReport.Error.Code == ErrorCode.NoError)
            {
                Console.WriteLine($"-------- Message Successfully Sent! --------");
                Console.WriteLine($"Offset: {deliveryReport.Offset}");
                Console.WriteLine($"Key: {deliveryReport.Message.Key}");
                Console.WriteLine($"Value: {deliveryReport.Message.Value}");
            }
            else
            {
                Console.WriteLine($"-------- Error sending message! --------");
                Console.WriteLine($"Error Code: {deliveryReport.Error.Code}");
                Console.WriteLine($"isBrokerError: {deliveryReport.Error.IsBrokerError}");
                Console.WriteLine($"isError: {deliveryReport.Error.IsError}");
                Console.WriteLine($"isFatal: {deliveryReport.Error.IsFatal}");
                Console.WriteLine($"isLocalError: {deliveryReport.Error.IsLocalError}");
                Console.WriteLine($"Reason: {deliveryReport.Error.Reason}");
            }   
        }
    }
}
