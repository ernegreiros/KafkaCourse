using Confluent.Kafka;
using System;
using System.Net;

namespace KafkaProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            var configs = new ProducerConfig
            {
                BootstrapServers = "127.0.0.1:9092",
                ClientId = Dns.GetHostName()
            };

            using (var producer = new ProducerBuilder<string, string>(configs).Build())
            {
                producer.Produce(topic:"GENERIC_TOPIC", 
                                 message: new Message<string, string>{ Key = "MessageKey", Value ="MessageValue" },
                                 CallBack);

                producer.Flush();
            }

            Console.ReadLine();
        }

        private static void CallBack(DeliveryReport<string, string> deliveryReport)
        {
            if (deliveryReport.Error.Code == ErrorCode.NoError)
            {
                Console.WriteLine($"Message Sucessfuly Sent! Offset:{deliveryReport.Offset}, Key:{deliveryReport.Message.Key}, Value:{deliveryReport.Message.Value} ");
            }
            else
            {
                Console.WriteLine($"Error sending message! " +
                                  $"Error Code: {deliveryReport.Error.Code}, " +
                                  $"isBrokerError: {deliveryReport.Error.IsBrokerError}, " +
                                  $"isError: {deliveryReport.Error.IsError}, " +
                                  $"isFatal: {deliveryReport.Error.IsFatal}, " +
                                  $"isLocalError: {deliveryReport.Error.IsLocalError}, " +
                                  $"Reason: {deliveryReport.Error.Reason}, ");
            }
            
        }
    }
}
