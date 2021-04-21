using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Demo2Library
{
    class Program
    {
        static void Main(string[] args)
        {
            ConnectionFactory connectionFactory = new ConnectionFactory()
            {
                HostName = "localhost",
                Port = 5672,
            };

            using (var connection = connectionFactory.CreateConnection())
            {
                using (var model = connection.CreateModel())
                {
                    model.ExchangeDeclare("calculation_engine", "topic", true, false, new Dictionary<string, object>() {
                        { "alternate-exchange", "fallback" }
                    });

                    model.QueueDeclare("worker1", true, false, false);
                    model.QueueDeclare("worker2", true, false, false);

                    model.QueueBind("worker1", "calculation_engine", "*");
                    model.QueueBind("worker2", "calculation_engine", "*");

                    var prop = model.CreateBasicProperties();

                    var bytes = System.Text.Encoding.UTF8.GetBytes("teste");

                    var consumer = new EventingBasicConsumer(model);
                    consumer.Received += (sender, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" Worker 1 Received {0}", message);

                        int dots = message.Split('.').Length - 1;
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine(" [x] Done");

                        // Note: it is possible to access the channel via
                        //       ((EventingBasicConsumer)sender).Model here
                        model.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };

                    model.BasicConsume(queue: "worker1",
                                         autoAck: false,
                                         consumer: consumer);


                    var consumer2 = new EventingBasicConsumer(model);
                    consumer2.Received += (sender, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        Console.WriteLine(" Worker 2 Received {0}", message);

                        int dots = message.Split('.').Length - 1;
                        Thread.Sleep(dots * 1000);

                        Console.WriteLine(" [x] Done");

                        // Note: it is possible to access the channel via
                        //       ((EventingBasicConsumer)sender).Model here
                        model.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };

                    model.BasicConsume(queue: "worker2",
                                         autoAck: false,
                                         consumer: consumer2);
                    model.BasicPublish("calculation_engine", "*", prop, bytes);

                }
            }

            while (true)
            {

            }
            //Console.WriteLine("Hello World!");
        }
    }
}
