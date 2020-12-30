using Pink.RabbitMQ;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Samples
{
    class Program
    {
        const string ipAddress = "127.0.0.1";
        const int port = 5672;
        const string userName = "guest";
        const string pwd = "guest";

        static void Main(string[] args)
        {
            string exchangeName = "pink.topic";
            string routerKey = "pink";
            string queueName = "pinkQueue";

            CancellationTokenSource cts = new CancellationTokenSource();
            var ct = cts.Token;

            //发送消息
            PublishMQ(exchangeName, routerKey, 1, ct);

            //接收消息
            SubscriptionMQ(exchangeName, routerKey, queueName, ct);

            Console.ReadLine();

            cts.Cancel();

            Console.WriteLine("over.....");
        }

        /// <summary>
        /// 采取线程的方式，向指定的队列发送
        /// </summary>
        static void PublishMQ(string exchangeName, string routingKey, int count, CancellationToken cancellationToken)
        {
            RabbitMQClient c1 = new RabbitMQClient(ipAddress, "", port, userName, pwd);
            //创建一个exchange
            c1.ManagerInstance.ExchangeDeclare(exchangeName, RabbitExchangeType.Topic);

            Task taskPublish = Task.Factory.StartNew(() =>
            {
                int num = 0;
                while (!cancellationToken.IsCancellationRequested)
                {
                    Thread.Sleep(1000);
                    Dictionary<string, object> header = new Dictionary<string, object>();
                    c1.PublisherInstance.Publish(exchangeName, header, string.Format("这是第{0}条消息,发送时间{1:HH:mm:ss}", ++num, DateTime.Now), true, routingKey);
                }
                c1.Dispose();
            });
        }


        /// <summary>
        /// 创建10个客户端，每个客户端消费两个队列，查看是否同一消息有多个客户端接收
        /// </summary>
        private static void SubscriptionMQ(string exchange, string routerKey, string queue, CancellationToken cancellationToken)
        {
            Task tsk = Task.Factory.StartNew((num) =>
            {
                RabbitMQClient c1 = new RabbitMQClient(ipAddress, "", port, userName, pwd);
                c1.ManagerInstance.QueueDeclare(queue);
                c1.ManagerInstance.QueueBind(exchange, queue, routerKey);

                c1.ConsumerInstance.Subscription<RabbitBaseMessage>(queue, (msg) =>
                {
                    Console.WriteLine($"{DateTime.Now.ToShortTimeString()} 已经收到消息:{msg.OrginalBody}");
                });
            }, cancellationToken);
        }
    }
}
