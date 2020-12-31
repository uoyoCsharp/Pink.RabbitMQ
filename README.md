# Pink.RabbitMQ

在 .NET Framework 4.0下的RabbitMQ的操作类

## 关键类说明

**RabbitMQClient** ： 该类为客户端操作最基础的类，后续的所有操作您都将通过使用它来进行完成。通过传入于RabbitMQ的连接信息来进行实例化。

当实例化一个**RabbitMQClient**之后，您将可以使用它某些属性来进行RabbitMQ的操作。
下方将用 **"clientInstance"** 来表示一个**RabbitMQClient**类的实例：

**clientInstance.ManagerInstance** ： 提供RabbitMQ的管理操作类，包括对RabbitMQ的“Exchange”、“Queue”的基本操作（新增，删除，清空消息，绑定队列，解绑队列等）。

**clientInstance.PublisherInstance** ： 提供向RabbitMQ发送消息的操作。

**clientInstance.ConsumerInstance** ： 提供向RabbitMQ订阅消息的操作。

## 使用说明

通过以下的代码，我们将直观的了解到**Pink.RabbitMQ**是如何使用：

````csharp
// 连接RabbitMQ内容
const string ipAddress = "127.0.0.1";
const int port = 5672;
const string userName = "guest";
const string pwd = "guest";

// 交换机等内容
const string exchangeName = "pink.topic";
const string routerKey = "pink";
const string queueName = "pinkQueue";

// 实例化一个Client
RabbitMQClient clientInstance = new RabbitMQClient(ipAddress, "", port, userName, pwd);

// 创建一个使用Topic模式的Exchange
clientInstance.ManagerInstance.ExchangeDeclare(exchangeName, RabbitExchangeType.Topic);

// 通过Client来发送消息
Dictionary<string, object> emptyHeader = new Dictionary<string, object>();
clientInstance.PublisherInstance.Publish(exchangeName, emptyHeader, "这里是消息内容", true, routingKey);

// 创建一个Queue
clientInstance.ManagerInstance.QueueDeclare(queue);
// 将Queue和Exchange绑定，并且指定RouterKey
clientInstance.ManagerInstance.QueueBind(exchange, queue, routerKey);

// 通过Client来订阅队列所收到的消息
clientInstance.ConsumerInstance.Subscription<RabbitBaseMessage>(queue, (msg) =>
{
    Console.WriteLine($"已经收到消息:{msg.OrginalBody}");
});
````

通过[Samples案例项目](https://github.com/uoyoCsharp/Pink.RabbitMQ/tree/main/Pink.RabbitMQ/Samples)也可以了解API的使用。

## 特性

- 当使用 **QueueDeclare(string queueName)** 来创建一个队列的时候，默认会创建一个名为："yourQueueName.dead"的死信队列。如果您想自定义队列创建规则，请使用 **QueueDeclare(string queueName, RabbitQueueArgrmentContext arguments)** 方法中的 **RabbitQueueArgrmentContext** 来进行指定。
- 在与RabbitMQ进行交互时，大部分API都提供了**IDictionary<string, object> headerArguments**的参数，您可以根据需要将header参数传入给RabbitMQ，以实现更多的功能。
