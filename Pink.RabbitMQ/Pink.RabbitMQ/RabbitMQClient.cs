using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Pink.RabbitMQ
{

    /// <summary>
    /// RabbitMQ的客户端
    /// </summary>
    public class RabbitMQClient : IDisposable
    {
        #region Desciption

        /**
         * 1.AMQP： Advanced Message Queuing Protocol应用层标准高级消息队列协议
         * 2.TTL：TTL Time to Live Extensions过期时间
         * 3.per-queue TTL :删除未使用的且没有订阅者，且没有被重新声明。通过在声明Queue时在指定的Dic中设置Key:x-expires,Value:毫秒
         * 4.per-queue-message TTL：队列中消息的到期时间，到期后，此部分消息会在QUEUE的头部，会立即删除或转到死信队列中。通过在声明Queue时在指定的Dic中设置Key:x-message-ttl，Value:毫秒
         * 5.死信队列：当为队列设置了per-queue-message TTL后，1.消息到期后,2.或者消费端Reject拒收消息，3.或者超过max-length/max-length-bytes且x-over-flow被设置为drop-head，将自动转到对应的死信队列中。通过在声明Queue时在指定的Dic中设置Key:x-dead-letter-exchange/x-dead-letter-routing-key，Value:队列名称或路由Key
         * 6.per-Message TTL:每条消息的过期时间，每条消息在发布时都可设置不同的TTL。因此，每消息在过期时，不一定会及时扫描或过期，采取的策略很可能是消费端（推送或拉取）消费时来判断其是否有效。
         *                   通过在Publish消息时，为消息指定的BasicProperty上指定Expiration属性来设置。单位毫秒
         *                   如果队列中设置了per-queue-message TTL，且单个消息也设置了per-Message TTL，则取过期早的作为TTL。
         *                   
         * 交换机的类型：
         * a.fanout，基于该交换机发送的消息都会投递给所有的被绑定队列，不需匹配绑定时设定的key和header
         * b.direct，基于该交换机发送的消息会严格匹配绑定时的routeKey,只有消息的routeKey与绑定时routeKey匹配时，消息才会被投递到对应队列中
         * c.topic， 基于该交换机发送的消息会模糊匹配绑定时的routeKey,只有消息的routeKey与绑定时routeKey按通配符匹配上时(*匹配一个词，#匹配0至N个，因此建议routeKey必须用.来分隔词，.本身不占位置人用于分隔)，消息才会被投递到对应队列中                 
         * d.headers,基于该交换机发送的消息会根据消息发送时的Header头部信息，只有当消息的Header头部与绑定时定义的Header头部完全相等或包含时，消息才会被投递到对应队列中。header比较时，类型和值必须一样
         *
         * 事务和Confirm模式的区别
         * 1.Confirm只针对Publisher,而事务可Publisher也可Customer
         * 2.当Customer端是AutoAck时，事务不起作用。否则以事务为主
         * 3.Confirm只能针对同一个队列，而事务可以针对多个队列。性能上Confirm高于事务
         * 
         *  使用MQ来实现RPC
         * 1.生产端发布消息时，在BasicProperies中设置CorrelationId和ReplyTo两属性，分别代表关联ID以及消息消费端处理消息后须回复的队列名称
         *   生产端还需对ReplyTo所命名的队列做消息的订阅或拉取，以及正常处理。
         * 2.消费端正常消费和生产端投递的消息，在Ack前取到消息的CorrelationId和ReplyTo属性，进行相应业务处理，处理完后将结果封装成消息投递到ReplyTo所对应的队列中
         *   消费端在这里充当了服务端的角色，而生产端充当了调用者的角色。服务端可以部署多个，以解决负载均衡的效果
         * 
         * 本类的特点:
         * 1.队列定义时全部会自动定义同名的(xxx.dead)用于接收死信的队列
         * 2.死信队列接收超过72小时未处理的消息或重复入队超过10次的消息
         * 3.提供主动拉取和主动推送的消费机制，主动推送的效率更高
         * 4.消费端不管在拉取或推送消息时，如发生异常，为提高效率并不阻塞后续消息的消费，处理次数低于10次的消息会ACK+重新入队（自动记录入队次数）。处理次数大于等于10次的消息会Reject（自动进入死信对应的死信队列中）
         * 4.如果在消费消息时，想获取消息的更多信息，如MessageID、EnqueueTime、ExchangeName、RoutingKey等，请消息实体类继承RabbitDequeueBaseMessage即可
         */

        #endregion

        #region Fields

        private string VirtualHost;

        /// <summary>
        /// 基础连接
        /// </summary>
        private IConnection con;

        /// <summary>
        /// 基础的通道
        /// </summary>
        private IModel baseChannel;

        /// <summary>
        /// 生产端
        /// </summary>
        private IMQPublisher _innerPublisher;

        /// <summary>
        /// 管理端 
        /// </summary>
        private IMQManager _innerManager;

        /// <summary>
        /// 消费端
        /// </summary>
        private IMQConsumer _innerConsumer;

        /// <summary>
        /// 已经设置过(验证存在)的队列名称
        /// </summary>
        private HashSet<string> setExistsQueue = new HashSet<string>();

        private object lockTransObj = new object();

        private object lockInitObj = new object();

        #endregion

        #region Constructor

        /// <summary>
        /// 对主机、端口、访问的用户进行初始化
        /// </summary>
        /// <param name="hostName">主机名</param>
        /// <param name="virtualHost">虚拟机名称</param>
        /// <param name="port">端口</param>
        /// <param name="userName">用户名</param>
        /// <param name="password">密码</param>
        public RabbitMQClient(string hostName, string virtualHost, int port, string userName, string password)
        {
            ConnectionFactory fac = new ConnectionFactory();
            fac.HostName = hostName;
            fac.VirtualHost = VirtualHost = string.IsNullOrEmpty(virtualHost) ? "/" : virtualHost;
            fac.Port = port;
            fac.UserName = userName;
            fac.Password = password;
            fac.AutomaticRecoveryEnabled = true;
            fac.NetworkRecoveryInterval = TimeSpan.FromSeconds(10);

            con = fac.CreateConnection();
            baseChannel = con.CreateModel();
        }
        #endregion

        #region Private Members

        /// <summary>
        /// 可用的Channel
        /// </summary>
        private IModel UsableChannel
        {
            get
            {
                if (baseChannel.IsClosed && con.IsOpen)
                {
                    baseChannel = con.CreateModel();
                }

                return baseChannel;
            }
        }

        #endregion


        #region 公共属性

        /// <summary>
        /// 生产端实例
        /// </summary>
        public IMQPublisher PublisherInstance
        {
            get
            {
                if (_innerPublisher == null)
                {
                    lock (lockInitObj)
                    {
                        if (_innerPublisher == null)
                        {
                            _innerPublisher = new RabbitMQPublisher(() => UsableChannel, setExistsQueue, ManagerInstance.QueueDeclare);
                        }
                    }
                }

                return _innerPublisher;
            }
        }

        /// <summary>
        /// 管理端实例
        /// </summary>
        public IMQManager ManagerInstance
        {
            get
            {
                if (_innerManager == null)
                {
                    lock (lockInitObj)
                    {
                        if (_innerManager == null)
                        {
                            _innerManager = new RabbitMQManager(() => UsableChannel, setExistsQueue);
                        }
                    }
                }

                return _innerManager;
            }
        }

        /// <summary>
        /// 消费端实例
        /// </summary>
        public IMQConsumer ConsumerInstance
        {
            get
            {
                if (_innerConsumer == null)
                {
                    lock (lockInitObj)
                    {
                        if (_innerConsumer == null)
                        {
                            _innerConsumer = new RabbitMQConsumer(() => UsableChannel, () => con.CreateModel());
                        }
                    }
                }

                return _innerConsumer;
            }
        }

        #endregion


        #region 使用单独的Channel进行事务操作



        /// <summary>
        /// 使用事务的方法进行相应的处理，事务内部支持对列的管理和消息的发布
        /// </summary>
        /// <param name="publishDo">发布时的具体方法</param>
        /// <returns>操作是否成功，以及错误时的异常</returns>
        public Tuple<bool, Exception> UsingTrans(params Action<MQTransactionContent>[] publishDo)
        {
            var task = UsingTransAsync(publishDo);
            task.Wait();
            return task.Result;
        }

        /// <summary>
        /// 使用事务的方法进行相应的处理，事务内部支持对列的管理和消息的发布
        /// </summary>
        /// <param name="publishDo">发布时的具体方法</param>
        /// <returns>操作是否成功，以及错误时的异常</returns>
        public Task<Tuple<bool, Exception>> UsingTransAsync(params Action<MQTransactionContent>[] publishDo)
        {
            Task<Tuple<bool, Exception>> task = Task.Factory.StartNew(() =>
            {
                IModel transChannel = null;
                try
                {
                    //专门用于事务的Channel
                    transChannel = con.CreateModel();
                    MQTransactionContent transContent = new MQTransactionContent
                    (
                        new RabbitMQManager(() => transChannel, setExistsQueue),
                        new RabbitMQPublisher(() => transChannel, setExistsQueue, ManagerInstance.QueueDeclare)
                    );

                    transChannel.TxSelect();

                    foreach (var doSomething in publishDo)
                    {
                        doSomething.Invoke(transContent);
                    }

                    transChannel.TxCommit();

                    return new Tuple<bool, Exception>(true, null);
                }
                catch (Exception ex)
                {
                    if (transChannel != null)
                    {
                        transChannel.TxRollback();
                    }

                    return new Tuple<bool, Exception>(false, ex);
                }
                finally
                {
                    if (transChannel != null)
                    {
                        if (transChannel.IsOpen)
                        {
                            transChannel.Close();
                        }
                        transChannel.Dispose();
                    }
                }

            });
            return task;
        }

        #endregion

        #region 使用单独的Channel进行Confirm发送消息

        /// <summary>
        /// 使用专用的Confirm模式，每Publish消息一次就确认一次
        /// </summary>
        /// <param name="publishActions">要发布消息的具体方法集，每个方法只有一次Publish的调用</param>
        /// <returns>每个发布方法按序生成(从1开始)的发布成功标识</returns>
        public SortedDictionary<ulong, bool> PublishConfirm(params Action<IMQPublisher>[] publishActions)
        {

            var confirmChannel = con.CreateModel();
            IMQPublisher confirmPublisher = new RabbitMQPublisher(() => confirmChannel, setExistsQueue, ManagerInstance.QueueDeclare);
            confirmChannel.ConfirmSelect();
            SortedDictionary<ulong, bool> dicValues = new SortedDictionary<ulong, bool>();

            try
            {
                foreach (var action in publishActions)
                {
                    //当Channe关闭时直接退出
                    if (confirmChannel.IsClosed)
                    {
                        break;
                    }

                    var seqNo = confirmChannel.NextPublishSeqNo;
                    if (!dicValues.ContainsKey(seqNo))
                    {
                        dicValues.Add(seqNo, false);
                    }

                    try
                    {
                        action.Invoke(confirmPublisher);

                        dicValues[dicValues.Keys.Max()] = confirmChannel.WaitForConfirms();
                    }
                    catch (AlreadyClosedException)
                    {
                        break;
                    }
                    catch (OperationInterruptedException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("PublishConfirm方法使用Confirm模式发送消息时发生异常:{0},StackTrace:{1}", ex.Message, ex.StackTrace);
                    }
                }
            }
            finally
            {
                confirmPublisher.Dispose();
            }

            return dicValues;
        }

        /// <summary>
        /// 使用专用的Confirm模式，多次Publish消息后。全部批量确认
        /// </summary>
        /// <param name="publishActions">要发布消息的具体方法集，每个方法只有一次Publish的调用</param>
        /// <returns>已发布成功的方法序号(从1开始)，以及有异常时的相应描述</returns>
        public Tuple<ulong, Exception> PublishConfirms(params Action<IMQPublisher>[] publishActions)
        {
            var confirmChannel = con.CreateModel();
            IMQPublisher confirmPublisher = new RabbitMQPublisher(() => confirmChannel, setExistsQueue, ManagerInstance.QueueDeclare);
            confirmChannel.ConfirmSelect();
            Tuple<ulong, Exception> rtnValue;

            try
            {
                foreach (var action in publishActions)
                {
                    action.Invoke(confirmPublisher);
                }

                confirmChannel.WaitForConfirmsOrDie();
                rtnValue = new Tuple<ulong, Exception>(confirmChannel.NextPublishSeqNo - 1, null);
            }
            catch (AlreadyClosedException ex)
            {
                rtnValue = new Tuple<ulong, Exception>(Math.Max(0, confirmChannel.NextPublishSeqNo - 2), ex);
            }
            catch (OperationInterruptedException ex)
            {
                rtnValue = new Tuple<ulong, Exception>(Math.Max(0, confirmChannel.NextPublishSeqNo - 2), ex);
            }
            catch (System.IO.IOException ex)
            {
                rtnValue = new Tuple<ulong, Exception>(Math.Max(0, confirmChannel.NextPublishSeqNo - 2), ex);
            }
            catch (Exception ex)
            {
                //publishActions内的异常，这时还未调用Publish故计算器不会预加1
                rtnValue = new Tuple<ulong, Exception>(confirmChannel.NextPublishSeqNo - 1, ex);
            }
            finally
            {
                confirmPublisher.Dispose();
            }

            return rtnValue;
        }


        /// <summary>
        /// 使用专用的Confirm模式，Publish消息后通过异步的方式对消息进行确认
        /// </summary>
        /// <param name="ackHandler">消息确认成功时的事件处理方法</param>
        /// <param name="nackHandler">消息确认失败时的事件处理方法</param>
        /// <param name="publishActions">要发布消息的具体方法集，每个方法只有一次Publish的调用</param>
        /// <returns>发送消息的专用生产端，可获取已发送的序号，关闭通道等</returns>
        public IMQPublisher PublishConfirmAsync(Action<PublishAckArgs> ackHandler, Action<PublishNAckArgs> nackHandler, params Action<IMQPublisher>[] publishActions)
        {
            var confirmChannel = con.CreateModel();
            IMQPublisher confirmPublisher = new RabbitMQPublisher(() => confirmChannel, setExistsQueue, ManagerInstance.QueueDeclare);
            confirmChannel.ConfirmSelect();

            //确认接收
            confirmChannel.BasicAcks += (sender, e) =>
            {
                ackHandler(new PublishAckArgs(confirmChannel.NextPublishSeqNo - 1, e.DeliveryTag, e.Multiple));
            };

            //未确认接收
            confirmChannel.BasicNacks += (sender, e) =>
            {
                nackHandler(new PublishNAckArgs(confirmChannel.NextPublishSeqNo - 1, e.DeliveryTag, e.Multiple, e.Requeue));
            };

            try
            {
                foreach (var action in publishActions)
                {
                    action.Invoke(confirmPublisher);
                }

            }
            catch (AlreadyClosedException)
            {

            }
            catch (OperationInterruptedException)
            {

            }
            catch (System.IO.IOException)
            {

            }
            catch (Exception)
            {
                //publishActions内的异常，这时还未调用Publish故计算器不会预加1
            }


            return confirmPublisher;
        }

        #endregion

        #region IDisposable Support

        /// <summary>
        /// 要检测冗余调用
        /// </summary>
        private bool hasDisposed = false;


        protected virtual void Dispose(bool disposing)
        {
            if (!hasDisposed)
            {
                //释放托管资源
                if (disposing)
                {
                    //如果有消费端，须先关闭和释放消费端的每个通道
                    if (_innerConsumer != null)
                    {
                        _innerConsumer.Dispose();
                    }

                    //公共通道的关闭和释放
                    if (baseChannel != null && baseChannel.IsOpen)
                    {
                        baseChannel.Close();
                    }
                    baseChannel.Dispose();

                    //连接的关闭与释放
                    if (con != null && con.IsOpen)
                    {
                        con.Close();
                    }
                    con.Dispose();
                }

                //释放非托管对象，如将大型字段设置为 null。
                hasDisposed = true;
            }
        }

        /// <summary>
        /// 析构函数,只释放非托管资源
        /// </summary>
        ~RabbitMQClient()
        {
            Dispose(false);
        }

        /// <summary>
        /// 释放托管和非托管资源
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        #endregion


    }
}
