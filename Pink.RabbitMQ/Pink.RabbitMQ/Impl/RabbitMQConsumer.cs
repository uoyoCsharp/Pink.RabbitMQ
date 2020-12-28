/*
 * 
 * Encoding:UTF-8
 * Version: 1.0
 * Create Date:  2019-08-20
 * Author: Richie
 * Description: 适用于RabbitMQ的消息消费端
 *           
 * Modify Date: 
 * Modifier: 
 * Description: 
*/

using Newtonsoft.Json;
using Pink.RabbitMQ.Helper;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Pink.RabbitMQ
{
    /// <summary>
    /// RabbitMQ的消息消费端
    /// </summary>
    internal class RabbitMQConsumer : IMQConsumer
    {
        #region INNER CLASS DECLARE

        /// <summary>
        /// MQ的Channel上下文环境
        /// </summary>
        private class MQChannelContext
        {
            public IModel Channel { get; set; }

            public string ConsumerTag { get; set; }

            public ulong ReceiveCount { get; set; }

            public ulong AckCount { get; set; }

            public ulong NAckOrFailedCount { get; set; }

            private bool canConsume;

            public bool CanConsume
            {
                get
                {
                    return canConsume && Channel != null && Channel.IsOpen;
                }
                set
                {
                    canConsume = value;
                }
            }

            public bool QosInited { get; set; }

        }

        #endregion

        #region FIELDS

        /// <summary>
        /// 消费端的上下文环境 
        /// </summary>
        private ConcurrentDictionary<string, MQChannelContext> dicConsumerContext = new ConcurrentDictionary<string, MQChannelContext>();

        private const long MAX_TIMESTAMP = 253402214400000;

        private object lockConsumeObj = new object();

        #endregion

        #region 构造函数

        /// <summary>
        /// 获取管理用的共用通道，持续可用
        /// </summary>
        private Func<IModel> manageChannelGetMethod;

        /// <summary>
        /// 专门用于消费某个队列的通道
        /// </summary>
        private Func<IModel> consumeChannelCreateMethod;

        /// <summary>
        /// 获取管理用的通道的表达式进行初始化
        /// </summary>
        /// <param name="manageChannelGetExp">管理MQ所用通道获取的方法表达式</param>
        /// <param name="consumeChannelCreateExp">创建专用消费的通道的方法表达式</param>
        public RabbitMQConsumer(Expression<Func<IModel>> manageChannelGetExp, Expression<Func<IModel>> consumeChannelCreateExp)
        {
            manageChannelGetMethod = manageChannelGetExp.Compile();
            consumeChannelCreateMethod = consumeChannelCreateExp.Compile();
        }

        #endregion

        #region 私有成员

        /// <summary>
        /// 获取管理MQ的专用通道
        /// </summary>
        private IModel ManageChannel
        {
            get
            {
                return manageChannelGetMethod.Invoke();
            }
        }


        private void InitializeQos(string queueName)
        {
            if (dicConsumerContext.ContainsKey(queueName) && !dicConsumerContext[queueName].QosInited)
            {
                /*
                 * prefetchSize 当前channel的最大预取数,目前未实现批量获取 只能为0
                 * prefetchCount 最大未Ack数量，为1表示必须Ack一个再推送一个（可实现公平分发），为0表示不受限（尽早将消息分发给消费端）
                 * global 表示表示为是否全局，还是仅限此channel
                 */
                dicConsumerContext[queueName].Channel.BasicQos(0, 2000, false);
                dicConsumerContext[queueName].QosInited = true;
            }

        }

        #endregion

        #region 实现IMQConsumer的成员

        /// <summary>
        /// 消费端订阅某个队列,采取推送的方式
        /// </summary>
        /// <typeparam name="T">该队列中存放的消息实体类型</typeparam>
        /// <param name="queueName">队列名称</param>
        /// <param name="consumeAction">消费消息的方法,当处理失败时请直接抛出异常</param>
        public void Subscription<T>(string queueName, Action<T> consumeAction) where T : RabbitBaseMessage, new()
        {
            #region 1.进行验证或前置准备

            if (dicConsumerContext.ContainsKey(queueName))
            {
                throw new Exception("Only one queue can be consumed on the same client");
            }

            //先检查消息队列是否存在，不存在时直接抛出异常
            var channel = ManageChannel;
            try
            {
                var declareOk = channel.QueueDeclarePassive(queueName);
            }
            catch (OperationInterruptedException ex)
            {
                if (channel.IsClosed && channel.IsOpen)
                {
                    throw ex;
                }
            }

            if (!dicConsumerContext.TryAdd(queueName, new MQChannelContext()
            {
                Channel = consumeChannelCreateMethod.Invoke(),
                CanConsume = true,
            }))
            {
                return;
            }

            InitializeQos(queueName);

            #endregion

            #region 2.注册订阅事件相关

            var consumer = new EventingBasicConsumer(dicConsumerContext[queueName].Channel);

            #region 2.1 自动解析头部的方法

            //自动解析消息的头部信息
            Action<T, BasicDeliverEventArgs, string> analysizeHead = (model, evArg, msgBody) =>
            {
                model.OrginalBody = msgBody;
                model.ExchangeName = evArg.Exchange;
                model.HeaderArguments = evArg.BasicProperties.Headers;
                model.Redelivered = evArg.Redelivered || evArg.BasicProperties.Headers != null && evArg.BasicProperties.Headers.ContainsKey("Redelivered");
                model.RoutingKey = evArg.RoutingKey;
                model.MessageID = evArg.BasicProperties.MessageId;
                model.EnqueueTime = evArg.BasicProperties.Timestamp.UnixTime > 0 && evArg.BasicProperties.Timestamp.UnixTime <= MAX_TIMESTAMP ?
                                         (DateTime?)TimeHelper.ToDateTime(evArg.BasicProperties.Timestamp.UnixTime) : null;
                int tmpCount;
                model.EnqueueCount = model.HeaderArguments != null && model.HeaderArguments.ContainsKey("EnqueueCount") && int.TryParse(model.HeaderArguments["EnqueueCount"] + "", out tmpCount) ?
                                          tmpCount : 1;
                model.AutoParseHeaderString();

            };

            #endregion

            BasicDeliverEventHandler receivedHandler = (sender, e) =>
            {
                T entity = null;
                string messageBody = Encoding.UTF8.GetString(e.Body);

                try
                {
                    #region 2.2 正常消费

                    dicConsumerContext[queueName].ReceiveCount++;

                    if (typeof(T) == typeof(RabbitBaseMessage))
                    {
                        entity = new T();
                    }
                    else
                    {
                        entity = JsonConvert.DeserializeObject<T>(messageBody);
                    }

                    analysizeHead(entity, e, messageBody);

                    consumeAction(entity);
                    dicConsumerContext[queueName].Channel.BasicAck(e.DeliveryTag, true);
                    //一次性Ack所有小于此deliveryTag的消息,对于某个Channel来说其DeliveryTag是连续且安全的

                    dicConsumerContext[queueName].AckCount++;

                    #endregion
                }
                catch (Exception ex)
                {
                    #region 2.3 消费异常

                    try
                    {
                        //当序列化失败时，entity为空
                        if (entity == null)
                        {
                            entity = new T();
                            analysizeHead(entity, e, messageBody);
                        }

                        var basicProp = e.BasicProperties;
                        basicProp.Headers["EnqueueCount"] = ++entity.EnqueueCount;
                        basicProp.Headers["Redelivered"] = true;

                        //根据消息头部的EnqueueCount来判断是重新入原队列还是入死信队列中
                        if (entity.EnqueueCount <= 10)
                        {
                            //使用公用可用的通道将消息重新投递到原队列中
                            ManageChannel.BasicPublish("", queueName, basicProp, Encoding.UTF8.GetBytes(entity.OrginalBody));

                            if (dicConsumerContext[queueName].Channel.IsOpen)
                            {
                                dicConsumerContext[queueName].Channel.BasicAck(e.DeliveryTag, true);
                            }
                        }
                        else
                        {
                            //自动转到对应的死信队列中
                            if (dicConsumerContext[queueName].Channel.IsOpen)
                            {
                                dicConsumerContext[queueName].Channel.BasicReject(e.DeliveryTag, false);
                            }

                            //this.Republish(queueName + ".dead", entity.OrginalBody, basicProp);
                        }
                    }
                    finally
                    {
                        dicConsumerContext[queueName].NAckOrFailedCount++;
                        Console.WriteLine("{0:yyyy-MM-dd HH:mm:ss}在订阅{1}时获取或处理消息异常:{2}\r\n{3}", DateTime.Now, queueName, ex.Message, ex.StackTrace);
                    }

                    #endregion
                }
            };

            consumer.Received += receivedHandler;
            dicConsumerContext[queueName].ConsumerTag = dicConsumerContext[queueName].Channel.BasicConsume(queueName, false, consumer);

            #endregion
        }


        /// <summary>
        /// 消费端主动拉取某个队列
        /// </summary>
        /// <typeparam name="T">该队列中存放的消息实体类型</typeparam>
        /// <param name="queueName">队列名称</param>
        /// <param name="consumeAction">消费消息的方法,当处理失败时请直接抛出异常</param>
        public void Pull<T>(string queueName, Action<T> consumeAction) where T : RabbitBaseMessage, new()
        {
            if (dicConsumerContext.ContainsKey(queueName))
            {
                throw new Exception("Only one queue can be consumed on the same client");
            }

            //先检查消息队列是否存在，不存在时直接抛出异常
            var channel = ManageChannel;
            try
            {
                var declareOk = channel.QueueDeclarePassive(queueName);
            }
            catch (OperationInterruptedException ex)
            {
                if (channel.IsClosed && channel.IsOpen)
                {
                    throw ex;
                }

            }

            if (!dicConsumerContext.TryAdd(queueName, new MQChannelContext()
            {
                Channel = consumeChannelCreateMethod.Invoke(),
                CanConsume = true,
            }))
            {
                return;
            }

            InitializeQos(queueName);

            Action pullAction = () =>
            {
                bool isBreak = false;
                bool isEmpty = false;

                while (dicConsumerContext[queueName].CanConsume)
                {
                    //Task.Factory.StartNew(() =>
                    //{
                    //});
                    PullHandle(queueName, consumeAction, ref isBreak, ref isEmpty);

                    if (isBreak)
                    {
                        break;
                    }
                    if (isEmpty)
                    {
                        Thread.SpinWait(50);
                    }
                    else
                    {
                        //Thread.Sleep(1);
                    }
                }
            };

            var mainTask = Task.Factory.StartNew(pullAction);
        }

        /// <summary>
        /// 消费队列单条消息的方法(以拉取方式)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="queueName"></param>
        /// <param name="consumeAction"></param>
        /// <param name="isBreak"></param>
        /// <param name="isEmpty"></param>
        private void PullHandle<T>(string queueName, Action<T> consumeAction, ref bool isBreak, ref bool isEmpty) where T : RabbitBaseMessage, new()
        {
            /*
            * 1.进行数据获取
            * 2.登记获取数量+1
            * 3.执行相应业务方法，并进行相应的Ack或NAck
            */
            BasicGetResult msgResult = null;

            #region 1.进行验证或前置准备

            try
            {
                //连接已关闭时通知主线程
                if (dicConsumerContext[queueName].Channel.IsClosed)
                {
                    isBreak = true;
                    return;
                }

                msgResult = dicConsumerContext[queueName].Channel.BasicGet(queueName, false);
                if (msgResult == null)
                {
                    isEmpty = true;
                    return;
                }
            }
            catch (Exception)
            {
                //异常时通知主线程中断
                isBreak = true;
                return;
            }

            #endregion

            #region 2.拉取消息后的消费事件相关

            #region 2.1 自动解析头部的方法

            //自动解析消息的头部信息
            Action<T, BasicGetResult, string> analysizeHead = (model, evArg, msgBody) =>
            {
                model.OrginalBody = msgBody;
                model.ExchangeName = evArg.Exchange;
                model.HeaderArguments = evArg.BasicProperties.Headers;
                model.Redelivered = evArg.Redelivered || evArg.BasicProperties.Headers != null && evArg.BasicProperties.Headers.ContainsKey("Redelivered");
                model.RoutingKey = evArg.RoutingKey;
                model.MessageID = evArg.BasicProperties.MessageId;
                model.EnqueueTime = evArg.BasicProperties.Timestamp.UnixTime > 0 && evArg.BasicProperties.Timestamp.UnixTime <= MAX_TIMESTAMP ?
                                         (DateTime?)TimeHelper.ToDateTime(evArg.BasicProperties.Timestamp.UnixTime) : null;
                int tmpCount;
                model.EnqueueCount = model.HeaderArguments != null && model.HeaderArguments.ContainsKey("EnqueueCount") && int.TryParse(model.HeaderArguments["EnqueueCount"] + "", out tmpCount) ?
                                          tmpCount : 1;
                model.AutoParseHeaderString();

            };

            #endregion

            T entity = null;
            string messageBody = Encoding.UTF8.GetString(msgResult.Body);

            try
            {
                #region 2.2 正常消费

                dicConsumerContext[queueName].ReceiveCount++;
                isEmpty = false;

                if (typeof(T) == typeof(RabbitBaseMessage))
                {
                    entity = new T();
                }
                else
                {
                    entity = JsonConvert.DeserializeObject<T>(messageBody);
                }

                analysizeHead(entity, msgResult, messageBody);

                consumeAction(entity);
                dicConsumerContext[queueName].Channel.BasicAck(msgResult.DeliveryTag, true);
                //拉取时线程是随机的，所以先Ack的DeliveryTag不一定是连续和安全的

                if (Monitor.TryEnter(lockConsumeObj, 500))
                {
                    dicConsumerContext[queueName].AckCount++;
                    Monitor.Exit(lockConsumeObj);
                }
                else
                {
                    dicConsumerContext[queueName].AckCount++;
                }

                #endregion
            }
            catch (Exception ex)
            {
                #region 2.3 消费异常

                try
                {
                    //当序列化失败时，entity为空
                    if (entity == null)
                    {
                        entity = new T();
                        analysizeHead(entity, msgResult, messageBody);
                    }

                    var basicProp = msgResult.BasicProperties;
                    basicProp.Headers["EnqueueCount"] = ++entity.EnqueueCount;
                    basicProp.Headers["Redelivered"] = true;

                    //根据消息头部的EnqueueCount来判断是重新入原队列还是入死信队列中
                    if (entity.EnqueueCount <= 10)
                    {
                        //使用公用可用的通道将消息重新投递到原队列中
                        ManageChannel.BasicPublish("", queueName, basicProp, Encoding.UTF8.GetBytes(entity.OrginalBody));

                        if (dicConsumerContext[queueName].Channel.IsOpen)
                        {
                            dicConsumerContext[queueName].Channel.BasicAck(msgResult.DeliveryTag, true);
                        }
                    }
                    else
                    {
                        //自动转到对应的死信队列中
                        if (dicConsumerContext[queueName].Channel.IsOpen)
                        {
                            dicConsumerContext[queueName].Channel.BasicReject(msgResult.DeliveryTag, false);
                        }

                        //this.Republish(queueName + ".dead", entity.OrginalBody, basicProp);
                    }

                }
                finally
                {
                    if (Monitor.TryEnter(lockConsumeObj, 500))
                    {
                        dicConsumerContext[queueName].NAckOrFailedCount++;
                        Monitor.Exit(lockConsumeObj);
                    }
                    else
                    {
                        dicConsumerContext[queueName].NAckOrFailedCount++;
                    }

                    Console.WriteLine("{0:yyyy-MM-dd HH:mm:ss}在拉取{1}并处理消息时发生异常:{2}\r\n{3}", DateTime.Now, queueName, ex.Message, ex.StackTrace);
                }

                #endregion
            }

            #endregion

        }


        /// <summary>
        /// 获取成功消费指定队列的消息总个数
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <returns></returns>
        public ulong GetConsumeSuccessCount(string queueName)
        {
            return dicConsumerContext.ContainsKey(queueName) ? dicConsumerContext[queueName].AckCount : 0;
        }

        /// <summary>
        /// 获取成功消费指定队列的消息总个数
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <returns></returns>
        public ulong GetConsumeFailedCount(string queueName)
        {
            return dicConsumerContext.ContainsKey(queueName) ? dicConsumerContext[queueName].NAckOrFailedCount : 0;
        }

        /// <summary>
        /// 消费端停止消费某个队列,此方法为确保每个消费的消息都被成功Ack或Nack因此可能会阻塞
        /// </summary>
        /// <param name="queueName">队列名称</param>
        public void StopConsume(string queueName)
        {
            //关闭对应的Channel
            Action stopAndClose = () =>
            {
                var currentContext = dicConsumerContext[queueName];
                if (currentContext.Channel.IsClosed) return;

                Console.WriteLine("{0:hh:mm:ss.fff}:[线程{1}_队列{2}_CT{3}],开始取消消费。总接收({4})=Ack({5})+NAck({6})",
                                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, queueName, currentContext.ConsumerTag,
                                    currentContext.ReceiveCount, currentContext.AckCount, currentContext.NAckOrFailedCount);

                Stopwatch swatch = Stopwatch.StartNew();

                int validTimes = 0; //相等的情况下致少验证3次。以保证每个消息都能 Ack或NACK
                while (currentContext.ReceiveCount > currentContext.AckCount + currentContext.NAckOrFailedCount || validTimes < 3)
                {
                    if (currentContext.ReceiveCount > currentContext.AckCount + currentContext.NAckOrFailedCount)
                    {
                        Thread.Sleep(500);
                        validTimes = 0;
                    }
                    else
                    {
                        Thread.Sleep(300);
                        validTimes++;
                    }
                }

                if (!currentContext.Channel.IsClosed)
                {
                    currentContext.Channel.Close();
                }

                Console.WriteLine("{0:hh:mm:ss.fff}:[线程{1}_队列{2}_CT{3}],从取消到关闭连接共花费{7:f2}秒。总接收({4})=Ack({5})+NAck({6})",
                                    DateTime.Now, Thread.CurrentThread.ManagedThreadId, queueName, currentContext.ConsumerTag,
                                    currentContext.ReceiveCount, currentContext.AckCount, currentContext.NAckOrFailedCount,
                                   Convert.ToSingle(swatch.ElapsedMilliseconds) / 1000.0f);
            };

            if (dicConsumerContext.ContainsKey(queueName) && dicConsumerContext[queueName].CanConsume)
            {
                if (!string.IsNullOrEmpty(dicConsumerContext[queueName].ConsumerTag))
                {
                    //有消费端标记的,表示异步推的
                    dicConsumerContext[queueName].Channel.BasicCancel(dicConsumerContext[queueName].ConsumerTag);
                    stopAndClose.Invoke();
                    dicConsumerContext[queueName].CanConsume = false;
                }
                else
                {
                    dicConsumerContext[queueName].CanConsume = false;
                    stopAndClose.Invoke();
                }
            }
        }

        #endregion

        #region 实现IDisposable的成员


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
                    /*
                     * 1.检查消费端的上下文状态是否为可消费的状态，如是调用相关方法停止消费
                     * 2.检查消费端的Channel是否还处于非状态的状态，如是则继续等待
                     */

                    foreach (var item in dicConsumerContext)
                    {
                        if (item.Value.CanConsume)
                        {
                            StopConsume(item.Key);
                        }
                    }

                    //必须等待所有消费端的Channel全部关闭完
                    Task t = Task.Factory.StartNew(() =>
                    {
                        bool ackCompleted = true;
                        do
                        {
                            ackCompleted = true;

                            foreach (var item in dicConsumerContext)
                            {
                                if (!item.Value.Channel.IsClosed)
                                {
                                    ackCompleted = false;

                                    Console.WriteLine("ERROR:{0:hh:mm:ss.fff}:[线程{1}_队列{2}_CT{3}],在Dispose时发现还未关闭连接,故需等待。",
                                        DateTime.Now, Thread.CurrentThread.ManagedThreadId, item.Key, item.Value.ConsumerTag);

                                    break;
                                }
                            }

                            if (!ackCompleted)
                            {
                                Thread.Sleep(300);
                            }

                        } while (!ackCompleted);

                    });

                    t.Wait();

                    //释放所有的Channel
                    foreach (var item in dicConsumerContext)
                    {
                        item.Value.Channel.Dispose();
                    }
                }

                //释放非托管对象，如将大型字段设置为 null。
                hasDisposed = true;
            }
        }

        /// <summary>
        /// 析构函数,只释放非托管资源
        /// </summary>
        ~RabbitMQConsumer()
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
