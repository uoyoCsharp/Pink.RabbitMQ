using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace Pink.RabbitMQ
{
    /// <summary>
    /// 用于交换机、队列的RabbitMQ的管理者
    /// </summary>
    internal class RabbitMQManager : IMQManager
    {
        #region FIELDS

        private const long MAX_TIMESTAMP = 253402214400000;

        public const string DEFAULT_EXCHANGE_NAME = "";//"(AMQP default)";

        #endregion

        #region 构造函数

        private Func<IModel> channelGetMethod;


        private HashSet<string> setExistsQueue = null;

        /// <summary>
        /// 获取管理用的通道的表达式进行初始化
        /// </summary>
        /// <param name="manageChannelGetExp">管理MQ所用通道获取的方法表达式</param>
        /// <param name="existsQueue">已存在的队列名称</param>
        public RabbitMQManager(Expression<Func<IModel>> manageChannelGetExp, HashSet<string> existsQueue)
        {
            channelGetMethod = manageChannelGetExp.Compile();
            setExistsQueue = existsQueue;
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
                return channelGetMethod.Invoke();
            }
        }

        #endregion

        #region 实现IMQManager的成员

        #region Exchange交换机定义、绑定、解绑

        /// <summary>
        /// 定义交换机
        /// </summary>
        /// <param name="name">交换机名称</param>
        /// <param name="type">交换机类型</param>
        /// <param name="alternateExchangeName">当消息投递不成功时的转发到的备用交换机名称(由此交换机进行转投)</param>
        /// <param name="arguments">其它参数</param>
        public void ExchangeDeclare(string name, RabbitExchangeType type, string alternateExchangeName = null, IDictionary<string, object> arguments = null)
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentNullException(nameof(name));
            }
            if (!string.IsNullOrEmpty(alternateExchangeName))
            {
                if (arguments == null)
                {
                    arguments = new Dictionary<string, object>();
                }
                string altKey = "alternate-exchange";
                arguments[altKey] = alternateExchangeName;
            }

            ManageChannel.ExchangeDeclare(name, type.ToString().ToLower(), true, false, arguments);
        }

        /// <summary>
        /// 将队列绑定到指定的交换机上，适用于direct、fanout、topic类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="routeKey">路由Key</param>
        public void QueueBind(string exchangeName, string queueName, string routeKey)
        {
            if (string.IsNullOrEmpty(exchangeName))
                throw new ArgumentNullException(nameof(exchangeName));

            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException(nameof(queueName));

            if (routeKey == null)
                throw new ArgumentNullException(nameof(routeKey));

            ManageChannel.QueueBind(queueName, exchangeName, routeKey, null);

        }

        /// <summary>
        /// 将队列绑定到指定的交换机上，适用于headers类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="headerArguments"></param>
        public void QueueBind(string exchangeName, string queueName, IDictionary<string, object> headerArguments)
        {
            if (string.IsNullOrEmpty(exchangeName))
                throw new ArgumentNullException(nameof(exchangeName));

            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException(nameof(queueName));

            ManageChannel.QueueBind(queueName, exchangeName, "", headerArguments);

        }

        /// <summary>
        /// 队列从指定的交换机上解绑，适用于direct、fanout、topic类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="routeKey">路由Key</param>
        public void QueueUnbind(string exchangeName, string queueName, string routeKey)
        {
            if (string.IsNullOrEmpty(exchangeName))
                throw new ArgumentNullException(nameof(exchangeName));

            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException(nameof(queueName));

            if (routeKey == null)
                throw new ArgumentNullException(nameof(routeKey));

            ManageChannel.QueueUnbind(queueName, exchangeName, routeKey, null);
        }

        /// <summary>
        /// 队列从指定的交换机上解绑，适用于headers类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="headerArguments"></param>
        public void QueueUnbind(string exchangeName, string queueName, IDictionary<string, object> headerArguments)
        {
            if (string.IsNullOrEmpty(exchangeName))
                throw new ArgumentNullException(nameof(exchangeName));

            if (string.IsNullOrEmpty(queueName))
                throw new ArgumentNullException(nameof(queueName));


            ManageChannel.QueueUnbind(queueName, exchangeName, "", headerArguments);

        }

        /// <summary>
        /// 删除指定的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="ifUnused">只有当未使用时才进行删除</param>
        public void ExchangeDelete(string exchangeName, bool ifUnused = true)
        {
            ManageChannel.ExchangeDelete(exchangeName, ifUnused);
        }

        #endregion

        #region Queue定义、删除、清空

        /// <summary>
        /// 定义消息队列
        /// </summary>
        /// <param name="queueName">队列名称</param>
        public void QueueDeclare(string queueName)
        {
            IDictionary<string, object> dic = new Dictionary<string, object>();
            //72小时自动过期，并转给对应的死信对列
            dic.Add("x-message-ttl", 3 * 24 * 60 * 60 * 1000);
            //dic.Add("x-message-ttl", 60 * 1000);
            dic.Add("x-dead-letter-exchange", DEFAULT_EXCHANGE_NAME);
            dic.Add("x-dead-letter-routing-key", queueName + ".dead");

            ManageChannel.QueueDeclare(queueName, true, false, false, dic);
            //同时创建同名的转门接收死信的消息队列
            ManageChannel.QueueDeclare(queueName + ".dead", true, false, false, new Dictionary<string, object>());

            if (!setExistsQueue.Contains(queueName))
            {
                setExistsQueue.Add(queueName);
            }
        }

        /// <summary>
        /// 定义消息队列
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="arguments">队列的其它定义参数</param>
        public void QueueDeclare(string queueName, RabbitQueueArgrmentContext arguments)
        {
            var notExists = false;
            try
            {
                //当没有定义时，会抛出异常，才执行相应的定义
                var declareOk = ManageChannel.QueueDeclarePassive(queueName);

            }
            catch (OperationInterruptedException ex)
            {
                if (ex.Message.ToUpper().Contains("NOT_FOUND"))
                {
                    notExists = true;
                }
            }

            if (!notExists) return;

            IDictionary<string, object> dic = new Dictionary<string, object>();
            if (arguments.XExpireSeconds > 0)
            {
                dic.Add("x-expires", Convert.ToInt64(arguments.XExpireSeconds.Value * 1000));
            }
            if (arguments.XMessageTTLSeconds > 0)
            {
                dic.Add("x-message-ttl", Convert.ToInt64(arguments.XMessageTTLSeconds.Value * 1000));
            }
            else
            {
                //未设置时默认为72小时过期
                dic.Add("x-message-ttl", 3 * 24 * 60 * 60 * 1000);
            }
            if (arguments.MaxLength > 0)
            {
                dic.Add("x-max-length", Convert.ToInt64(arguments.MaxLength.Value));
            }
            if (arguments.MaxBytes > 0)
            {
                dic.Add("x-max-length-bytes", Convert.ToInt64(arguments.MaxBytes.Value));
            }
            if (arguments.OverflowBehaviour > 0 && arguments.OverflowBehaviour < 3)
            {
                dic.Add("x-overflow", arguments.OverflowBehaviour == 1 ? "drop-head" : "reject-publish");
            }

            //指定给默认的死信队列
            if (arguments.DeadLetterRepublishRule == 1)
            {
                dic.Add("x-dead-letter-exchange", DEFAULT_EXCHANGE_NAME);
                dic.Add("x-dead-letter-routing-key", queueName + ".dead");
            }
            else if (arguments.DeadLetterRepublishRule == 2)
            {
                if (!string.IsNullOrEmpty(arguments.DeadLetterExchangeName))
                {
                    dic.Add("x-dead-letter-exchange", arguments.DeadLetterExchangeName);
                }

                if (!string.IsNullOrEmpty(arguments.DeadLetterRoutingKey))
                {
                    dic.Add("x-dead-letter-routing-key", arguments.DeadLetterRoutingKey);
                }
            }

            ManageChannel.QueueDeclare(queueName, true, false, false, dic);
            //同时创建同名的转门接收死信的消息队列
            ManageChannel.QueueDeclare(queueName + ".dead", true, false, false, new Dictionary<string, object>());

            if (!setExistsQueue.Contains(queueName))
            {
                setExistsQueue.Add(queueName);
            }
        }

        /// <summary>
        /// 清空队列中所有消息
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="purgeDeadQueue">同时清空对应的死信对列中的消息</param>
        /// <returns>清空的消息数,负1表示该对列不存在</returns>
        public long QueuePurge(string queueName, bool purgeDeadQueue = false)
        {
            long rtnValue = -1;
            try
            {
                rtnValue = Convert.ToInt64(ManageChannel.QueuePurge(queueName));
                if (purgeDeadQueue)
                {
                    ManageChannel.QueuePurge(queueName + ".dead");
                }
                return rtnValue;
            }
            catch (OperationInterruptedException)
            {
                return rtnValue;
            }
        }

        /// <summary>
        /// 删除指定的队列
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="deleteDeadQueue">同时删除对应的死信对列</param>
        /// <param name="ifUnused">只有未使用时才进行删除</param>
        /// <param name="ifEmpty">只有为空时才进行删除</param>
        /// <returns>队列删除时的消息数</returns>
        public long QueueDelete(string queueName, bool deleteDeadQueue = false, bool ifUnused = false, bool ifEmpty = false)
        {
            long rtnValue = -1;
            try
            {
                rtnValue = Convert.ToInt64(ManageChannel.QueueDelete(queueName, ifUnused, ifEmpty));

                if (rtnValue >= 0 && setExistsQueue.Contains(queueName))
                {
                    setExistsQueue.Remove(queueName);
                }

                if (deleteDeadQueue)
                {
                    ManageChannel.QueueDelete(queueName + ".dead");
                }
                return rtnValue;
            }
            catch (OperationInterruptedException)
            {
                return rtnValue;
            }
        }
        #endregion

        #endregion
    }
}
