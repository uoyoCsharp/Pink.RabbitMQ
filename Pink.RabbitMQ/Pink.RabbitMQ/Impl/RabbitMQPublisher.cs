/*
 * 
 * Encoding:UTF-8
 * Version: 1.0
 * Create Date:  2019-08-20
 * Author: Richie
 * Description: 适用于RabbitMQ的消息生产端
 *           
 * Modify Date: 
 * Modifier: 
 * Description: 
*/

using Newtonsoft.Json;
using Pink.RabbitMQ.Helper;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace Pink.RabbitMQ
{
    /// <summary>
    /// 用于发送消息的RabbitMQ的生产者
    /// </summary>
    internal class RabbitMQPublisher : IMQPublisher
    {
        #region 构造函数

        private Func<IModel> channelGetMethod;

        private Action<string> autoDeclareQueueMethod;

        private HashSet<string> setExistsQueue = null;

        /// <summary>
        /// 获取发送消息时通道的表达式进行初始化
        /// </summary>
        /// <param name="pubishChannelGetExp">发布消息通道获取的方法表达式</param>
        /// <param name="existsQueue">已存在的队列名称</param>
        /// <param name="autoDeclareQueueExp">自动注册队列的方法</param>
        public RabbitMQPublisher(Expression<Func<IModel>> pubishChannelGetExp, HashSet<string> existsQueue, Action<string> autoDeclareQueueExp)
        {
            channelGetMethod = pubishChannelGetExp.Compile();
            autoDeclareQueueMethod = autoDeclareQueueExp;
            setExistsQueue = existsQueue;
        }
        #endregion

        #region 私有成员

        /// <summary>
        /// 获取发布消息的专用通道
        /// </summary>
        private IModel PublishChannel
        {
            get
            {
                return channelGetMethod.Invoke();
            }
        }

        /// <summary>
        /// 创建消息的基础属性
        /// </summary>
        /// <param name="persistent">是否持久久</param>
        /// <param name="currentChannel">当前可用的通道</param>
        /// <returns></returns>
        private IBasicProperties CreateBaseProperties(bool persistent)
        {
            var newProperties = PublishChannel.CreateBasicProperties();
            newProperties.ContentType = "text/plain";
            newProperties.DeliveryMode = (byte)(persistent ? 2 : 1);
            newProperties.MessageId = Guid.NewGuid().ToString();
            newProperties.Timestamp = new AmqpTimestamp(TimeHelper.ToTimestamp(DateTime.Now));

            //记录入队的次数
            newProperties.Headers = new Dictionary<string, object>() { ["EnqueueCount"] = 1 };

            return newProperties;
        }

        #endregion

        #region 实现IMQPublisher的成员


        /// <summary>
        /// 向指定的队列发送消息，队列不存在时自动创建
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="message">消息内容</param>
        /// <param name="basicProp">消息相应发送时相应的属性</param>
        public void Publish(string queueName, string message, IBasicProperties basicProp)
        {
            //mandatory true:当exchane根据类型和routeKey未匹配到queue时，会调用和触发channel.BasicReturn将消息返还给生产者 false:直接将消息扔掉
            //immediate 已废弃

            if (!setExistsQueue.Contains(queueName))
            {
                //当队列不存在时，自动定义队列信息和对应的死信对列
                autoDeclareQueueMethod.Invoke(queueName);
                setExistsQueue.Add(queueName);
            }

            PublishChannel.BasicPublish("", queueName, basicProp, Encoding.UTF8.GetBytes(message));
        }

        /// <summary>
        /// 向指定的队列发送消息，队列不存在时自动创建
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="message">消息内容</param>
        /// <param name="persistent">该消息是否持久化</param>
        public void Publish(string queueName, string message, bool persistent = true)
        {
            var basicProp = CreateBaseProperties(persistent);
            Publish(queueName, message, basicProp);
        }

        /// <summary>
        /// 将实体对象向指定的队列进行发送，队列不存在时自动创建
        /// </summary>
        /// <typeparam name="T">消息实体的类型</typeparam>
        /// <param name="queueName">队列名称</param>
        /// <param name="entity">实体对象</param>
        /// <param name="persistent">该消息是否持久化</param>
        public void Publish<T>(string queueName, T entity, bool persistent = true) where T : class
        {
            Publish(queueName, JsonConvert.SerializeObject(entity), persistent);
        }


        /// <summary>
        /// 通过交换机和路由Key发送消息，适用于类型为Direct/Fanout/Topic类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="routingKey">路由Key</param>
        /// <param name="message">消息内容</param>
        /// <param name="persistent">该消息是否持久化</param>
        public void Publish(string exchangeName, string routingKey, string message, bool persistent = true)
        {
            PublishChannel.BasicPublish(exchangeName, string.IsNullOrEmpty(routingKey) ? "" : routingKey, CreateBaseProperties(persistent), Encoding.UTF8.GetBytes(message));
        }

        /// <summary>
        /// 通过交换机和路由Key发送实体对象,适用于类型为Direct/Fanout/Topic类型的交换机
        /// </summary>
        /// <typeparam name="T">消息实体的类型</typeparam>
        ///<param name="exchangeName">交换机名称</param>
        /// <param name="routingKey">路由Key</param>
        /// <param name="entity">实体对象</param>
        /// <param name="persistent">该消息是否持久化</param>
        public void Publish<T>(string exchangeName, string routingKey, T entity, bool persistent = true) where T : class
        {
            Publish(exchangeName, routingKey, JsonConvert.SerializeObject(entity), persistent);
        }

        /// <summary>
        /// 通过交换机和Header头部发送消息，适用于类型为Headers/Fanout类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="headerArguments">定义的头部参数，Value的类型和值必须与绑定时相同才能匹配</param>
        /// <param name="message">消息内容</param>
        /// <param name="persistent">该消息是否持久化</param>
        /// <param name="routingKey">可选的路由Key，对于Headers/Fanout交换机来说此参数无意义</param>
        public void Publish(string exchangeName, IDictionary<string, object> headerArguments, string message, bool persistent = true, string routingKey = "")
        {
            var properties = CreateBaseProperties(persistent);
            if (headerArguments != null)
            {
                foreach (var item in headerArguments)
                {
                    properties.Headers.Add(item.Key, item.Value);
                }
            }

            PublishChannel.BasicPublish(exchangeName, string.IsNullOrEmpty(routingKey) ? "" : routingKey, properties, Encoding.UTF8.GetBytes(message));
        }

        /// <summary>
        /// 通过交换机和Header头部发送实体对象，适用于类型为Headers/Fanout类型的交换机
        /// </summary>
        /// <typeparam name="T">消息实体的类型</typeparam>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="headerArguments">定义的头部参数，Value的类型和值必须与绑定时相同才能匹配</param>
        /// <param name="entity">实体对象</param>
        /// <param name="persistent">该消息是否持久化</param>
        /// <param name="routingKey">可选的路由Key，对于Headers/Fanout交换机来说此参数无意义</param>
        public void Publish<T>(string exchangeName, IDictionary<string, object> headerArguments, T entity, bool persistent = true, string routingKey = "") where T : class
        {
            Publish(exchangeName, headerArguments, JsonConvert.SerializeObject(entity), persistent, routingKey);
        }

        /// <summary>
        /// 获取下一个发送消息的序号,当使用Confirm模式时有效
        /// </summary>
        public ulong NextPublishSeqNo
        {
            get { return PublishChannel.NextPublishSeqNo; }
        }

        /// <summary>
        /// 关闭并释放通道
        /// </summary>
        public void Dispose()
        {
            if (PublishChannel.IsOpen)
            {
                PublishChannel.Close();
            }
            PublishChannel.Dispose();
        }

        #endregion

    }
}
