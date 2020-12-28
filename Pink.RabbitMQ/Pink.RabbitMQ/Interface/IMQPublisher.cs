/*
 * 
 * Encoding:UTF-8
 * Version: 1.0
 * Create Date:  2019-08-20
 * Author: Richie
 * Description: MQ的生产者
 *           
 * Modify Date: 
 * Modifier: 
 * Description: 
*/
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;

namespace Pink.RabbitMQ
{
    /// <summary>
    /// MQ消息的生产端
    /// </summary>
    public interface IMQPublisher:IDisposable
    {

        /// <summary>
        /// 向指定的队列发送消息，队列不存在时自动创建
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="message">消息内容</param>
        /// <param name="basicProp">消息相应发送时相应的属性</param>
        void Publish(string queueName, string message, IBasicProperties basicProp);

        /// <summary>
        /// 向指定的队列发送消息，队列不存在时自动创建
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="message">消息内容</param>
        /// <param name="persistent">该消息是否持久化</param>
        void Publish(string queueName, string message, bool persistent = true);


        /// <summary>
        /// 将实体对象向指定的队列进行发送，队列不存在时自动创建
        /// </summary>
        /// <typeparam name="T">消息实体的类型</typeparam>
        /// <param name="queueName">队列名称</param>
        /// <param name="entity">实体对象</param>
        /// <param name="persistent">该消息是否持久化</param>
        void Publish<T>(string queueName, T entity, bool persistent = true) where T : class;


        /// <summary>
        /// 通过交换机和路由Key发送消息，适用于类型为Direct/Fanout/Topic类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="routingKey">路由Key</param>
        /// <param name="message">消息内容</param>
        /// <param name="persistent">该消息是否持久化</param>
        void Publish(string exchangeName, string routingKey, string message, bool persistent = true);

        /// <summary>
        /// 通过交换机和路由Key发送实体对象,适用于类型为Direct/Fanout/Topic类型的交换机
        /// </summary>
        /// <typeparam name="T">消息实体的类型</typeparam>
        ///<param name="exchangeName">交换机名称</param>
        /// <param name="routingKey">路由Key</param>
        /// <param name="entity">实体对象</param>
        /// <param name="persistent">该消息是否持久化</param>
        void Publish<T>(string exchangeName, string routingKey, T entity, bool persistent = true) where T : class;


        /// <summary>
        /// 通过交换机和Header头部发送消息，适用于类型为Headers/Fanout类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="headerArguments">定义的头部参数，Value的类型和值必须与绑定时相同才能匹配</param>
        /// <param name="message">消息内容</param>
        /// <param name="persistent">该消息是否持久化</param>
        /// <param name="routingKey">可选的路由Key，对于Headers/Fanout交换机来说此参数无意义</param>
        void Publish(string exchangeName, IDictionary<string, object> headerArguments, string message, bool persistent = true, string routingKey = "");


        /// <summary>
        /// 通过交换机和Header头部发送实体对象，适用于类型为Headers/Fanout类型的交换机
        /// </summary>
        /// <typeparam name="T">消息实体的类型</typeparam>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="headerArguments">定义的头部参数，Value的类型和值必须与绑定时相同才能匹配</param>
        /// <param name="entity">实体对象</param>
        /// <param name="persistent">该消息是否持久化</param>
        /// <param name="routingKey">可选的路由Key，对于Headers/Fanout交换机来说此参数无意义</param>
        void Publish<T>(string exchangeName, IDictionary<string, object> headerArguments, T entity, bool persistent = true, string routingKey = "") where T : class;


        /// <summary>
        /// 获取下一个发送消息的序号,当使用Confirm模式时有效
        /// </summary>
        ulong NextPublishSeqNo { get; }

    }

    /// <summary>
    /// 发送消息后已Confrim的事件参数
    /// </summary>
    public class PublishAckArgs : BasicAckEventArgs
    {
        internal PublishAckArgs(ulong publishedSeqNo, ulong deliveryTag, bool multiple)
        {
            this.PublishedSeqNo = publishedSeqNo;
            this.DeliveryTag = deliveryTag;
            this.Multiple = multiple;
        }

        /// <summary>
        /// 获取已发送消息的序号
        /// </summary>
        public ulong PublishedSeqNo { get; private set; }
    }

    /// <summary>
    /// 发送消息后Confirm失败的事件参数
    /// </summary>
    public class PublishNAckArgs : BasicNackEventArgs
    {
        internal PublishNAckArgs(ulong publishedSeqNo, ulong deliveryTag, bool multiple, bool requeue)
        {
            this.PublishedSeqNo = publishedSeqNo;
            this.DeliveryTag = deliveryTag;
            this.Multiple = multiple;
            this.Requeue = requeue;
        }

        /// <summary>
        /// 获取已发送消息的序号
        /// </summary>
        public ulong PublishedSeqNo { get; private set; }
    }
}
