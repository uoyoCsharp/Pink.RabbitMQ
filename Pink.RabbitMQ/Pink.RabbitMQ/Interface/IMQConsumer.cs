using System;

namespace Pink.RabbitMQ
{
    /// <summary>
    /// MQ队列的消费端，支持订阅推送方式和主动获取方式
    /// </summary>
    public interface IMQConsumer:IDisposable
    {
        /// <summary>
        /// 消费端订阅某个队列,采取推送的方式
        /// </summary>
        /// <typeparam name="T">该队列中存放的消息实体类型</typeparam>
        /// <param name="queueName">队列名称</param>
        /// <param name="consumeAction">消费消息的方法,当处理失败时请直接抛出异常</param>
        void Subscription<T>(string queueName, Action<T> consumeAction) where T : RabbitBaseMessage, new();


        /// <summary>
        /// 消费端主动拉取某个队列
        /// </summary>
        /// <typeparam name="T">该队列中存放的消息实体类型</typeparam>
        /// <param name="queueName">队列名称</param>
        /// <param name="consumeAction">消费消息的方法,当处理失败时请直接抛出异常</param>
        void Pull<T>(string queueName, Action<T> consumeAction) where T : RabbitBaseMessage, new();


        /// <summary>
        /// 消费端停止消费某个队列,此方法为确保每个消费的消息都被成功Ack或Nack因此可能会阻塞
        /// </summary>
        /// <param name="queueName">队列名称</param>
        void StopConsume(string queueName);

        /// <summary>
        /// 获取成功消费指定队列的消息总个数
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <returns></returns>
        UInt64 GetConsumeSuccessCount(string queueName);

        /// <summary>
        /// 获取成功消费指定队列的消息总个数
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <returns></returns>
        UInt64 GetConsumeFailedCount(string queueName);
    }
}
