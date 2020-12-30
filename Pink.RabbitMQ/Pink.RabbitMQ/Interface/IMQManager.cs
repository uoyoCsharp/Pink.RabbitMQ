using System.Collections.Generic;

namespace Pink.RabbitMQ
{
    /// <summary>
    /// MQ队列的定义、管理、维护等接口
    /// </summary>
    public interface IMQManager
    {
        #region 交换机定义/绑定/解绑/删除

        /// <summary>
        /// 定义交换机
        /// </summary>
        /// <param name="name">交换机名称</param>
        /// <param name="type">交换机类型</param>
        /// <param name="alternateExchangeName">当消息投递不成功时的转发到的备用交换机名称(由此交换机进行转投)</param>
        /// <param name="arguments">其它参数</param>
        /// <remarks>arguments意义不太大</remarks>
        void ExchangeDeclare(string name, RabbitExchangeType type, string alternateExchangeName = null, IDictionary<string, object> arguments = null);

        /// <summary>
        /// 将队列绑定到指定的交换机上，适用于direct、fanout、topic类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="routeKey">路由Key</param>
        void QueueBind(string exchangeName, string queueName, string routeKey);

        /// <summary>
        /// 将队列绑定到指定的交换机上，适用于headers类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="headerArguments"></param>
        void QueueBind(string exchangeName, string queueName, IDictionary<string, object> headerArguments);

        /// <summary>
        /// 队列从指定的交换机上解绑，适用于direct、fanout、topic类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="routeKey">路由Key</param>
        void QueueUnbind(string exchangeName, string queueName, string routeKey);

        /// <summary>
        /// 队列从指定的交换机上解绑，适用于headers类型的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="headerArguments"></param>
        void QueueUnbind(string exchangeName, string queueName, IDictionary<string, object> headerArguments);

        /// <summary>
        /// 删除指定的交换机
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="ifUnused">只有当未使用时才进行删除</param>
        void ExchangeDelete(string exchangeName, bool ifUnused = true);


        #endregion

        #region Queue定义、删除、清空

        /// <summary>
        /// 定义消息队列
        /// </summary>
        /// <param name="queueName">队列名称</param>
        void QueueDeclare(string queueName);


        /// <summary>
        /// 定义消息队列
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="arguments">队列的其它定义参数</param>
        void QueueDeclare(string queueName, RabbitQueueArgrmentContext arguments);


        /// <summary>
        /// 清空队列中所有消息
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="purgeDeadQueue">同时清空对应的死信对列中的消息</param>
        /// <returns>清空的消息数,负1表示该对列不存在</returns>
        long QueuePurge(string queueName, bool purgeDeadQueue = false);

        /// <summary>
        /// 删除指定的队列
        /// </summary>
        /// <param name="queueName">队列名称</param>
        /// <param name="deleteDeadQueue">同时删除对应的死信对列</param>
        /// <param name="ifUnused">只有未使用时才进行删除</param>
        /// <param name="ifEmpty">只有为空时才进行删除</param>
        /// <returns>队列删除时的消息数</returns>
        long QueueDelete(string queueName, bool deleteDeadQueue = false, bool ifUnused = false, bool ifEmpty = false);

        #endregion
    }

}
