/*
 * 
 * Encoding:UTF-8
 * Version: 1.0
 * Create Date:  2019-08-20
 * Author: Richie
 * Description: RabbitMQ中定义的队列参数
 *           
 * Modify Date: 
 * Modifier: 
 * Description: 
*/

namespace Pink.RabbitMQ
{
    /// <summary>
    /// Rabbit队列定义时使用的参数
    /// </summary>
    public class RabbitQueueArgrmentContext
    {
        /// <summary>
        /// 队列自动过期的时长，单位为秒。自动过期指在此时间段内，队列都没有订阅者,则该队列将自动被删除
        /// </summary>
        public uint? XExpireSeconds { get; set; }

        /// <summary>
        /// 队列中消息默认的过期时长，单个为秒。消息默认过期时长，指消息在指定时段内没有被消费时，将会从队列中自动删除或移动到死信队列中
        /// </summary>
        public uint? XMessageTTLSeconds { get; set; }

        /// <summary>
        /// 队列中可存储的最大消息数
        /// </summary>
        public uint? MaxLength { get; set; }

        /// <summary>
        /// 队列中可存储的消息总字节数
        /// </summary>
        public ulong? MaxBytes { get; set; }

        /// <summary>
        /// 当队列中消息溢出(超过Length或Bytes时)采取的行为
        /// 1-DropHead删除头部的消息
        /// 2-RejectPublish拒绝接收
        /// </summary>
        public ushort? OverflowBehaviour { get; set; }

        /// <summary>
        /// 死信处理的方式：
        /// 1-自动转发到匹配的的死信队列中，死信队列名为xxx.dead
        /// 2-根据自定义的交换机和路由键来进行转发
        /// </summary>
        internal ushort DeadLetterRepublishRule { get; private set; } = 1;

        /// <summary>
        /// 当消息过期或被拒绝时重新发布到的交换机名称
        /// </summary>
        internal string DeadLetterExchangeName { get; private set; }


        /// <summary>
        /// 当消息过期或被拒绝时重新发布到交换机时使用的路由键，未设置时使用原消息的RoutingKey
        /// </summary>
        internal string DeadLetterRoutingKey { get; private set; }

        /// <summary>
        /// 死信自动转发到同名的(xxx.dead)的队列中
        /// </summary>
        /// <param name="republishToDeadQueue">是否转到对应的同名队列(xxx.dead)中</param>
        public void SetDeadLetterRepublish(bool republishToDeadQueue)
        {
            if (republishToDeadQueue)
            {
                this.DeadLetterRepublishRule = 1;
            }
        }

        /// <summary>
        /// 死信根据设定的交换机和路由键来进行再转发
        /// </summary>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="routingKey">路由键,未传入时依然使用原Message的路由键</param>
        public void SetDeadLetterRepublish(string exchangeName, string routingKey = null)
        {
            if (!string.IsNullOrEmpty(exchangeName))
            {
                this.DeadLetterRepublishRule = 2;
                this.DeadLetterExchangeName = exchangeName;
                this.DeadLetterRoutingKey = routingKey;
            }
        }

    }
}
