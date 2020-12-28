namespace Pink.RabbitMQ
{
    /// <summary>
    /// MQ事务中可使用的上下文
    /// </summary>
    public class MQTransactionContent
    {
        /// <summary>
        /// MQ管理端对象
        /// </summary>
        public IMQManager ManagerInstance { get; private set; }

        /// <summary>
        /// MQ生产端对象
        /// </summary>
        public IMQPublisher PublisherInstance { get; private set; }

        /// <summary>
        /// 对管理端和生产端进行初始化
        /// </summary>
        /// <param name="manager"></param>
        /// <param name="publisher"></param>
        internal MQTransactionContent(IMQManager manager, IMQPublisher publisher)
        {
            this.ManagerInstance = manager;
            this.PublisherInstance = publisher;
        }
    }
}
