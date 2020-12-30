namespace Pink.RabbitMQ
{
    /// <summary>
    /// RabbitMQ中交换机的类型
    /// </summary>
    public enum RabbitExchangeType
    {
        Fanout = 1,

        Direct = 2,

        Topic = 3,

        Headers = 4,
    }
}
