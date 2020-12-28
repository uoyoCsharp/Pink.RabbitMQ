/*
 * 
 * Encoding:UTF-8
 * Version: 1.0
 * Create Date:  2019-08-20
 * Author: Richie
 * Description: RabbitMQ中所用的枚举类定义
 *           
 * Modify Date: 
 * Modifier: 
 * Description: 
*/

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
