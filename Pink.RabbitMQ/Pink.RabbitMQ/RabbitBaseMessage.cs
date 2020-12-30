using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Pink.RabbitMQ
{
    /// <summary>
    /// Rabbitb出队消息的基础信息
    /// </summary>
    public class RabbitBaseMessage
    {
        /// <summary>
        /// 交换机名称
        /// </summary>
        [JsonIgnore]
        public string ExchangeName { get; internal set; }

        /// <summary>
        /// 路由Key
        /// </summary>
        [JsonIgnore]
        public string RoutingKey { get; internal set; }

        /// <summary>
        /// 是否再次投递的消息
        /// </summary>
        [JsonIgnore]
        public bool Redelivered { get; internal set; }

        /// <summary>
        /// 消息的唯一ID
        /// </summary>
        [JsonIgnore]
        public string MessageID { get; internal set; }

        /// <summary>
        /// 最初入队的时间
        /// </summary>
        [JsonIgnore]
        public DateTime? EnqueueTime { get; internal set; }

        /// <summary>
        /// 累计进入该队列的次数
        /// </summary>
        [JsonIgnore]
        public int EnqueueCount { get; internal set; }

        /// <summary>
        /// 头部参数
        /// </summary>
        [JsonIgnore]
        public IDictionary<string, object> HeaderArguments { get; internal set; }

        /// <summary>
        /// 原始消息内容
        /// </summary>
        [JsonIgnore]
        public string OrginalBody { get; internal set; }

        /// <summary>
        /// 自动解析Header头部中Base64加密的Value项
        /// </summary>
        internal void AutoParseHeaderString()
        {
            if (HeaderArguments == null || HeaderArguments.Count == 0)
            {
                return;
            }

            var stringItems = HeaderArguments.Where(x => x.Value != null && x.Value.GetType() == typeof(byte[])).Select(x => x.Key).ToArray();
            foreach (var key in stringItems)
            {
                HeaderArguments[key] = Encoding.UTF8.GetString(HeaderArguments[key] as byte[]);
            }
        }

    }
}
