using System;

namespace Pink.RabbitMQ.Helper
{
    public static class TimeHelper
    {
        /// <summary>
        /// 将日期转换为时间戳
        /// </summary>
        /// <param name="date">指定的时间</param>
        /// <returns>返回与1970-01-01所相差的秒数</returns>
        public static long ToTimestamp(DateTime date)
        {
            var startDate = TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));
            return (long)(date - startDate).TotalMilliseconds;
        }

        /// <summary>
        /// 将时间戳转换为日期
        /// </summary>
        /// <param name="timestamp">与1970-01-01所相差的秒数所记录的时间戳</param>
        /// <returns>转换后的日期</returns>
        public static DateTime ToDateTime(long timestamp)
        {
            var startDate = TimeZone.CurrentTimeZone.ToLocalTime(new DateTime(1970, 1, 1));
            return startDate.AddMilliseconds(timestamp);
        }
    }
}
