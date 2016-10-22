using System;
using System.Linq;

namespace RandomEntityGenerator
{
    public class RandomValueFactory
    {
        private const string Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private const int DefaultStringLength = 8;

        public static string GetRandomString(int length)
        {
            length = length > 255 ? DefaultStringLength : length;
            var random = new Random(DateTime.Now.Millisecond);
            System.Threading.Thread.Sleep(1);
            return new string(Enumerable.Repeat(Chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public static int GetRandomInteger(int min = 0, int max = 10000)
        {
            var random = new Random(DateTime.Now.Millisecond);
            System.Threading.Thread.Sleep(1);
            return random.Next(min, max);
        }

        public static double GetRandomDouble(int range = 10000)
        {
            var random = new Random(DateTime.Now.Millisecond);
            System.Threading.Thread.Sleep(1);
            return Math.Round(random.NextDouble() * range, 2);
        }

        public static DateTime GetRandomDateTime()
        {
            return DateTime.Today.AddDays(-1 * GetRandomInteger());
        }

        public static object GetRandomValue(Type type, int min = 0, int max = 10000)
        {
            if (type == typeof(int) || type == typeof(int?))
            {
                return GetRandomInteger(min, max);
            }
            if (type == typeof(double) || type == typeof(double?))
            {
                return GetRandomDouble(max);
            }
            if (type == typeof(decimal) || type == typeof(decimal?))
            {
                return (decimal)GetRandomDouble(max);
            }
            if (type == typeof(string))
            {
                return GetRandomString(max);
            }
            if (type == typeof(DateTime) || type == typeof(DateTime?))
            {
                return GetRandomDateTime();
            }
            if (type == typeof(bool))
            {
                return GetRandomInteger()%2 == 0;
            }
            return null;
        }
    }
}
