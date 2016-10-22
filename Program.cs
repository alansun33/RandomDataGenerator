using System;
using System.Data;
using System.Diagnostics;
using System.Threading;

namespace RandomEntityGenerator
{
    class Program
    {
        // ReSharper disable once UnusedParameter.Local
        static void Main(string[] args)
        {
            Console.WriteLine("Input name of table to generate data:\r\n");
            var tableName = Console.ReadLine();
            PerformJob(tableName);
        }

        private static void PerformJob(string tableName)
        {
            DataTable dt = null;
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {
                var popDataTableTask = DBHelper.PopulateDataTable(tableName, 10000);
                for (var i = 1; !popDataTableTask.IsCompleted; i++)
                {
                    Console.Clear();
                    Console.WriteLine($"Populating DataTable [{tableName}]...\r\nTime ellapsed: {i} seconds.");
                    Thread.Sleep(1000);
                }
                dt = popDataTableTask.Result;
            }
            catch (AggregateException exception)
            {
                stopwatch.Stop();
                dt?.Dispose();
                Debug.Fail(exception.InnerException?.Message);
            }
            try
            {
                var sendTask = DBHelper.SendToDatabase(dt, tableName);
                for (var i = 1; !sendTask.IsCompleted; i++)
                {
                    Console.Clear();
                    Console.WriteLine($"Sending DataTable [{tableName}] to Database...\r\nTime ellapsed: {i} seconds.");
                    Thread.Sleep(1000);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                stopwatch.Stop();
                Console.WriteLine($"Job is done. \r\nTime ellapsed: {stopwatch.Elapsed}.");
            }
            dt?.Dispose();
        }
    }
}
