using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertInvestigation
{
    class Program
    {
        private static Random random = new Random();
        private static string connectionString = ConfigurationManager.ConnectionStrings["testDB"].ConnectionString;

        static void Main(string[] args)
        {
            Console.WriteLine("Preparing data records");
            int batchSize = 10000;
            var dataRecords = CreateDataRecords(100000);
            
            IEnumerable<Type> inserterTypes = GetInserterTypes();
            foreach (var inserterType in inserterTypes)
            {
                try
                {
                    RunTest(dataRecords, batchSize, inserterType);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    throw;
                }
            }

            Console.WriteLine();
        }

        private static void RunTest(IList<Customer> dataRecords, int batchSize, Type inserterType)
        {
            Console.WriteLine("Inserting {0} records in {1} batch size via {2}", dataRecords.Count, batchSize, inserterType);
            var inserter = (IInserter) Activator.CreateInstance(inserterType, connectionString, batchSize);

            var stopWatch = Stopwatch.StartNew();
            inserter.Insert(dataRecords);
            stopWatch.Stop();

            Console.WriteLine("{0}: Done in {1} seconds", inserterType, stopWatch.Elapsed.TotalSeconds);

            TruncateTable();
        }

        private static void TruncateTable()
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = "TRUNCATE TABLE dbo.Customers";
                command.CommandType = CommandType.Text;

                command.ExecuteNonQuery();

                Console.WriteLine("Customers table truncated");
            }
        }

        private static IEnumerable<Type> GetInserterTypes()
        {
            return Assembly.GetExecutingAssembly()
                .GetTypes()
                .Where(type => typeof(IInserter).IsAssignableFrom(type) && type.IsClass);
        }

        private static IList<Customer> CreateDataRecords(int count)
        {
            var result = new List<Customer>(count);
            for (int i = 0; i < count; i++)
            {
                result.Add(GenerateDataRecord());
            }

            return result;
        }

        private static Customer GenerateBadDataRecord()
        {
            return new Customer
            {
                Address = null,
                Email = null,
                Name = null
            };
        }

        private static Customer GenerateDataRecord()
        {
            return new Customer
            {
                Address = string.Format("Amosova str, {0}", random.Next(100)),
                Email = string.Format("{0}@email.com", Guid.NewGuid()),
                IsActive = random.NextDouble() > 0.5,
                Longitude = random.NextDouble(),
                Latitude = random.NextDouble(),
                Name = "John Smith"
            };
        }
    }
}
