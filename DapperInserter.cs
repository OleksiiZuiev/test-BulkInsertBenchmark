using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace BulkInsertInvestigation
{
    public class DapperInserter : IInserter
     {
        private string connectionString;
        private int batchSize;

        public DapperInserter(string connectionString, int batchSize)
        {
            this.connectionString = connectionString;
            this.batchSize = batchSize;
        }

        public void Insert(IEnumerable<Customer> dataRecords)
        {
            int inserted = 0;
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                var transaction = connection.BeginTransaction();

                foreach (Customer customer in dataRecords)
                {
                    try
                    {
                        connection.Execute(
                            @"INSERT INTO Customers(Email, Name, [Address], IsActive, Latitude, Longitude)
                        VALUES (@Email, @Name, @Address, @IsActive, @Latitude, @Longitude)",
                            customer,
                            transaction);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }

                    if (++inserted%batchSize == 0)
                    {
                        Console.WriteLine("Inserted {0} customers", inserted);
                    }
                }

                transaction.Commit();
            }
        }
    }
}
