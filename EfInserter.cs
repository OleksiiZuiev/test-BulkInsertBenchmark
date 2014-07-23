using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;

namespace BulkInsertInvestigation
{
    public class EfInserter
    {
        private readonly string connectionString;
        private readonly int batchSize;

        public EfInserter(string connectionString, int batchSize)
        {
            this.connectionString = connectionString;
            this.batchSize = batchSize;
        }

        public void Insert(IEnumerable<Customer> dataRecords)
        {
            int inserted = 0;
            IEnumerable<Customer> batch = null;

            
            using (var connection = new SqlConnection(connectionString))
            using (var context = new EfContext(connection))
            {

                context.Configuration.AutoDetectChangesEnabled = false;
                context.Configuration.ValidateOnSaveEnabled = false;

                do
                {
                    batch = dataRecords.Skip(inserted).Take(this.batchSize);

                    foreach (Customer customer in batch)
                    {
                        context.Customers.Add(customer);
                    }

                    context.SaveChanges();
                    inserted += batchSize;
                    Console.WriteLine("Inserted {0} rows", inserted);
                } while (batch.Any());
            }
             
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var transaction = connection.BeginTransaction();

                do
                {
                    batch = dataRecords.Skip(inserted).Take(this.batchSize);
                    
                    connection.Execute(
                        @"INSERT INTO Customers(Email, Name, [Address], IsActive, Latitude, Longitude)
                        VALUES (@Email, @Name, @Address, @IsActive, @Latitude, @Longitude)",
                        batch,
                        transaction);

                    inserted += batchSize;
                    Console.WriteLine("Inserted {0} rows", inserted);
                } while (batch.Any());

                transaction.Commit();
            }
        }
    }
}
