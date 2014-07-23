using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertInvestigation
{
    public class GenerateSqlInserter : IInserter
    {
        private string connectionString;
        private int batchSize;

        public GenerateSqlInserter(string connectionString, int batchSize)
        {
            this.connectionString = connectionString;
            this.batchSize = batchSize;
        }

        public void Insert(IEnumerable<Customer> dataRecords)
        {
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var transaction = connection.BeginTransaction();

                IEnumerable<Customer> batch = null;
                int inserted = 0;
                do
                {
                    batch = dataRecords.Skip(inserted).Take(this.batchSize);

                    if (!batch.Any())
                        break;

                    var command = connection.CreateCommand();
                    command.CommandText = GenerateBatchInsertSql(batch);
                    command.CommandType = CommandType.Text;
                    command.Transaction = transaction;


                    //command.ExecuteNonQuery();
                    var dataReader = command.ExecuteReader();
                    dataReader.Close();
                        
                    inserted += batchSize;
                    Console.WriteLine("Inserted {0} rows", inserted);
                } while (batch.Any());

                transaction.Commit();
            }
        }

        private string GenerateBatchInsertSql(IEnumerable<Customer> customers)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append(@"
                    DECLARE @InsertResults TABLE(
	                    Email NVARCHAR(256),
	                    Inserted BIT NOT NULL,
	                    Error NVARCHAR(MAX) NULL
                    );");

            foreach (Customer customer in customers)
            {
                stringBuilder.AppendFormat(@"
                     INSERT INTO Customers (Email, Name, [Address], IsActive, Latitude, Longitude)
	                    OUTPUT '{0}', 1, NULL INTO @InsertResults
                     VALUES ('{0}', '{1}', '{2}', {3}, {4}, {5})	"
                    /*@"

                    BEGIN TRY
	                    INSERT INTO Customers (Email, Name, [Address], IsActive, Latitude, Longitude)
		                    OUTPUT '{0}', 1, NULL INTO @InsertResults
	                    VALUES('{0}', '{1}', '{2}', {3}, {4}, {5})	
                    END TRY
                    BEGIN CATCH
	                    INSERT INTO @InsertResults
	                    VALUES ('{0}', 0, ERROR_MESSAGE())
                    END CATCH;
                    "*/,customer.Email, customer.Name, customer.Address, customer.IsActive ? 1 : 0,
                    customer.Latitude, customer.Longitude);
            }

            stringBuilder.Append("SELECT * FROM @InsertResults");

            return stringBuilder.ToString();
        }
    }
}
