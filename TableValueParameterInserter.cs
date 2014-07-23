using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertInvestigation
{
    public class TableValueParameterInserter : IInserter
    {
        private string connectionString;
        private int batchSize;

        public TableValueParameterInserter(string connectionString, int batchSize)
        {
            this.connectionString = connectionString;
            this.batchSize = batchSize;
        }


        public void Insert(IEnumerable<Customer> dataRecords)
        {
            int inserted = 0;
            IEnumerable<Customer> batch = null;

            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                var command = connection.CreateCommand();
                command.CommandText = "InsertCustomers";
                command.CommandType = CommandType.StoredProcedure;

                var parameter = new SqlParameter();
                parameter.SqlDbType = SqlDbType.Structured;
                parameter.TypeName = "dbo.CustomersTableType";
                parameter.ParameterName = "@Customers";
                command.Parameters.Add(parameter);

                do
                {
                    batch = dataRecords.Skip(inserted).Take(this.batchSize);
                    var dataTable = batch.ToDataTable();

                    parameter.Value = dataTable;
                    command.ExecuteNonQuery();
                    inserted += batchSize;
                    Console.WriteLine("Inserted {0} rows", inserted);
                } while (batch.Any());
            }
        }
    }
}
