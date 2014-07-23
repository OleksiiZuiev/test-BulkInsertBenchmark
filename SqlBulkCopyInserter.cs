using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertInvestigation
{
    public class SqlBulkCopyInserter : IInserter
    {
        private readonly string connectionString;
        private readonly int batchSize;


        public SqlBulkCopyInserter(string connectionString, int batchSize)
        {
            this.connectionString = connectionString;
            this.batchSize = batchSize;
        }

        public void Insert(IEnumerable<Customer> batch)
        {
            var dataTable = batch.ToDataTable();


            using (var bulkCopy = new SqlBulkCopy(connectionString, (SqlBulkCopyOptions)0))
            {
                bulkCopy.NotifyAfter = batchSize;
                bulkCopy.SqlRowsCopied += bulkCopy_SqlRowsCopied;
                bulkCopy.DestinationTableName = "Customers";
                bulkCopy.BulkCopyTimeout = 0; 

                ConfigureColumnMappings(bulkCopy, dataTable);

                try
                {
                    bulkCopy.WriteToServer(dataTable);
                }
                catch (Exception ex)
                {
                    Console.Write(ex.ToString());
                }
                
            }
        }

        private void ConfigureColumnMappings(SqlBulkCopy bulkCopy, DataTable dataTable)
        {
            foreach (DataColumn column in dataTable.Columns)
            {
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }
        }

        private void bulkCopy_SqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            Console.WriteLine("Inserted {0} rows...", e.RowsCopied);
        }
    }
}
