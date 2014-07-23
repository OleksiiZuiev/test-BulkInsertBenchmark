using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertInvestigation
{
    public class GenerateSqlSingularInserter
    {
        private string connectionString;
        private int batchSize;

        public GenerateSqlSingularInserter (string connectionString, int batchSize)
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

                var command = connection.CreateCommand();
                command.CommandText =
                    @"INSERT INTO Customers(Email, Name, [Address], IsActive, Latitude, Longitude)
                     VALUES (@Email, @Name, @Address, @IsActive, @Latitude, @Longitude)";
                command.CommandType = CommandType.Text;
                command.Transaction = transaction;

                var emailParameter = new SqlParameter {ParameterName = "@Email"};
                command.Parameters.Add(emailParameter);

                var nameParameter = new SqlParameter {ParameterName = "@Name"};
                command.Parameters.Add(nameParameter);

                var addressParameter = new SqlParameter {ParameterName = "@Address"}; 
                command.Parameters.Add(addressParameter);

                var isActiveParameter = new SqlParameter {ParameterName = "@IsActive"};
                command.Parameters.Add(isActiveParameter);

                var latitudeParameter = new SqlParameter {ParameterName = "@Latitude"};
                command.Parameters.Add(latitudeParameter);

                var longitudeParameter = new SqlParameter {ParameterName = "@Longitude"};
                command.Parameters.Add(longitudeParameter);

                foreach (Customer customer in dataRecords)
                {

                    emailParameter.Value = customer.Email;
                    nameParameter.Value = customer.Name;
                    addressParameter.Value = customer.Address;
                    isActiveParameter.Value = customer.IsActive;
                    latitudeParameter.Value = customer.Latitude;
                    longitudeParameter.Value = customer.Longitude;

                    command.ExecuteNonQuery();

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
