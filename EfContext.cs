using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkInsertInvestigation
{
    public class EfContext : DbContext
    {
        public EfContext(DbConnection connection) : base(connection, false)
        {
        }

        public DbSet<Customer> Customers { get; set; }
    }
}
