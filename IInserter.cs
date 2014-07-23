using System.Collections.Generic;
using System.Linq;

namespace BulkInsertInvestigation
{
    public interface IInserter
    {
        void Insert(IEnumerable<Customer> dataRecords);
    }

}