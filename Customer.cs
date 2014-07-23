using System;

namespace BulkInsertInvestigation
{
    public class Customer
    {
        public int Id { get; set; }

        public string Email { get; set; }

        public string Name { get; set; }

        public string Address { get; set; }

        public bool IsActive { get; set; }

        public double? Latitude { get; set; }

        public double? Longitude { get; set; }
    }
}