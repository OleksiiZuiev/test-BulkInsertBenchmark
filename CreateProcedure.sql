CREATE  PROCEDURE  InsertCustomers
    @Customers dbo.CustomersTableType READONLY
AS 
BEGIN
    INSERT INTO Customers(Email, Name, [Address], IsActive, Latitude, Longitude)
    SELECT Email, Name, [Address], IsActive, Latitude, Longitude
    FROM @Customers
END