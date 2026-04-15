-- Scenario 1: place order
BEGIN;

INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
VALUES (1, NOW(), 0)
RETURNING OrderID;

-- Example order items (replace order_id with returned id):
-- INSERT INTO OrderItems (OrderID, ProductID, Quantity, Subtotal)
-- VALUES (order_id, 1, 1, 1200.00);
-- INSERT INTO OrderItems (OrderID, ProductID, Quantity, Subtotal)
-- VALUES (order_id, 2, 2, 50.00);

UPDATE Orders
SET TotalAmount = (
    SELECT COALESCE(SUM(Subtotal), 0)
    FROM OrderItems
    WHERE OrderID = Orders.OrderID
)
WHERE OrderID = 1;

COMMIT;
-- ROLLBACK;


-- Scenario 2: update customer email
BEGIN;

UPDATE Customers
SET Email = 'new_email@example.com'
WHERE CustomerID = 1;

COMMIT;
-- ROLLBACK;


-- Scenario 3: add new product
BEGIN;

INSERT INTO Products (ProductName, Price)
VALUES ('Webcam', 70.00);

COMMIT;
-- ROLLBACK;
