import os
import time
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor


DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "onlinestore")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


def connect_with_retry(retries=20, delay=2):
    last_error = None
    for _ in range(retries):
        try:
            return psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
            )
        except psycopg2.OperationalError as error:
            last_error = error
            time.sleep(delay)
    raise last_error


def create_tables(conn):
    query = """
    CREATE TABLE IF NOT EXISTS Customers (
        CustomerID SERIAL PRIMARY KEY,
        FirstName VARCHAR(100) NOT NULL,
        LastName VARCHAR(100) NOT NULL,
        Email VARCHAR(255) UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS Products (
        ProductID SERIAL PRIMARY KEY,
        ProductName VARCHAR(255) NOT NULL,
        Price NUMERIC(10, 2) NOT NULL CHECK (Price >= 0)
    );

    CREATE TABLE IF NOT EXISTS Orders (
        OrderID SERIAL PRIMARY KEY,
        CustomerID INT NOT NULL REFERENCES Customers(CustomerID),
        OrderDate TIMESTAMP NOT NULL DEFAULT NOW(),
        TotalAmount NUMERIC(10, 2) NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS OrderItems (
        OrderItemID SERIAL PRIMARY KEY,
        OrderID INT NOT NULL REFERENCES Orders(OrderID) ON DELETE CASCADE,
        ProductID INT NOT NULL REFERENCES Products(ProductID),
        Quantity INT NOT NULL CHECK (Quantity > 0),
        Subtotal NUMERIC(10, 2) NOT NULL CHECK (Subtotal >= 0)
    );
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()


def seed_data(conn):
    with conn.cursor() as cur:
        customers = [
            ("Ivan", "Petrov", "ivan@example.com"),
            ("Anna", "Sidorova", "anna@example.com"),
        ]
        for first_name, last_name, email in customers:
            cur.execute(
                """
                INSERT INTO Customers (FirstName, LastName, Email)
                SELECT %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM Customers WHERE Email = %s
                );
                """,
                (first_name, last_name, email, email),
            )

        products = [
            ("Laptop", Decimal("1200.00")),
            ("Mouse", Decimal("25.00")),
            ("Keyboard", Decimal("55.00")),
        ]
        for product_name, price in products:
            cur.execute(
                """
                INSERT INTO Products (ProductName, Price)
                SELECT %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM Products WHERE ProductName = %s
                );
                """,
                (product_name, price, product_name),
            )
    conn.commit()


def place_order_transaction(conn, customer_id, items):
    """
    Scenario 1:
    1. Add row into Orders
    2. Add rows into OrderItems
    3. Update TotalAmount in Orders from OrderItems sum
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
                VALUES (%s, NOW(), 0)
                RETURNING OrderID;
                """,
                (customer_id,),
            )
            order_id = cur.fetchone()[0]

            for product_id, quantity in items:
                cur.execute(
                    "SELECT Price FROM Products WHERE ProductID = %s;",
                    (product_id,),
                )
                product_row = cur.fetchone()
                if product_row is None:
                    raise ValueError(f"ProductID={product_id} not found")

                price = product_row[0]
                subtotal = Decimal(price) * quantity

                cur.execute(
                    """
                    INSERT INTO OrderItems (OrderID, ProductID, Quantity, Subtotal)
                    VALUES (%s, %s, %s, %s);
                    """,
                    (order_id, product_id, quantity, subtotal),
                )

            cur.execute(
                """
                UPDATE Orders
                SET TotalAmount = (
                    SELECT COALESCE(SUM(Subtotal), 0)
                    FROM OrderItems
                    WHERE OrderID = %s
                )
                WHERE OrderID = %s;
                """,
                (order_id, order_id),
            )

        conn.commit()
        return order_id
    except Exception:
        conn.rollback()
        raise


def update_customer_email_transaction(conn, customer_id, new_email):
    """
    Scenario 2:
    Atomic customer email update
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE Customers
                SET Email = %s
                WHERE CustomerID = %s;
                """,
                (new_email, customer_id),
            )

            if cur.rowcount == 0:
                raise ValueError(f"CustomerID={customer_id} not found")

        conn.commit()
    except Exception:
        conn.rollback()
        raise


def add_product_transaction(conn, product_name, price):
    """
    Scenario 3:
    Atomic product insert
    """
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO Products (ProductName, Price)
                VALUES (%s, %s)
                RETURNING ProductID;
                """,
                (product_name, price),
            )
            product_id = cur.fetchone()[0]
        conn.commit()
        return product_id
    except Exception:
        conn.rollback()
        raise


def print_state(conn):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        print("\nCustomers:")
        cur.execute("SELECT * FROM Customers ORDER BY CustomerID;")
        for row in cur.fetchall():
            print(dict(row))

        print("\nProducts:")
        cur.execute("SELECT * FROM Products ORDER BY ProductID;")
        for row in cur.fetchall():
            print(dict(row))

        print("\nOrders:")
        cur.execute("SELECT * FROM Orders ORDER BY OrderID;")
        for row in cur.fetchall():
            print(dict(row))

        print("\nOrderItems:")
        cur.execute("SELECT * FROM OrderItems ORDER BY OrderItemID;")
        for row in cur.fetchall():
            print(dict(row))


def main():
    conn = connect_with_retry()
    try:
        create_tables(conn)
        seed_data(conn)

        order_id = place_order_transaction(
            conn,
            customer_id=1,
            items=[
                (1, 1),  # Laptop x1
                (2, 2),  # Mouse x2
            ],
        )
        print(f"Created order: {order_id}")

        update_customer_email_transaction(conn, customer_id=1, new_email="ivan.new@example.com")
        print("Updated customer email for CustomerID=1")

        new_product_id = add_product_transaction(conn, "Webcam", Decimal("70.00"))
        print(f"Added product: {new_product_id}")

        print_state(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
