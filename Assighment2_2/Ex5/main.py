import psycopg2
import pandas as pd

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "developer"


    try:
        # 1. Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=pas
        )
        cursor = conn.cursor()
        print("Database connected.")

        # 2. Create all required tables (DDL)
        create_tables = """
        DROP TABLE IF EXISTS products CASCADE;
        
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_code VARCHAR(10),
            product_description TEXT
        );

        DROP TABLE IF EXISTS accounts CASCADE;
        
        CREATE TABLE IF NOT EXISTS accounts (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            address_1 TEXT,
            address_2 TEXT,
            city VARCHAR(50),
            state VARCHAR(50),
            zip_code VARCHAR(10),
            join_date DATE
        );

        DROP TABLE IF EXISTS transactions CASCADE;

        CREATE TABLE transactions (
            transaction_id VARCHAR(50) PRIMARY KEY,
            transaction_date DATE,
            product_id INT REFERENCES products(product_id),
            product_code VARCHAR(10),
            product_description TEXT,
            quantity INT,
            account_id INT REFERENCES accounts(customer_id)
        );
        """
        cursor.execute(create_tables)
        print("Tables created.")

        # 3. Load CSV: products
        with open('data/products.csv', 'r') as file:
            next(file)  # Skip header
            cursor.copy_from(file, 'products', sep=',', null='')
        print("Products loaded.")

        # 4. Load CSV: accounts
        with open('data/accounts.csv', 'r') as file:
            next(file)
            cursor.copy_from(file, 'accounts', sep=',', null='')
        print("Accounts loaded.")

        # 5. Load CSV: transactions
        with open('data/transactions.csv', 'r') as file:
            next(file)
            cursor.copy_from(file, 'transactions', sep=',', null='')
        print("Transactions loaded.")

        # 6. Commit & close
        conn.commit()

        # ------------------
        cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema='public'
                    """)

        tables = [table[0] for table in cursor.fetchall()]
        print("Tables in database:", tables)
        
        # Function to display data from a specific table
        def view_table_data(table_name):
            print(f"\nData in table '{table_name}':")
            
            # Execute query to get all data from the table
            cursor.execute(f"SELECT * FROM {table_name}")
            
            # Get column names
            col_names = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create a DataFrame for better display
            df = pd.DataFrame(rows, columns=col_names)
            return df
        
        # Example: View data from each table
        for table in tables:
            print(view_table_data(table))
        # ------------------

        
        cursor.close()
        conn.close()
        # print("DONE! Data ingested into PostgreSQL successfully.")

    except Exception as e:
        print("Error occurred:", e)

if __name__ == "__main__":
    main()
