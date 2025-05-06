import psycopg2
import csv
import os
from os import environ

class Bai5:
    CREATE_TABLES_SQL = """
    CREATE TABLE IF NOT EXISTS accounts (
        customer_id INTEGER PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        address_1 VARCHAR(100),
        address_2 VARCHAR(100),
        city VARCHAR(50),
        state VARCHAR(50),
        zip_code VARCHAR(10),
        join_date DATE
    );
    CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts (customer_id);

    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        product_code VARCHAR(10),
        product_description TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_products_product_id ON products (product_id);

    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(50) PRIMARY KEY,
        transaction_date DATE,
        product_id INTEGER NOT NULL REFERENCES products(product_id),
        quantity INTEGER,
        account_id INTEGER NOT NULL REFERENCES accounts(customer_id)
    );
    CREATE INDEX IF NOT EXISTS idx_transactions_transaction_date ON transactions (transaction_date);
    """

    def __init__(self, connection, data_dir='data'):
        self.conn = connection
        self.cur = self.conn.cursor()
        self.data_dir = data_dir

    def create_tables(self):
        self.cur.execute(self.CREATE_TABLES_SQL)

    def _import_table_data(self, table_name, csv_file, columns):
        csv_path = os.path.join(self.data_dir, csv_file)
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, skipinitialspace=True)
            for row in reader:
                placeholders = ', '.join(['%s'] * len(columns))
                values = [row[col] for col in columns]
                
                sql = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                """
                self.cur.execute(sql, values)

    def import_data(self):
        tables_config = [
            {
                'table_name': 'accounts',
                'csv_file': 'accounts.csv',
                'columns': [
                    'customer_id', 'first_name', 'last_name',
                    'address_1', 'address_2', 'city', 'state',
                    'zip_code', 'join_date'
                ]
            },
            {
                'table_name': 'products',
                'csv_file': 'products.csv',
                'columns': [
                    'product_id', 'product_code', 'product_description'
                ]
            },
            {
                'table_name': 'transactions',
                'csv_file': 'transactions.csv',
                'columns': [
                    'transaction_id', 'transaction_date',
                    'product_id', 'quantity', 'account_id'
                ]
            }
        ]

        for config in tables_config:
            self._import_table_data(**config)

    def run(self):
        try:
            self.create_tables()
            self.import_data()
            self.conn.commit()
            print("🎉 Dữ liệu đã được nhập thành công!")
        except Exception as e:
            self.conn.rollback()
            print(f" Có lỗi xảy ra: {str(e)}")

if __name__ == "__main__":
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    
    # your code here
    port = environ.get('POSTGRES_PORT', '5432')
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas,port=port)
    cur = conn.cursor()
        
    manager = Bai5(conn, data_dir='data')
    manager.run()