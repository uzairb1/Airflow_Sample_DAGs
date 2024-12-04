-- Create customers table
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    country TEXT
);

-- Create bank_account_transactions table
CREATE TABLE transactions (
    deposit_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    date DATE,
    amount DECIMAL,
    currency TEXT,
    FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);
CREATE TABLE exchange_rates(
    date date PRIMARY KEY,
    gbp_to_eur float
)