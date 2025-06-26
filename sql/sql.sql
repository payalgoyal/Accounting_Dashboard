-- Create raw data table
CREATE TABLE ledgers (
    ledger_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    type ENUM('Customer', 'Supplier') NOT NULL
);

CREATE TABLE sales (
    invoice_no VARCHAR(20) PRIMARY KEY,
    date DATE NOT NULL,
    party VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2),
    gst DECIMAL(10,2),
    mode VARCHAR(20),
    source VARCHAR(20),
    FOREIGN KEY (party) REFERENCES ledgers(name)
);

CREATE TABLE purchases (
    bill_no VARCHAR(20) PRIMARY KEY,
    date DATE NOT NULL,
    supplier VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2),
    gst DECIMAL(10,2),
    mode VARCHAR(20),
    source VARCHAR(20),
    FOREIGN KEY (supplier) REFERENCES ledgers(name)
);

CREATE TABLE bank_transactions (
    txn_id VARCHAR(20) PRIMARY KEY,
    date DATE NOT NULL,
    description VARCHAR(255),
    amount DECIMAL(10,2),
    type ENUM('credit', 'debit') NOT NULL,
    mode VARCHAR(20)
);

-- Create cleaned tables

CREATE TABLE ledgers_cleaned (
    ledger_id VARCHAR(10) PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    type ENUM('Customer', 'Supplier') NOT NULL
);

CREATE TABLE sales_cleaned (
    invoice_no VARCHAR(20) PRIMARY KEY,
    date DATE NOT NULL,
    party VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2),
    gst DECIMAL(10,2),
    mode VARCHAR(20),
    source VARCHAR(20),
    FOREIGN KEY (party) REFERENCES ledgers(name)
);

CREATE TABLE purchases_cleaned (
    bill_no VARCHAR(20) PRIMARY KEY,
    date DATE NOT NULL,
    supplier VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2),
    gst DECIMAL(10,2),
    mode VARCHAR(20),
    source VARCHAR(20),
    FOREIGN KEY (supplier) REFERENCES ledgers(name)
);

CREATE TABLE bank_transactions_cleaned (
    txn_id VARCHAR(20) PRIMARY KEY,
    date DATE NOT NULL,
    description VARCHAR(255),
    amount DECIMAL(10,2),
    type ENUM('credit', 'debit') NOT NULL,
    mode VARCHAR(20)
);