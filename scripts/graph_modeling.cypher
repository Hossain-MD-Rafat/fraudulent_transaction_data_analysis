// --- 1. Create Customer Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
MERGE (c:Customer {CustomerID: row.cc_num})
SET c.firstName = row.first,
    c.lastName = row.last,
    c.gender = row.gender,
    c.street = row.street,
    c.city = row.city,
    c.state = row.state,
    c.zip = row.zip,
    c.job = row.job,
    c.dob = row.dob;

// --- 2. Create Transaction Nodes ---
:auto
LOAD CSV WITH HEADERS FROM 'file:///transactions.csv' AS row
CALL {
    WITH row
    CREATE (t:Transaction {TransactionID: row.TransactionID})
    SET t.TransactionDate = row.TransactionDate,
        t.Amount = row.Amount,
        t.IsFraud = row.IsFraud
} IN TRANSACTIONS OF 10000 ROWS;

// --- 3. Create Merchant Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///merchants.csv' AS row
MERGE (m:Merchant {MerchantID: row.MerchantID})
SET m.Category = row.Category;

// --- 4. Create Bank Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///banks.csv' AS row
MERGE (b:Bank {BankName: row.BankName});

// --- 5. Create Account Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///accounts.csv' AS row
MERGE (a:Account {AccountID: row.AccountID});

// --- 6. Create Customer -> Transaction Relationships ---
:auto
LOAD CSV WITH HEADERS FROM 'file:///customer_transactions.csv' AS row
CALL {
    WITH row
    CREATE (c:Customer {CustomerID: row.CustomerID})
    CREATE (t:Transaction {TransactionID: row.TransactionID})
    CREATE (c)-[:PERFORMED]->(t)
} IN TRANSACTIONS OF 10000 ROWS;


// --- 7. Create Transaction -> Merchant Relationships ---
:auto
LOAD CSV WITH HEADERS FROM 'file:///transaction_merchants.csv' AS row
CALL {
    WITH row
    MATCH (t:Transaction {TransactionID: row.TransactionID})
    MATCH (m:Merchant {MerchantID: row.MerchantID})
    CREATE (t)-[:PROCESSED_AT]->(m)
} IN TRANSACTIONS OF 10000 ROWS;

// --- 8. Create Transaction -> Account Relationships ---
:auto
LOAD CSV WITH HEADERS FROM 'file:///transaction_accounts.csv' AS row
CALL {
    WITH row
    MATCH (t:Transaction {TransactionID: row.Timestamp})  // Ensure TransactionID is indexed
    MATCH (aFrom:Account {AccountID: row.FromAccount})
    MATCH (aTo:Account {AccountID: row.ToAccount})
    CREATE (t)-[:TRANSFERRED_FROM]->(aFrom)
    CREATE (t)-[:TRANSFERRED_TO]->(aTo)
    SET t.AmountPaid = toFloat(row.AmountPaid),
        t.AmountReceived = toFloat(row.AmountReceived),
        t.IsLaundering = toBoolean(row.IsLaundering)
} IN TRANSACTIONS OF 1000 ROWS;


// --- 9. Create Account -> Bank Relationships ---
:auto
LOAD CSV WITH HEADERS FROM 'file:///account_banks.csv' AS row
CALL {
    WITH row
    MATCH (a:Account {AccountID: row.AccountID})
    MATCH (b:Bank {BankName: row.BankName})
    CREATE (a)-[:ASSOCIATED_WITH]->(b)
} IN TRANSACTIONS OF 10000 ROWS;















// --- 1. Create Customer Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
MERGE (c:Customer {CustomerID: row.cc_num})
SET c.firstName = row.first,
    c.lastName = row.last,
    c.gender = row.gender,
    c.street = row.street,
    c.city = row.city,
    c.state = row.state,
    c.zip = row.zip,
    c.job = row.job,
    c.dob = row.dob;

// --- 2. Create Transaction Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///transactions.csv' AS row
MERGE (t:Transaction {TransactionID: row.TransactionID})
SET t.TransactionDate = row.TransactionDate,
    t.Amount = toFloat(row.Amount),
    t.IsFraud = toBoolean(row.IsFraud);

// --- 3. Create Merchant Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///merchants.csv' AS row
MERGE (m:Merchant {MerchantID: row.MerchantID})
SET m.Category = row.Category;

// --- 4. Create Bank Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///banks.csv' AS row
MERGE (b:Bank {BankName: row.BankName});

// --- 5. Create Account Nodes ---
LOAD CSV WITH HEADERS FROM 'file:///accounts.csv' AS row
MERGE (a:Account {AccountID: row.AccountID});

// --- 6. Create Customer -> Transaction Relationships ---
LOAD CSV WITH HEADERS FROM 'file:///customer_transactions.csv' AS row
MATCH (c:Customer {CustomerID: row.CustomerID})
MATCH (t:Transaction {TransactionID: row.TransactionID})
MERGE (c)-[:PERFORMED]->(t);

// --- 7. Create Transaction -> Merchant Relationships ---
LOAD CSV WITH HEADERS FROM 'file:///transaction_merchants.csv' AS row
MATCH (t:Transaction {TransactionID: row.TransactionID})
MATCH (m:Merchant {MerchantID: row.MerchantID})
MERGE (t)-[:PROCESSED_AT]->(m);

// --- 8. Create Transaction -> Account Relationships ---
LOAD CSV WITH HEADERS FROM 'file:///transaction_accounts.csv' AS row
MATCH (t:Transaction {TransactionID: row.Timestamp})  // Assuming Timestamp uniquely identifies transactions
MATCH (aFrom:Account {AccountID: row.FromAccount})
MATCH (aTo:Account {AccountID: row.ToAccount})
MERGE (t)-[:TRANSFERRED_FROM]->(aFrom)
MERGE (t)-[:TRANSFERRED_TO]->(aTo)
SET t.AmountPaid = toFloat(row.AmountPaid),
    t.AmountReceived = toFloat(row.AmountReceived),
    t.IsLaundering = toBoolean(row.is_laundering);

// --- 9. Create Account -> Bank Relationships ---
LOAD CSV WITH HEADERS FROM 'file:///account_banks.csv' AS row
MATCH (a:Account {AccountID: row.AccountID})
MATCH (b:Bank {BankName: row.BankName})
MERGE (a)-[:ASSOCIATED_WITH]->(b);

