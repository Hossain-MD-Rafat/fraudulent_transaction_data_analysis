// Phase 2 Neo4j graph model
//
// Design goals:
// - preserve every fraud transaction and AML transfer as a distinct event node
// - avoid duplicate nodes/relationships on re-import
// - connect fraud_ds2 transactions to merchants even though that source has no
//   customer/card identifier
// - make AML topology suitable for cycles, funnels, mule-account and community
//   analysis

// -----------------------------------------------------------------------------
// 0. Constraints / indexes
// -----------------------------------------------------------------------------
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer) REQUIRE c.CustomerID IS UNIQUE;

CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS
FOR (t:Transaction) REQUIRE t.TransactionID IS UNIQUE;

CREATE CONSTRAINT merchant_id_unique IF NOT EXISTS
FOR (m:Merchant) REQUIRE m.MerchantID IS UNIQUE;

CREATE CONSTRAINT bank_id_unique IF NOT EXISTS
FOR (b:Bank) REQUIRE b.BankID IS UNIQUE;

CREATE CONSTRAINT account_id_unique IF NOT EXISTS
FOR (a:Account) REQUIRE a.AccountID IS UNIQUE;

CREATE CONSTRAINT aml_transfer_id_unique IF NOT EXISTS
FOR (x:AMLTransfer) REQUIRE x.TransferID IS UNIQUE;

CREATE INDEX transaction_date_index IF NOT EXISTS
FOR (t:Transaction) ON (t.TransactionDate);

CREATE INDEX aml_timestamp_index IF NOT EXISTS
FOR (x:AMLTransfer) ON (x.Timestamp);

CREATE INDEX aml_laundering_index IF NOT EXISTS
FOR (x:AMLTransfer) ON (x.IsLaundering);

// -----------------------------------------------------------------------------
// 1. Customer nodes
// -----------------------------------------------------------------------------
:auto
LOAD CSV WITH HEADERS FROM 'file:///customers.csv' AS row
CALL {
    WITH row
    MERGE (c:Customer {CustomerID: row.CustomerID})
    SET c.firstName = row.first,
        c.lastName = row.last,
        c.gender = row.gender,
        c.street = row.street,
        c.city = row.city,
        c.state = row.state,
        c.zip = row.zip,
        c.job = row.job,
        c.dob = row.dob
} IN TRANSACTIONS OF 5000 ROWS;

// -----------------------------------------------------------------------------
// 2. Fraud transaction event nodes
// -----------------------------------------------------------------------------
:auto
LOAD CSV WITH HEADERS FROM 'file:///transactions.csv' AS row
CALL {
    WITH row
    MERGE (t:Transaction {TransactionID: row.TransactionID})
    SET t.TransactionDate = datetime(row.TransactionDate),
        t.Amount = toFloat(row.Amount),
        t.IsFraud = toBoolean(row.IsFraud),
        t.SourceDataset = row.SourceDataset
} IN TRANSACTIONS OF 5000 ROWS;

// -----------------------------------------------------------------------------
// 3. Merchant nodes
// -----------------------------------------------------------------------------
:auto
LOAD CSV WITH HEADERS FROM 'file:///merchants.csv' AS row
CALL {
    WITH row
    MERGE (m:Merchant {MerchantID: row.MerchantID})
    SET m.Category = row.Category,
        m.Location = CASE WHEN row.Location = '' THEN null ELSE row.Location END,
        m.Latitude = CASE WHEN row.Latitude = '' THEN null ELSE toFloat(row.Latitude) END,
        m.Longitude = CASE WHEN row.Longitude = '' THEN null ELSE toFloat(row.Longitude) END,
        m.SourceDataset = row.SourceDataset
} IN TRANSACTIONS OF 5000 ROWS;

// -----------------------------------------------------------------------------
// 4. Fraud-transaction relationships
// -----------------------------------------------------------------------------
:auto
LOAD CSV WITH HEADERS FROM 'file:///customer_transactions.csv' AS row
CALL {
    WITH row
    MATCH (c:Customer {CustomerID: row.CustomerID})
    MATCH (t:Transaction {TransactionID: row.TransactionID})
    MERGE (c)-[:PERFORMED]->(t)
} IN TRANSACTIONS OF 5000 ROWS;

:auto
LOAD CSV WITH HEADERS FROM 'file:///transaction_merchants.csv' AS row
CALL {
    WITH row
    MATCH (t:Transaction {TransactionID: row.TransactionID})
    MATCH (m:Merchant {MerchantID: row.MerchantID})
    MERGE (t)-[:PROCESSED_AT]->(m)
} IN TRANSACTIONS OF 5000 ROWS;

// -----------------------------------------------------------------------------
// 5. Bank and account nodes
// -----------------------------------------------------------------------------
:auto
LOAD CSV WITH HEADERS FROM 'file:///banks.csv' AS row
CALL {
    WITH row
    MERGE (b:Bank {BankID: row.BankID})
} IN TRANSACTIONS OF 5000 ROWS;

:auto
LOAD CSV WITH HEADERS FROM 'file:///accounts.csv' AS row
CALL {
    WITH row
    MERGE (a:Account {AccountID: row.AccountID})
} IN TRANSACTIONS OF 5000 ROWS;

:auto
LOAD CSV WITH HEADERS FROM 'file:///account_banks.csv' AS row
CALL {
    WITH row
    MATCH (a:Account {AccountID: row.AccountID})
    MATCH (b:Bank {BankID: row.BankID})
    MERGE (a)-[:ASSOCIATED_WITH]->(b)
} IN TRANSACTIONS OF 5000 ROWS;

// -----------------------------------------------------------------------------
// 6. AML transfer event nodes
// -----------------------------------------------------------------------------
// Every CSV row becomes one AMLTransfer node, even when the same two accounts
// transact repeatedly. This prevents transaction history from being collapsed
// into a single Account->Account relationship.
:auto
LOAD CSV WITH HEADERS FROM 'file:///aml_transfers.csv' AS row
CALL {
    WITH row
    MERGE (x:AMLTransfer {TransferID: row.TransferID})
    SET x.Timestamp = datetime(row.Timestamp),
        x.AmountPaid = toFloat(row.AmountPaid),
        x.AmountReceived = toFloat(row.AmountReceived),
        x.PaymentCurrency = row.PaymentCurrency,
        x.ReceivingCurrency = row.ReceivingCurrency,
        x.PaymentFormat = row.PaymentFormat,
        x.IsLaundering = toBoolean(row.IsLaundering),
        x.FromBank = row.FromBank,
        x.ToBank = row.ToBank

    WITH row, x
    MATCH (fromAccount:Account {AccountID: row.FromAccount})
    MATCH (toAccount:Account {AccountID: row.ToAccount})
    MERGE (fromAccount)-[:SENT]->(x)
    MERGE (x)-[:RECEIVED_BY]->(toAccount)

    WITH row, x
    MATCH (fromBank:Bank {BankID: row.FromBank})
    MATCH (toBank:Bank {BankID: row.ToBank})
    MERGE (x)-[:ORIGINATED_AT]->(fromBank)
    MERGE (x)-[:DESTINED_TO]->(toBank)
} IN TRANSACTIONS OF 5000 ROWS;

// -----------------------------------------------------------------------------
// 7. Integrity checks to run after import
// -----------------------------------------------------------------------------
// All fraud transactions should have a merchant relationship.
MATCH (t:Transaction)
RETURN
    count(t) AS transaction_count,
    count { (t)-[:PROCESSED_AT]->(:Merchant) } AS transactions_with_merchant,
    count { (t)<-[:PERFORMED]-(:Customer) } AS transactions_with_customer;

// Every AML transfer should have exactly one sender and one receiver.
MATCH (x:AMLTransfer)
RETURN
    count(x) AS aml_transfer_count,
    count { (:Account)-[:SENT]->(x) } AS transfers_with_sender,
    count { (x)-[:RECEIVED_BY]->(:Account) } AS transfers_with_receiver;
