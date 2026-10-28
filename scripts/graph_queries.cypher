// Phase 2 fraud / AML investigation queries
// Run after scripts/graph_modeling.cypher has imported the redesigned graph.

// -----------------------------------------------------------------------------
// 1. Highest-value known fraudulent card transactions
// -----------------------------------------------------------------------------
MATCH (t:Transaction {IsFraud: true})-[:PROCESSED_AT]->(m:Merchant)
OPTIONAL MATCH (c:Customer)-[:PERFORMED]->(t)
RETURN
    t.TransactionID AS transactionId,
    t.SourceDataset AS source,
    t.TransactionDate AS transactionDate,
    t.Amount AS amount,
    c.CustomerID AS customerId,
    m.MerchantID AS merchantId,
    m.Category AS category
ORDER BY amount DESC
LIMIT 25;

// -----------------------------------------------------------------------------
// 2. Merchants with elevated observed fraud rate
// -----------------------------------------------------------------------------
MATCH (t:Transaction)-[:PROCESSED_AT]->(m:Merchant)
WITH
    m,
    count(t) AS transactionCount,
    sum(CASE WHEN t.IsFraud THEN 1 ELSE 0 END) AS fraudCount
WHERE transactionCount >= 5
RETURN
    m.MerchantID AS merchantId,
    m.Category AS category,
    transactionCount,
    fraudCount,
    toFloat(fraudCount) / transactionCount AS observedFraudRate
ORDER BY observedFraudRate DESC, fraudCount DESC
LIMIT 25;

// -----------------------------------------------------------------------------
// 3. Known laundering transfers with complete sender/receiver context
// -----------------------------------------------------------------------------
MATCH (from:Account)-[:SENT]->(x:AMLTransfer {IsLaundering: true})-[:RECEIVED_BY]->(to:Account)
RETURN
    x.TransferID AS transferId,
    x.Timestamp AS timestamp,
    from.AccountID AS fromAccount,
    x.FromBank AS fromBank,
    to.AccountID AS toAccount,
    x.ToBank AS toBank,
    x.AmountPaid AS amountPaid,
    x.PaymentCurrency AS paymentCurrency,
    x.AmountReceived AS amountReceived,
    x.ReceivingCurrency AS receivingCurrency,
    x.PaymentFormat AS paymentFormat
ORDER BY x.AmountPaid DESC;

// -----------------------------------------------------------------------------
// 4. High-throughput / funnel-like accounts
// Accounts receiving from many distinct senders can warrant investigation.
// -----------------------------------------------------------------------------
MATCH (sender:Account)-[:SENT]->(:AMLTransfer)-[:RECEIVED_BY]->(receiver:Account)
WITH
    receiver,
    count(*) AS incomingTransfers,
    count(DISTINCT sender) AS distinctSenders
WHERE distinctSenders >= 3
RETURN
    receiver.AccountID AS accountId,
    incomingTransfers,
    distinctSenders
ORDER BY distinctSenders DESC, incomingTransfers DESC
LIMIT 50;

// -----------------------------------------------------------------------------
// 5. Possible fan-out / mule-distribution accounts
// -----------------------------------------------------------------------------
MATCH (sender:Account)-[:SENT]->(:AMLTransfer)-[:RECEIVED_BY]->(receiver:Account)
WITH
    sender,
    count(*) AS outgoingTransfers,
    count(DISTINCT receiver) AS distinctReceivers
WHERE distinctReceivers >= 3
RETURN
    sender.AccountID AS accountId,
    outgoingTransfers,
    distinctReceivers
ORDER BY distinctReceivers DESC, outgoingTransfers DESC
LIMIT 50;

// -----------------------------------------------------------------------------
// 6. Accounts adjacent to known laundering activity
// Useful for expanding an investigation beyond the labeled transfer itself.
// -----------------------------------------------------------------------------
MATCH (a:Account)-[:SENT|RECEIVED_BY]-(x:AMLTransfer {IsLaundering: true})
WITH a, count(DISTINCT x) AS launderingTransferCount
OPTIONAL MATCH (a)-[:SENT|RECEIVED_BY]-(allTransfer:AMLTransfer)
RETURN
    a.AccountID AS accountId,
    launderingTransferCount,
    count(DISTINCT allTransfer) AS totalTransferCount
ORDER BY launderingTransferCount DESC, totalTransferCount DESC;

// -----------------------------------------------------------------------------
// 7. Two-hop exposure to laundering
// Finds accounts that are not necessarily directly labeled but are within two
// transfer hops of an account participating in known laundering.
// -----------------------------------------------------------------------------
MATCH (seed:Account)-[:SENT|RECEIVED_BY]-(l:AMLTransfer {IsLaundering: true})
MATCH path = (seed)-[:SENT|RECEIVED_BY*1..4]-(candidate:Account)
WHERE candidate <> seed
RETURN DISTINCT
    seed.AccountID AS launderingAdjacentAccount,
    candidate.AccountID AS exposedAccount,
    length(path) AS relationshipDistance
ORDER BY relationshipDistance, launderingAdjacentAccount
LIMIT 200;

// -----------------------------------------------------------------------------
// 8. Circular three-account movement (A -> B -> C -> A)
// Event nodes mean each transfer is preserved and the path can be inspected.
// -----------------------------------------------------------------------------
MATCH (a:Account)-[:SENT]->(x1:AMLTransfer)-[:RECEIVED_BY]->(b:Account),
      (b)-[:SENT]->(x2:AMLTransfer)-[:RECEIVED_BY]->(c:Account),
      (c)-[:SENT]->(x3:AMLTransfer)-[:RECEIVED_BY]->(a)
WHERE a.AccountID < b.AccountID
  AND b.AccountID < c.AccountID
RETURN
    a.AccountID AS accountA,
    b.AccountID AS accountB,
    c.AccountID AS accountC,
    x1.TransferID AS transferAB,
    x2.TransferID AS transferBC,
    x3.TransferID AS transferCA,
    x1.AmountPaid AS amountAB,
    x2.AmountPaid AS amountBC,
    x3.AmountPaid AS amountCA
LIMIT 100;

// -----------------------------------------------------------------------------
// 9. Rapid pass-through pattern
// B receives money and sends another transfer shortly afterward.
// This is a simplified layering heuristic, not a laundering determination.
// -----------------------------------------------------------------------------
MATCH (a:Account)-[:SENT]->(incoming:AMLTransfer)-[:RECEIVED_BY]->(b:Account)
MATCH (b)-[:SENT]->(outgoing:AMLTransfer)-[:RECEIVED_BY]->(c:Account)
WHERE outgoing.Timestamp >= incoming.Timestamp
  AND duration.between(incoming.Timestamp, outgoing.Timestamp).hours <= 24
RETURN
    b.AccountID AS passThroughAccount,
    a.AccountID AS incomingFrom,
    c.AccountID AS outgoingTo,
    incoming.TransferID AS incomingTransfer,
    outgoing.TransferID AS outgoingTransfer,
    incoming.Timestamp AS receivedAt,
    outgoing.Timestamp AS sentAt,
    incoming.AmountReceived AS amountReceived,
    outgoing.AmountPaid AS amountSent
ORDER BY receivedAt
LIMIT 200;

// -----------------------------------------------------------------------------
// 10. Account risk-feature export for hybrid graph + ML modeling
// Produces basic graph-derived features that can later be joined to a model.
// -----------------------------------------------------------------------------
MATCH (a:Account)
OPTIONAL MATCH (a)-[:SENT]->(sent:AMLTransfer)-[:RECEIVED_BY]->(sentTo:Account)
WITH
    a,
    count(DISTINCT sent) AS outgoingTransferCount,
    count(DISTINCT sentTo) AS distinctReceivers,
    sum(coalesce(sent.AmountPaid, 0.0)) AS outgoingAmount,
    sum(CASE WHEN sent.IsLaundering THEN 1 ELSE 0 END) AS outgoingLaunderingCount
OPTIONAL MATCH (receivedFrom:Account)-[:SENT]->(received:AMLTransfer)-[:RECEIVED_BY]->(a)
RETURN
    a.AccountID AS accountId,
    outgoingTransferCount,
    distinctReceivers,
    outgoingAmount,
    outgoingLaunderingCount,
    count(DISTINCT received) AS incomingTransferCount,
    count(DISTINCT receivedFrom) AS distinctSenders,
    sum(coalesce(received.AmountReceived, 0.0)) AS incomingAmount,
    sum(CASE WHEN received.IsLaundering THEN 1 ELSE 0 END) AS incomingLaunderingCount
ORDER BY accountId;
