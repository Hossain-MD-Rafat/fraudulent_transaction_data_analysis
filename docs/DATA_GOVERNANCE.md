# Data Governance and Privacy Notes

This portfolio project uses public benchmark / synthetic-style fraud and anti-money-laundering datasets for experimentation and reproducibility. Some source files contain **identity-like fields** such as names, card/account identifiers, addresses, dates of birth, and geographic attributes.

Even when those values are synthetic or benchmark-generated, the project treats them as **sensitive-looking data** and applies the following principles:

- Identity-like fields are not used as direct model features when they are unnecessary for risk prediction.
- Operational scoring logs do not persist raw feature payloads.
- Transaction identifiers are hashed before being written to the monitoring event stream.
- Public screenshots and portfolio visuals should avoid exposing names, full card/account values, street addresses, or dates of birth.
- Model artifacts are stored outside Git and validated using versioned metadata plus SHA-256 checksums.
- Drift reference profiles store aggregate distributions rather than raw training records.
- The Neo4j model uses account/customer identifiers for graph connectivity, but presentation layers should prefer tokenized or masked identifiers.

## Production recommendation

For a real financial institution, raw source data should live in access-controlled storage rather than a public code repository. The GitHub repository should contain only a minimal anonymized sample or schema fixture, while CI obtains approved test data from protected storage or generates synthetic fixtures.

## Portfolio boundary

The repository demonstrates engineering and modeling patterns. It should not be interpreted as a production compliance framework, and the cost assumptions, graph thresholds, and decision policy are project assumptions that would require governance, legal/compliance review, and calibration before use with real customer data.
