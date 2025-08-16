%sql
-- Create Finance table if not exists
CREATE TABLE IF NOT EXISTS Finance.finance (
    FiscalYear INT,
    PostingPeriod INT,
    DocumentNumber STRING,
    CompCode STRING,
    LegalEntity STRING,
    GLAccount STRING,
    GoldenGLAcct STRING,
    TradingPartner STRING,
    GoldenTradingPartner STRING,
    GainLossGC DECIMAL(18,2),
    GainLossLC DECIMAL(18,2),
    LocalCurrency STRING,
    GainLossTC DECIMAL(18,2),
    TransactionCurrency STRING,
    OffsetAccount STRING,
    GoldenOffsetAccount STRING,
    OffsetAccountLCAmount DECIMAL(18,2),
    OffsetAccountTCAmount DECIMAL(18,2),
    OffsetClearingDocumentNumber STRING,
    SourceSystem STRING,
    LoadTimestamp TIMESTAMP
);