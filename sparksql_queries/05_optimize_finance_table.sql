%sql
-- Optimize the Finance table for better query performance
OPTIMIZE Finance.finance
ZORDER BY (FiscalYear, PostingPeriod, CompCode, LegalEntity);