%sql
-- Map GL accounts to golden GL accounts
-- This query maps the offset accounts to their golden GL account equivalents

CREATE OR REPLACE TEMPORARY VIEW v_finance_mapped AS
SELECT 
  g.*,
  COALESCE(m.GoldenGLAccount, g.OffsetAccount) AS GoldenOffsetAccount
FROM 
  v_finance_gainloss g
LEFT JOIN 
  Finance.GL_Account_Mapping m
ON 
  g.OffsetAccount = m.SourceGLAccount
  AND g.CompanyCode = m.CompanyCode;