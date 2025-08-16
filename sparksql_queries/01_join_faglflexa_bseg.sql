%sql
-- Join FAGLFLEXA and BSEG tables
-- This query joins the two source tables from ECC Everest
-- and applies the initial filtering requirements

CREATE OR REPLACE TEMPORARY VIEW v_finance_joined AS
SELECT 
  f.RCLNT AS Client,
  f.RBUKRS AS CompanyCode,
  f.GJAHR AS FiscalYear,
  f.POPER AS Period,
  f.DOCNR AS DocumentNumber,
  f.RYEAR AS FiscalYearDocument,
  f.RACCT AS GLAccount,
  f.RCNTR AS CostCenter,
  f.PRCTR AS ProfitCenter,
  f.RFAREA AS FunctionalArea,
  f.RBUSA AS BusinessArea,
  f.KOKRS AS ControllingArea,
  f.DRCRK AS DebitCreditIndicator,
  f.HSL AS AmountInLocalCurrency,
  f.KSL AS AmountInGroupCurrency,
  f.OSL AS AmountInTransactionCurrency,
  f.KTOSL AS AccountAssignment,
  f.KTOPL AS ChartOfAccounts,
  f.HRKFT AS CostElement,
  f.RHCUR AS LocalCurrency,
  f.RKCUR AS GroupCurrency,
  f.RTCUR AS TransactionCurrency,
  b.BELNR AS DocumentNumberBSEG,
  b.BUKRS AS CompanyCodeBSEG,
  b.GJAHR AS FiscalYearBSEG,
  b.BUZEI AS LineItem,
  b.HKONT AS GLAccountBSEG,
  b.AUGDT AS ClearingDate,
  b.AUGCP AS ClearingPeriod,
  b.AUGBL AS ClearingDocument,
  b.ZUONR AS AssignmentNumber,
  b.SGTXT AS ItemText,
  b.HBKID AS HouseBank,
  b.BSCHL AS PostingKey,
  b.KOART AS AccountType,
  b.XBILK AS BalanceSheetAccount
FROM 
  ECC_Everest.FAGLFLEXA f
INNER JOIN 
  ECC_Everest.BSEG b
ON 
  f.DOCNR = b.BELNR
  AND f.RBUKRS = b.BUKRS
  AND f.RYEAR = b.GJAHR
WHERE 
  f.RBUKRS NOT LIKE '8%'
  AND b.XBILK != 'X';