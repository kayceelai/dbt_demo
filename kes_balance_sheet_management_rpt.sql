CREATE OR REPLACE VIEW dv_kes_balance_sheet_management_rpt FOLDER = '/integrate/derived views/kes-data-lake/kes management user reporting model' AS --combine hierarchy sub and os/LN sub INCLUDES END BALANCE data. 
--In production, use view parameters within Denodo rather than variables. Try leaving defaults blank so query will not run without parameters.  
--Commented out 'previous period' pdates cte - not using anymore.
--7-1-2022 - removed all OS data that is consolidated from LN, or is just a different account number from LN. Causeing dup amounts. 
--WITH pdates AS
--(select  GETVAR('varperiod', 'localdate', '2020-01-01') as pd,
--ADDMONTH(to_localdate('yyyy-MM-dd',(GETVAR('varperiod', 'localdate', '2020-01-01'))), -1) as ppd
--) 
--(select period_param as pd, ADDMONTH(to_localdate('yyyy-MM-dd', period_param), -1) as ppd
--)
--,
---------------------------
WITH Hierarchy as (
  select
    SUBSTR(
      accountdesc
      FROM
        5 for INSTR(accountdesc, ' - ') + 1 -5
    ) AS child,
CASE
      WHEN parent = '#VALUE!' then null
      else SUBSTR(
        parent
        FROM
          5 for LEN(parent)
      )
    END as parent,
    SUBSTR(
      accountdesc
      FROM
        INSTR(accountdesc, ' - ') + 4 FOR LEN(accountdesc)
    ) AS accountdesc,
    level
  from
    dv_onestream_accounts_compress
  where
    ingestion_ts = (
      select
        max(ingestion_ts) as maxdate
      from
        dv_onestream_accounts_compress
    )
),
-----------------------
osdata AS --cte for OS data
(
  SELECT
    ossub3.*
  FROM
    (
      SELECT
        ossub2.*,
        xrf.Company,
        xrf.entityname,
        CASE
          WHEN pl.osstrategyone IS NULL THEN xrf.osstrategyone
          ELSE pl.osstrategyone
        END AS strategyone
      FROM
        (
          SELECT
            LASTDAYOFMONTH(to_localdate('yyyy''M''M', OS.time)) AS period,
            SUBSTR(
              OS.entity
              FROM
                0 for INSTR(os.entity, ' - ') + 1
            ) AS entity,
            SUBSTR(
              OS.account
              FROM
                5 for INSTR(os.account, ' - ') + 1 -5
            ) AS account,
            SUBSTR(
              OS.account
              FROM
                INSTR(os.account, ' - ') + 4 FOR LEN(os.account)
            ) AS account_desc,
            CASE
              WHEN flow = 'EndBal' THEN null
              ELSE CAST(os.amount as decimal(15, 2))
            END AS amount -- , SUBSTR(OS.account FROM 5 for 1) 											AS accountstart
,
            SUBSTR(
              OS.ud1
              FROM
                INSTR(os.ud1, ' - ') + 4 FOR LEN(os.ud1)
            ) AS trans_type,
            CASE
              WHEN ud4 = 'None' THEN 'None'
              ELSE Substring(ud4, INSTR(ud4, ' - ') + 3)
            END AS product,
            CASE
              WHEN ud3 = 'None' THEN 'None'
              ELSE Substring(ud3, INSTR(ud3, ' - ') + 3)
            END AS department,
            CASE
              WHEN ud5 = 'None' THEN 'None'
              ELSE Substring(ud5, INSTR(ud5, ' - ') + 3)
            END AS line_of_business,
            CASE
              WHEN ud6 = 'None' THEN 'None'
              ELSE Substring(ud6, INSTR(ud6, ' - ') + 3)
            END AS country_sales_region,
CASE
              WHEN flow = 'EndBal' THEN CAST(os.amount as decimal(15, 2))
              ELSE null
            END AS endbal,
            flow
          FROM
            dv_kesmgmtreportingmodel_onestreamexportdata_compress os
        ) ossub2
        LEFT JOIN dv_kesmgmtreportingmodel_xref_productline_stratone_compress pl ON pl.productline = ossub2.product
        LEFT JOIN dv_kesmgmtreportingmodel_xref_company_stratone_compress xrf ON xrf.entity = ossub2.entity
        LEFT JOIN (
          select
            distinct os_acct_short
          from
            dv_kesmgmtreportingmodel_ln_os_acctmap_compress
        ) am ON am.os_acct_short = ossub2.account
      WHERE
        (
          am.os_acct_short is null
          OR ossub2.flow = 'EndBal'
        )
        AND ossub2.period >= ADDMONTH(CURRENT_DATE, -3) --AND ossub2.entity = os_entity_param  
    ) ossub3
),
--------------------------------------
lndata AS --cte for LN data:
(
  Select
    LN.*,
    null as endbal
  FROM
    (
      SELECT
        lnsub.*
      FROM
        (
          SELECT
            LASTDAYOFMONTH(
              to_localdate(
                'yyyy-M-d',
                CONCAT(
                  fte.reporting_year,
                  '-',
                  fte.reporting_period,
                  '-',
                  '1'
                )
              )
            ) AS period,
            (
              CASE
                WHEN pl.osstrategyone IS NULL THEN xrf.osstrategyone
                ELSE pl.osstrategyone
              END
            ) AS strategyone,
            xrf.entity AS os_entity,
            fte.compnr AS ln_company,
            fte.ledger_account AS account,
            ac.ledger_account_description AS account_desc,
            CASE
              WHEN SUBSTR(
                fte.ledger_account
                from
                  1 for 1
              ) IN (3, 4, 5, 6, 7, 9) THEN --make amount_home match signing of amount_dc, then reverse all signing, 
              --income stmt accounts only
              (
                case
                  WHEN (debit_credit = 2) THEN (
                    cast(amount_in_home_currency_1 as decimal(15, 2)) * -1
                  )
                  ELSE cast(amount_in_home_currency_1 as decimal(15, 2))
                END
              ) * -1
              ELSE --make amount_home match signing of amount_dc, do NOT reverse signing.
              (
                case
                  WHEN (debit_credit = 2) THEN (
                    cast(amount_in_home_currency_1 as decimal(15, 2)) * -1
                  )
                  ELSE cast(amount_in_home_currency_1 as decimal(15, 2))
                END
              )
            END AS amount,
            tt.description AS trans_type,
            d1.dimension_description AS Product,
            d2.dimension_description AS Department,
            d3.dimension_description AS BusinessPartner --DOES NOT EXIST IN OS
,
            d4.dimension_description AS Project --DOES NOT EXIST IN OS
,
            d5.dimension_description AS Line_Of_Business,
            d6.dimension_description AS country_sales_region,
            'LN' AS source,
            CASE
              WHEN am.os_acct IS NULL THEN fte.ledger_account
              ELSE SUBSTR(
                am.os_acct
                FROM
                  5 for LEN(am.os_acct) -3
              )
            END AS os_account --use the mapping from LN to OS where accounts are consolidated
,
            xrf.entityname AS entityname,
            so.sales_order AS so,
            so.reference_a AS so_desc,
            po.purchase_order AS po,
            po.reference_a AS po_desc
          FROM
            dv_infor_ln_tfgld106_finalized_transactions_uc00_j_integration_transactions_uc00 fte
            LEFT JOIN dv_kesmgmtreportingmodel_xref_productline_stratone_compress pl ON pl.productline = fte.dimension_1
            LEFT JOIN dv_kesmgmtreportingmodel_xref_company_stratone_compress xrf ON xrf.company = fte.compnr
            LEFT JOIN dv_infor_ln_transaction_types_uc00 tt ON tt.trans_type_id = fte.trans_type_id
            LEFT JOIN dv_infor_ln_tfgld010_dimensions_dim1_uc00 d1 ON d1.dimension = fte.dimension_1
            LEFT JOIN dv_infor_ln_tfgld010_dimensions_dim2_uc00 d2 ON d2.dimension = fte.dimension_2
            LEFT JOIN dv_infor_ln_tfgld010_dimensions_dim3_uc00 d3 ON d3.dimension = fte.dimension_3
            LEFT JOIN dv_infor_ln_tfgld010_dimensions_dim4_uc00 d4 ON d4.dimension = fte.dimension_4
            LEFT JOIN dv_infor_ln_tfgld010_dimensions_dim5_uc00 d5 ON d5.dimension = fte.dimension_5
            LEFT JOIN dv_infor_ln_tfgld010_dimensions_dim6_uc00 d6 ON d6.dimension = fte.dimension_6
            LEFT JOIN dv_infor_ln_tfgld008_chart_of_accounts_uc00 ac ON ac.ledger_account = fte.ledger_account
            LEFT JOIN dv_kesmgmtreportingmodel_ln_os_acctmap_compress am ON am.ln_acct = fte.ledger_account
            LEFT JOIN dv_infor_ln_tdsls400_sales_orders_uc00 SO ON so.sales_order = fte.it_business_object_id
            LEFT JOIN dv_infor_ln_tdpur400_purchase_orders_uc00 po ON po.purchase_order = fte.it_business_object_id
        ) lnsub
      WHERE
        lnsub.period >= ADDMONTH(CURRENT_DATE, -3) --and lnsub.os_entity = os_entity_param 
    ) LN
) --**************************************
--**************************************
SELECT
  Lev0,
  Lev0_desc,
  Lev1,
  Lev1_desc,
  Lev2,
  Lev2_desc,
  Lev3,
  Lev3_desc,
  Lev4,
  Lev4_desc,
  Lev5,
  Lev5_desc,
  Lev6,
  Lev6_desc,
  Lev7,
  Lev7_desc,
  Lev8,
  Lev8_desc,
  Lev9,
  Lev9_desc,
  Lev10,
  Lev10_desc,
  osln.period,
  GETYEAR(osln.period) AS year_calendar,
CASE
    WHEN osln.period BETWEEN TO_LOCALDATE(
      'yyyyMMdd',
      CONCAT(GETYEAR(osln.period), '1101')
    )
    AND TO_LOCALDATE(
      'yyyyMMdd',
      CONCAT(GETYEAR(osln.period), '1231')
    ) THEN GETYEAR(ADDYEAR(osln.period, 1))
    WHEN osln.period BETWEEN TO_LOCALDATE(
      'yyyyMMdd',
      CONCAT(GETYEAR(osln.period), '0101')
    )
    AND TO_LOCALDATE(
      'yyyyMMdd',
      CONCAT(GETYEAR(osln.period), '1031')
    ) THEN GETYEAR(osln.period)
  END AS year_icomp,
  osln.strategy_one AS os_platform,
  osln.os_entity,
  osln.entity_name,
  osln.ln_company,
  osln.account,
  osln.account_desc,
  h.rolluplevel,
  osln.os_account,
  osln.trans_type,
  osln.product,
  osln.department,
  osln.business_partner,
  osln.project,
  osln.line_of_business,
  osln.country_sales_region,
  osln.source,
  osln.so,
  osln.so_desc,
  osln.po,
  osln.po_desc,
  osln.amount,
  osln.end_balance,
CASE
    WHEN osln.end_balance IS NULL
    AND (
      osln.os_account like 'MCC%'
      OR osln.os_account REGEXP_LIKE '[0-9][R]'
    ) THEN osln.amount
    ELSE osln.end_balance
  END AS amount_final
FROM
  (
    SELECT
      L1.child AS Lev0,
      L1.accountdesc AS Lev0_desc,
      L2.child AS Lev1,
      L2.accountdesc AS Lev1_desc,
      L3.child AS Lev2,
      L3.accountdesc AS Lev2_desc,
      L4.child AS Lev3,
      L4.accountdesc AS Lev3_desc,
      L5.child AS Lev4,
      L5.accountdesc AS Lev4_desc,
      L6.child AS Lev5,
      L6.accountdesc AS Lev5_desc,
      L7.child AS Lev6,
      L7.accountdesc AS Lev6_desc,
      L8.child AS Lev7,
      L8.accountdesc AS Lev7_desc,
      L9.child AS Lev8,
      L9.accountdesc AS Lev8_desc,
      L10.child AS Lev9,
      L10.accountdesc AS Lev9_desc,
      L11.child AS Lev10,
      L11.accountdesc AS Lev10_desc,
      COALESCE(
        L11.child,
        L10.child,
        L9.child,
        L8.child,
        L7.child,
        L6.child,
        L5.child,
        L4.child,
        L3.child,
        L2.child,
        L1.child
      ) as nodeacct,
      COALESCE(
        L11.level,
        L10.level,
        L9.level,
        L8.level,
        L7.level,
        L6.level,
        L5.level,
        L4.level,
        L3.level,
        L2.level,
        L1.level
      ) as rolluplevel
    FROM
      Hierarchy as L1
      LEFT JOIN Hierarchy as L2 ON L1.child = L2.parent
      LEFT JOIN Hierarchy as L3 ON L2.child = L3.parent
      LEFT JOIN Hierarchy as L4 ON L3.child = L4.parent
      LEFT JOIN Hierarchy as L5 ON L4.child = L5.parent
      LEFT JOIN Hierarchy as L6 ON L5.child = L6.parent
      LEFT JOIN Hierarchy as L7 ON L6.child = L7.parent
      LEFT JOIN Hierarchy as L8 ON L7.child = L8.parent
      LEFT JOIN Hierarchy as L9 ON L8.child = L9.parent
      LEFT JOIN Hierarchy as L10 ON L9.child = L10.parent
      LEFT JOIN Hierarchy as L11 ON L10.child = L11.parent
    WHERE
      L1.parent is null
      AND L1.child = 'BALANCESHEET' --*******************************
    UNION
    ALL --*******************************
    SELECT
      'BALANCESHEET' AS lev0,
      'Balance Sheet Accounts' AS Lev0_desc,
      '20' AS Lev1,
      'Total Liabilities and Equity' AS Lev1_desc,
      '290' AS Lev2,
      'Total Equity' AS Lev2_desc,
      '2900' AS Lev3,
      'Total Stockholders Equity' AS Lev3_desc,
      '2920' AS Lev4,
      'Retained Earnings' AS Lev4_desc,
      'MCC_KOCH_INTADJ' AS Lev5,
      'Interest Inc/(Exp) on Implied Debt' AS Lev5_desc,
      null AS Lev6,
      null AS Lev6_desc,
      null AS Lev7,
      null AS Lev7_desc,
      null AS Lev8,
      null AS Lev8_desc,
      null AS Lev9,
      null AS Lev9_desc,
      null AS Lev10,
      null AS Lev10_desc,
      'MCC_KOCH_INTADJ' AS nodeacct,
      '7' AS rolluplevel
  ) h
  INNER JOIN (
    --****************************
    --all rows unioned, ln and os
    select
      period,
      strategyone as strategy_one,
      os_entity,
      entityname AS entity_name,
      ln_company,
      account,
      account_desc,
      os_account,
      trans_type,
      product,
      department,
      businesspartner AS business_partner,
      project,
      line_of_business,
      country_sales_region,
      source,
      so,
      so_desc,
      po,
      po_desc,
      amount,
      endbal AS end_balance
    from
      lndata
    UNION
    ALL --OS data
    SELECT
      osdata.period,
      osdata.strategyone as strategy_one,
      osdata.entity AS os_entity,
      osdata.entityname AS entity_name,
      osdata.company AS ln_company,
      osdata.account,
      osdata.account_desc,
      osdata.account AS os_account,
      osdata.trans_type,
      osdata.product,
      osdata.department,
      null AS business_partner,
      null AS project,
      osdata.line_of_business,
      osdata.country_sales_region,
      'OS' AS source,
      null AS so,
      null AS so_desc,
      null AS po,
      null AS po_desc,
      osdata.amount AS amount,
      osdata.endbal AS end_balance
    FROM
      osdata
  ) osln ON osln.os_account = h.nodeacct --CONTEXT('VAR varperiod' = '2022-05-31', 'VAR varentity' = 'KES_L516')
  --USING PARAMETERS ( os_entity_param : text, period_param : localdate )  
  CONTEXT ('formatted' = 'yes');