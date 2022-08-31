CREATE OR REPLACE VIEW dv_kes_income_statement_management_rpt FOLDER = '/integrate/derived views/kes-data-lake/kes management user reporting model' AS 
WITH hierarchy AS (
  SELECT
    substr(
      accountdesc
      FROM
        5 FOR ((instr(accountdesc, ' - ') + 1) -5)
    ) AS child,
    case
      WHEN (parent = '#VALUE!') THEN NULL
      ELSE substr(
        parent
        FROM
          5 FOR len(parent)
      )
    END AS parent,
    substr(
      accountdesc
      FROM
        (instr(accountdesc, ' - ') + 4) FOR len(accountdesc)
    ) AS accountdesc,
    level
  FROM
    dv_onestream_accounts_compress
  WHERE
    ingestion_ts = (
      SELECT
        max(ingestion_ts) AS maxdate
      FROM
        dv_onestream_accounts_compress
    )
),
osdata AS (
  SELECT
    *
  FROM
    (
      SELECT
        lastdayofmonth(
          to_localdate(
            'yyyy''M''M',
            dv_kesmgmtreportingmodel_onestreamexportdata_compress.time
          )
        ) AS period,
        substr(
          dv_kesmgmtreportingmodel_onestreamexportdata_compress.entity
          FROM
            0 FOR (
              instr(
                dv_kesmgmtreportingmodel_onestreamexportdata_compress.entity,
                ' - '
              ) + 1
            )
        ) AS entity,
        substr(
          dv_kesmgmtreportingmodel_onestreamexportdata_compress.account
          FROM
            5 FOR (
              (
                instr(
                  dv_kesmgmtreportingmodel_onestreamexportdata_compress.account,
                  ' - '
                ) + 1
              ) -5
            )
        ) AS account,
        substr(
          dv_kesmgmtreportingmodel_onestreamexportdata_compress.account
          FROM
            (
              instr(
                dv_kesmgmtreportingmodel_onestreamexportdata_compress.account,
                ' - '
              ) + 4
            ) FOR len(
              dv_kesmgmtreportingmodel_onestreamexportdata_compress.account
            )
        ) AS accountdesc,
        cast(
          dv_kesmgmtreportingmodel_onestreamexportdata_compress.amount AS DECIMAL(15, 2)
        ) AS amount,
        substr(
          dv_kesmgmtreportingmodel_onestreamexportdata_compress.ud1
          FROM
            (
              instr(
                dv_kesmgmtreportingmodel_onestreamexportdata_compress.ud1,
                ' - '
              ) + 4
            ) FOR len(
              dv_kesmgmtreportingmodel_onestreamexportdata_compress.ud1
            )
        ) AS trans_type,
        case
          WHEN (ud4 = 'None') THEN 'None'
          ELSE substring(ud4, (instr(ud4, ' - ') + 3))
        END AS product,
        case
          WHEN (ud3 = 'None') THEN 'None'
          ELSE substring(ud3, (instr(ud3, ' - ') + 3))
        END AS department,
        case
          WHEN (ud5 = 'None') THEN 'None'
          ELSE substring(ud5, (instr(ud5, ' - ') + 3))
        END AS line_of_business,
        case
          WHEN (ud6 = 'None') THEN 'None'
          ELSE substring(ud6, (instr(ud6, ' - ') + 3))
        END AS country_sales_region
      FROM
        dv_kesmgmtreportingmodel_onestreamexportdata_compress
    )
  WHERE
    period >= addmonth(current_date(), -3)
),
lndata AS (
  SELECT
    *
  FROM
    (
      SELECT
        lastdayofmonth(
          to_localdate(
            'yyyy-M-d',
            concat(
              fte.reporting_year,
              '-',
              fte.reporting_period,
              '-',
              '1'
            )
          )
        ) AS period,
        case
          WHEN (pl.osstrategyone is null) THEN xrf.osstrategyone
          ELSE pl.osstrategyone
        END AS strategyone,
        xrf.entity AS os_entity,
        fte.compnr AS ln_company,
        fte.ledger_account AS account,
        ac.ledger_account_description AS accountdesc,
        case
          WHEN (
            substr(
              fte.ledger_account
              FROM
                1 FOR 1
            ) in (3, 4, 5, 6, 7, 9)
          ) THEN (
            case
              WHEN (debit_credit = 2) THEN (
                cast(amount_in_home_currency_1 AS DECIMAL(15, 2)) * -1
              )
              ELSE cast(amount_in_home_currency_1 AS DECIMAL(15, 2))
            END * -1
          )
          ELSE case
            WHEN (debit_credit = 2) THEN (
              cast(amount_in_home_currency_1 AS DECIMAL(15, 2)) * -1
            )
            ELSE cast(amount_in_home_currency_1 AS DECIMAL(15, 2))
          END
        END AS amount,
        tt.description AS trans_type,
        d1.dimension_description AS product,
        d2.dimension_description AS department,
        d3.dimension_description AS businesspartner,
        d4.dimension_description AS project,
        d5.dimension_description AS line_of_business,
        d6.dimension_description AS country_sales_region,
        'LN' AS source,
        case
          WHEN (am.os_acct is null) THEN fte.ledger_account
          ELSE substr(
            am.os_acct
            FROM
              5 FOR (len(am.os_acct) -3)
          )
        END AS os_account,
        xrf.entityname AS entityname,
        so.sales_order AS so,
        so.reference_a AS so_desc,
        po.purchase_order AS po,
        po.reference_a AS po_desc
      FROM
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                dv_infor_ln_tfgld106_finalized_transactions_uc00_j_integration_transactions_cache AS fte
                                LEFT OUTER JOIN dv_kesmgmtreportingmodel_xref_productline_stratone_compress AS pl ON pl.productline = fte.dimension_1
                              )
                              LEFT OUTER JOIN dv_kesmgmtreportingmodel_xref_company_stratone_compress AS xrf ON xrf.company = fte.compnr
                            )
                            LEFT OUTER JOIN dv_infor_ln_transaction_types_uc00 AS tt ON tt.trans_type_id = fte.trans_type_id
                          )
                          LEFT OUTER JOIN dv_infor_ln_tfgld010_dimensions_dim1_uc00 AS d1 ON d1.dimension = fte.dimension_1
                        )
                        LEFT OUTER JOIN dv_infor_ln_tfgld010_dimensions_dim2_uc00 AS d2 ON d2.dimension = fte.dimension_2
                      )
                      LEFT OUTER JOIN dv_infor_ln_tfgld010_dimensions_dim3_uc00 AS d3 ON d3.dimension = fte.dimension_3
                    )
                    LEFT OUTER JOIN dv_infor_ln_tfgld010_dimensions_dim4_uc00 AS d4 ON d4.dimension = fte.dimension_4
                  )
                  LEFT OUTER JOIN dv_infor_ln_tfgld010_dimensions_dim5_uc00 AS d5 ON d5.dimension = fte.dimension_5
                )
                LEFT OUTER JOIN dv_infor_ln_tfgld010_dimensions_dim6_uc00 AS d6 ON d6.dimension = fte.dimension_6
              )
              LEFT OUTER JOIN dv_infor_ln_tfgld008_chart_of_accounts_uc00 AS ac ON ac.ledger_account = fte.ledger_account
            )
            LEFT OUTER JOIN dv_kesmgmtreportingmodel_ln_os_acctmap_compress AS am ON am.ln_acct = fte.ledger_account
          )
          LEFT OUTER JOIN dv_infor_ln_tdsls400_sales_orders_uc00 AS so ON so.sales_order = fte.it_business_object_id
        )
        LEFT OUTER JOIN dv_infor_ln_tdpur400_purchase_orders_uc00 AS po ON po.purchase_order = fte.it_business_object_id
    )
  WHERE
    period >= addmonth(current_date(), -3)
)
SELECT
  lev0,
  lev0_desc,
  lev1,
  lev1_desc,
  lev2,
  lev2_desc,
  lev3,
  lev3_desc,
  lev4,
  lev4_desc,
  lev5,
  lev5_desc,
  lev6,
  lev6_desc,
  lev7,
  lev7_desc,
  lev8,
  lev8_desc,
  lev9,
  lev9_desc,
  lev10,
  lev10_desc,
  osln.period,
  getyear(osln.period) AS year_calendar,
  case
    WHEN (
      osln.period between to_localdate('yyyyMMdd', (getyear(osln.period) || '1101'))
      AND to_localdate('yyyyMMdd', (getyear(osln.period) || '1231'))
    ) THEN getyear(addyear(osln.period, 1))
    WHEN (
      osln.period between to_localdate('yyyyMMdd', (getyear(osln.period) || '0101'))
      AND to_localdate('yyyyMMdd', (getyear(osln.period) || '1031'))
    ) THEN getyear(osln.period)
  END AS year_icomp,
  osln.strategy_one AS os_platform,
  osln.os_entity,
  osln.entity_name,
  osln.ln_company,
  osln.account,
  osln.account_desc,
  h.rolluplevel,
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
  osln.amount
FROM
  (
    SELECT
      l1.child AS lev0,
      l1.accountdesc AS lev0_desc,
      l2.child AS lev1,
      l2.accountdesc AS lev1_desc,
      l3.child AS lev2,
      l3.accountdesc AS lev2_desc,
      l4.child AS lev3,
      l4.accountdesc AS lev3_desc,
      l5.child AS lev4,
      l5.accountdesc AS lev4_desc,
      l6.child AS lev5,
      l6.accountdesc AS lev5_desc,
      l7.child AS lev6,
      l7.accountdesc AS lev6_desc,
      l8.child AS lev7,
      l8.accountdesc AS lev7_desc,
      l9.child AS lev8,
      l9.accountdesc AS lev8_desc,
      l10.child AS lev9,
      l10.accountdesc AS lev9_desc,
      l11.child AS lev10,
      l11.accountdesc AS lev10_desc,
      coalesce(
        l11.child,
        l10.child,
        l9.child,
        l8.child,
        l7.child,
        l6.child,
        l5.child,
        l4.child,
        l3.child,
        l2.child,
        l1.child
      ) AS nodeacct,
      coalesce(
        l11.level,
        l10.level,
        l9.level,
        l8.level,
        l7.level,
        l6.level,
        l5.level,
        l4.level,
        l3.level,
        l2.level,
        l1.level
      ) AS rolluplevel
    FROM
      (
        (
          (
            (
              (
                (
                  (
                    (
                      (
                        hierarchy AS l1
                        LEFT OUTER JOIN hierarchy AS l2 ON l1.child = l2.parent
                      )
                      LEFT OUTER JOIN hierarchy AS l3 ON l2.child = l3.parent
                    )
                    LEFT OUTER JOIN hierarchy AS l4 ON l3.child = l4.parent
                  )
                  LEFT OUTER JOIN hierarchy AS l5 ON l4.child = l5.parent
                )
                LEFT OUTER JOIN hierarchy AS l6 ON l5.child = l6.parent
              )
              LEFT OUTER JOIN hierarchy AS l7 ON l6.child = l7.parent
            )
            LEFT OUTER JOIN hierarchy AS l8 ON l7.child = l8.parent
          )
          LEFT OUTER JOIN hierarchy AS l9 ON l8.child = l9.parent
        )
        LEFT OUTER JOIN hierarchy AS l10 ON l9.child = l10.parent
      )
      LEFT OUTER JOIN hierarchy AS l11 ON l10.child = l11.parent
    WHERE
      (
        l1.parent is null
        AND l1.child = 'INCOMESTATEMENT'
      )
  ) AS h
  INNER JOIN (
    SELECT
      lndata.period,
      strategyone AS strategy_one,
      os_entity,
      entityname AS entity_name,
      ln_company,
      account,
      accountdesc AS account_desc,
      os_account,
      amount,
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
      po_desc
    FROM
      lndata SQL
    UNION
    ALL
    SELECT
      osdata.period,
      case
        WHEN (pl.osstrategyone is null) THEN xrf.osstrategyone
        ELSE pl.osstrategyone
      END AS strategy_one,
      osdata.entity AS os_entity,
      xrf.entityname AS entity_name,
      xrf.company AS ln_company,
      account,
      accountdesc AS account_desc,
      account AS os_account,
      amount AS amount,
      trans_type,
      product,
      department,
      NULL AS business_partner,
      NULL AS project,
      line_of_business,
      country_sales_region,
      'OS' AS source,
      NULL AS so,
      NULL AS so_desc,
      NULL AS po,
      NULL AS po_desc
    FROM
      (
        osdata
        LEFT OUTER JOIN dv_kesmgmtreportingmodel_xref_productline_stratone_compress AS pl ON pl.productline = osdata.product
      )
      LEFT OUTER JOIN dv_kesmgmtreportingmodel_xref_company_stratone_compress AS xrf ON xrf.entity = osdata.entity
  ) AS osln ON osln.os_account = h.nodeacct;