// src/lib/sqlBuilder/columns.ts

export const columnMapping: { [key: string]: string } = {
  "gl identifier": "UTM_ID",
  "company": "CO_ID",
  "company name": "NAME",
  "well code": "WELLCODE",
  "accountingid": "WELLCODE",
  "cost center": "WELLCODE",
  "cost center name": "WELL_NAME",
  "well name": "WELL_NAME",
  "accounting period": "PER_END_DATE",
  "month end": "PER_END_DATE",
  "posting date": "POSTING_DATE",
  "accounting date": "LOS_PRODUCTIONDATE",
  "activity date": "LOS_PRODUCTIONDATE",
  "excalibur module": "SYSTEM_CODE",
  "journal entry": "VOUCHER",
  "vendor": "VENDORNAME",
  "vendor name": "VENDORNAME",
  "afe": "AFE_ID",
  "gas purchaser": "PURCH_ID",
  "purchaser": "PURCHNAME",
  "purchaser number": "PURCH_ID",
  "gl account": "ACCT_ID",
  "general ledger account": "ACCT_ID",
  "account number": "ACCT_ID",
  "account id": "ACCT_ID",
  "account code": "ACCT_ID",
  "account name": "ACCTNAME",
  "volume": "QUANTITY",
  "quantity": "QUANTITY",
  "mcf": "QUANTITY",
  "balance": "BALANCE",
  "modified by": "LAST_CHANGE_BY",
  "created by": "CREATED_BY",
  "invoice type": "DESCRIPTION"
};

export function safeMapFields(terms: any, type: string, columns: string[]): string[] {
  if (!terms) return [];
  const arr = Array.isArray(terms) ? terms : [terms];
  const mapped: string[] = [];
  arr.forEach((term) => {
    if (typeof term === "string") {
      const key = term.toLowerCase().trim();
      const mappedValue = columnMapping[key] || key;
      if (!columns.includes(mappedValue)) {
        console.warn(`⚠️ Unmapped ${type} field fallback: '${term}' → '${mappedValue}'`);
      } else {
        mapped.push(mappedValue);
      }
    }
  });
  return mapped;
}

export function extractExcludeClauses(filters: any, columns: string[]): string[] {
  const clauses: string[] = [];
  for (const [key, val] of Object.entries(filters || {})) {
    if (val && typeof val === "object" && "exclude" in val) {
      const field = columnMapping[key.toLowerCase()] || key;
      if (columns.map(c => c.toLowerCase()).includes(field.toLowerCase())) {
        const values = Array.isArray(val.exclude) ? val.exclude : [val.exclude];
        clauses.push(...values.map((v) => `${field} != '${v}'`));
      }
    }
  }
  return clauses;
}

export function extractKeywords(filters: any): string[] {
  const keywords: string[] = [];
  for (const [k, v] of Object.entries(filters || {})) {
    if (k.toLowerCase() === "keyword") {
      if (Array.isArray(v)) keywords.push(...v);
      else if (typeof v === "string") keywords.push(v);
    }
  }
  return keywords;
}

export async function getTableColumns(conn: any): Promise<string[]> {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText: `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'S3_GL' AND TABLE_SCHEMA = 'FINANCIAL'`,
      complete: (err: unknown, _stmt: any, rows: any[]) => {
        if (err) reject(err);
        else resolve(rows.map((r) => r.COLUMN_NAME));
      }
    });
  });
}
