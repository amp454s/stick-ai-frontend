import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

// Expanded column mapping
const columnMapping: { [key: string]: string } = {
  // Direct mappings and synonyms
  "gl identifier": "UTM_ID",
  "company": "CO_ID",
  "company name": "NAME",
  "accountingid": "WELLCODE",
  "well code": "WELLCODE",
  "cost center": "WELLCODE",
  "well name": "WELL_NAME",
  "cost center name": "WELL_NAME",
  "accounting period": "PER_END_DATE",
  "month end": "PER_END_DATE",
  "posting date": "POSTING_DATE",
  "accounting date": "LOS_PRODUCTIONDATE",
  "activity date": "LOS_PRODUCTIONDATE",
  "excalibur module": "SYSTEM_CODE",
  "journal entry": "VOUCHER",
  "vendor": "VENDORNAME",
  "AFE": "AFE_ID",
  "AFE name": "AFENAME",
  "gas purchaser": "PURCH_ID",
  "purchaser number": "PURCH_ID",
  "purchaser": "PURCHNAME",
  "gl account": "ACCT_ID",
  "general ledger account": "ACCT_ID",
  "account number": "ACCT_ID",
  "account name": "ACCTNAME",
  "volume": "QUANTITY",
  "mcf": "QUANTITY",
  "quantity": "QUANTITY",
  "balance": "BALANCE",
  "modified by": "LAST_CHANGE_BY",
  "created by": "CREATED_BY",
  "description": "DESCRIPTION",
  "annotation": "ANNOTATION",
  "document": "DOCUMENT"
};

function runSnowflakeQuery(connection: any, sqlText: string): Promise<any[]> {
  console.log("Running SQL:\n" + sqlText);
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      complete: (err: Error | null, _stmt: any, rows: any[]) => {
        if (err) {
          console.error("Snowflake query error:", err);
          reject(err);
        } else {
          resolve(rows || []);
        }
      },
    });
  });
}

async function getTableColumns(connection: any): Promise<string[]> {
  const rows = await runSnowflakeQuery(
    connection,
    `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'S3_GL' AND TABLE_SCHEMA = 'FINANCIAL'`
  );
  return rows.map((row: any) => row.COLUMN_NAME);
}

async function interpretQuery(query: string): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `
          You are an expert in interpreting financial queries.
          Given a user's query, extract:
          - data_type: 'expenses', 'balances', etc.
          - group_by: array of human-readable field names
          - filters: e.g., { keyword: 'electric' } or { vendor: 'Exxon' }
          - mode: 'summary' if the user wants a summarized report; otherwise 'search'
          Return a valid JSON object.
        `,
      },
      { role: "user", content: query },
    ],
  });

  const rawContent = response.choices[0].message.content || "";
  console.log("Raw GPT interpretation output:", rawContent);

  try {
    return JSON.parse(rawContent);
  } catch (e) {
    throw new Error(`Failed to parse GPT interpretation:\n${rawContent}`);
  }
}

function buildSnowflakeQuery(interpretation: any, tableColumns: string[], isRaw = false): string {
  const group_by_raw = interpretation.group_by || [];
  const filters = interpretation.filters || {};

  const group_by: string[] = group_by_raw.map((term: string) => {
    const lower = term.toLowerCase();
    const mapped = columnMapping[lower];
    if (!mapped) console.warn(`⚠️  Unknown mapping for group_by term: '${term}'`);
    return mapped || term;
  }).filter((col: string) => tableColumns.includes(col));

  const resolvedFilters = Object.entries(filters).reduce((acc: Record<string, any>, [key, val]) => {
    const mapped = columnMapping[key.toLowerCase()] || key;
    if (tableColumns.includes(mapped)) acc[mapped] = val;
    return acc;
  }, {});

  let whereClause = "";
  if (filters.keyword) {
    const keyword = filters.keyword.toLowerCase();
    whereClause = `(DESCRIPTION ILIKE '%${keyword}%' OR ANNOTATION ILIKE '%${keyword}%' OR DOCUMENT ILIKE '%${keyword}%')`;
  } else if (Object.keys(resolvedFilters).length) {
    whereClause = Object.entries(resolvedFilters)
      .map(([field, condition]) => {
        if (Array.isArray(condition) && condition.length === 2) {
          return `${field} ${condition[0]} '${condition[1]}'`;
        }
        return `${field} = '${condition}'`;
      })
      .join(" AND ");
  }

  const selectAllFields = isRaw ? "*" : `${group_by.join(", ")}${group_by.length ? ", " : ""}SUM(BALANCE) AS TOTAL`;
  const groupByClause = isRaw || !group_by.length ? "" : `GROUP BY ${group_by.join(", ")}`;
  const orderByClause = isRaw || !group_by.length ? "" : `ORDER BY ${group_by.join(", ")}`;

  return `/* Columns used in query: ${group_by.join(", ")} */\nSELECT ${selectAllFields}\nFROM STICK_DB.FINANCIAL.S3_GL\n${whereClause ? `WHERE ${whereClause}` : ""}\n${groupByClause}\n${orderByClause}\nLIMIT 100`;
}

function formatResultsAsTable(rows: any[]): string {
  if (!rows.length) return "No results found.";
  const headers = Object.keys(rows[0]);
  const headerRow = `| ${headers.join(" | ")} |`;
  const separatorRow = `| ${headers.map(() => "---").join(" | ")} |`;
  const dataRows = rows.map(row => `| ${headers.map(h => `${row[h] ?? ""}`).join(" | ")} |`);
  return [headerRow, separatorRow, ...dataRows].join("\n");
}

export async function POST(req: NextRequest) {
  let connection: any;
  try {
    const { query } = await req.json();
    if (!query) throw new Error("Query is required");

    const interpretation = await interpretQuery(query);
    console.log("Interpretation:", interpretation);

    connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT!,
      username: process.env.SNOWFLAKE_USER!,
      password: process.env.SNOWFLAKE_PASSWORD!,
      database: "STICK_DB",
      schema: "FINANCIAL",
      role: "STICK_ROLE",
      warehouse: "STICK_WH",
    });

    await new Promise((resolve, reject) => {
      connection.connect((err: Error | null, conn: any) => (err ? reject(err) : resolve(conn)));
    });

    const tableColumns = await getTableColumns(connection);
    const snowflakeQuery = buildSnowflakeQuery(interpretation, tableColumns);
    const results = await runSnowflakeQuery(connection, snowflakeQuery);

    const formatted = formatResultsAsTable(results);

    const summaryPrompt = `You are a CPA assistant. Based on the user's query: '${query}', summarize the following:\n\n${formatted}`;
    const gpt = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        { role: "system", content: "You are a CPA assistant." },
        { role: "user", content: summaryPrompt },
      ],
    });

    return NextResponse.json({
      summary: gpt.choices[0].message.content,
      rawData: formatted,
    });
  } catch (err) {
    console.error("Error:", err);
    return NextResponse.json({ error: String(err) }, { status: 500 });
  } finally {
    if (connection) connection.destroy(() => {});
  }
}