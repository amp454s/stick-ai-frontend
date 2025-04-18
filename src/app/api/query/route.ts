import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

const columnMapping: { [key: string]: string } = {
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

function safeMapFields(terms: any, type: string): string[] {
  if (!terms) return [];
  const arr = Array.isArray(terms) ? terms : [terms];
  const mapped: string[] = [];

  arr.forEach((term) => {
    if (typeof term === "string") {
      const key = term.toLowerCase().trim();
      const mappedValue = columnMapping[key] || key;
      if (!columns.includes(mappedValue)) {
        console.warn(`⚠️ Unmapped ${type} field fallback: '${term}'`);
      } else {
        mapped.push(mappedValue);
      }
    }
  });

  return mapped;
}

function formatDate(value: any): string {
  const date = new Date(value);
  return isNaN(date.getTime()) ? value : `${date.getMonth() + 1}/${date.getDate()}/${date.getFullYear()}`;
}

function formatResultsAsTable(rows: any[]): string {
  if (!rows.length) return "No results found.";
  const headers = Object.keys(rows[0]);
  const headerRow = `| ${headers.join(" | ")} |`;
  const separatorRow = `| ${headers.map(() => "---").join(" | ")} |`;
  const dataRows = rows.map((row) => `| ${headers.map((key) => {
    const val = row[key];
    return val instanceof Date || key.toLowerCase().includes("date") ? formatDate(val) : val ?? "";
  }).join(" | ")} |`);
  return [headerRow, separatorRow, ...dataRows].join("\n");
}

async function interpretQuery(query: string): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `You are an expert in interpreting financial queries. Given a user's query, extract:
- data_type: 'expenses', 'balances', etc.
- group_by: array of human-readable field names
- filters: keyword-based or explicit column filters (can include exclude subobject)
- mode: 'summary' or 'search'
Return a valid JSON object.`,
      },
      { role: "user", content: query },
    ],
  });

  const content = response.choices[0].message.content || "";
  console.log("Raw GPT interpretation output:", content);
  const parsed = JSON.parse(content);

  return {
    ...parsed,
    group_by: safeMapFields(parsed.group_by, "group_by"),
    filters: parsed.filters,
    exclude: parsed.filters?.exclude ? safeMapFields(parsed.filters.exclude, "exclude") : [],
  };
}

function buildSnowflakeQuery(interpretation: any, columns: string[], isRaw = false): string {
  const groupBy = (interpretation.group_by || []).filter((col: string) => columns.includes(col));
  const filters = interpretation.filters || {};
  const keyword = filters.keyword || (Array.isArray(filters) ? filters.find((f: any) => typeof f === "string") : "");
  const excludeFilters = filters.exclude || (Array.isArray(filters) ? filters.find((f: any) => typeof f === "object" && f.exclude) : {});

  const excludeClauses = Object.entries(excludeFilters || {}).map(([k, v]) => {
    const field = columnMapping[k.toLowerCase()] || k;
    return Array.isArray(v) ? v.map(val => `${field} != '${val}'`).join(" AND ") : `${field} != '${v}'`;
  });

  let whereClause = "";
  if (keyword) {
    const k = keyword.toLowerCase();
    whereClause = `(DESCRIPTION ILIKE '%${k}%' OR VENDORNAME ILIKE '%${k}%' OR ACCTNAME ILIKE '%${k}%' OR ANNOTATION ILIKE '%${k}%')`;
  }
  if (excludeClauses.length) {
    whereClause += (whereClause ? " AND " : "") + excludeClauses.join(" AND ");
  }

  if (isRaw) {
    return `SELECT * FROM STICK_DB.FINANCIAL.S3_GL ${whereClause ? `WHERE ${whereClause}` : ""} LIMIT 100`;
  }

  const selectFields = groupBy.length ? groupBy.join(", ") + ", " : "";
  const groupByClause = groupBy.length ? `GROUP BY ${groupBy.join(", ")}` : "";
  const orderByClause = groupBy.length ? `ORDER BY ${groupBy.join(", ")}` : "";

  return `SELECT ${selectFields}SUM(BALANCE) AS TOTAL FROM STICK_DB.FINANCIAL.S3_GL ${whereClause ? `WHERE ${whereClause}` : ""} ${groupByClause} ${orderByClause} LIMIT 100`;
}

function runSnowflakeQuery(conn: any, sqlText: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    console.log("Executing query:\n", sqlText);
    conn.execute({
      sqlText,
      complete: (err: any, _stmt: any, rows: any[]) => {
        if (err) reject(err);
        else resolve(rows);
      },
    });
  });
}

async function getTableColumns(conn: any): Promise<string[]> {
  const rows = await runSnowflakeQuery(conn, `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'S3_GL' AND TABLE_SCHEMA = 'FINANCIAL'`);
  return rows.map((r) => r.COLUMN_NAME);
}

async function fusionSmartRetrieval(query: string, interpretation: any, tableColumns: string[], conn: any) {
  const isSummary = interpretation.mode === "summary" || interpretation.mode === "Summarize";
  const aggSQL = buildSnowflakeQuery(interpretation, tableColumns, false);
  const rawSQL = buildSnowflakeQuery(interpretation, tableColumns, true);
  const aggResults = await runSnowflakeQuery(conn, aggSQL);
  const rawResults = await runSnowflakeQuery(conn, rawSQL);

  const summaryTable = formatResultsAsTable(aggResults);
  const rawDataTable = formatResultsAsTable(rawResults);

  return {
    combinedTextForSummary: summaryTable,
    rawDataText: rawDataTable,
    sourceNote: !aggResults.length ? "Note: no structured summary was available." : "",
  };
}

export async function POST(req: NextRequest) {
  let conn: any;
  try {
    const { query } = await req.json();
    if (!query) throw new Error("Query is required");
    const interpretation = await interpretQuery(query);
    console.log("Interpretation:", interpretation);

    conn = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT!,
      username: process.env.SNOWFLAKE_USER!,
      password: process.env.SNOWFLAKE_PASSWORD!,
      database: "STICK_DB",
      schema: "FINANCIAL",
      role: "STICK_ROLE",
      warehouse: "STICK_WH",
    });

    await new Promise<void>((res, rej) => conn.connect((err: unknown) => (err ? rej(err) : res())));
    const columns = await getTableColumns(conn);
    const { combinedTextForSummary, rawDataText, sourceNote } = await fusionSmartRetrieval(query, interpretation, columns, conn);

    const summaryPrompt = `You are a financial assistant. Based on the user's query: '${query}', and the aggregated data below, provide a concise summary (2–3 sentences).\n\nAggregated Data:\n${combinedTextForSummary}\n\n${sourceNote}`;

    const gptResponse = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        { role: "system", content: "You are a CPA assistant." },
        { role: "user", content: summaryPrompt },
      ],
    });

    return NextResponse.json({
      summary: gptResponse.choices[0].message.content,
      rawData: rawDataText,
    });
  } catch (err) {
    console.error("Error:", err);
    return NextResponse.json({ error: String(err) }, { status: 500 });
  } finally {
    if (conn) conn.destroy((err: any) => err && console.error("Disconnect error:", err));
  }
}
