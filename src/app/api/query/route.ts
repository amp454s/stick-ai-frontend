import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

// Full expanded column mapping
const columnMapping: { [key: string]: string } = {
  "gl identifier": "UTM_ID",
  "company": "CO_ID",
  "company name": "NAME",
  "name": "NAME",
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
  "afe": "AFE_ID",
  "gas purchaser": "PURCH_ID",
  "purchaser number": "PURCH_ID",
  "purchaser": "PURCHNAME",
  "gl account": "ACCT_ID",
  "general ledger account": "ACCT_ID",
  "account number": "ACCT_ID",
  "account id": "ACCT_ID",
  "account name": "ACCTNAME",
  "account": "ACCTNAME",
  "mcf": "QUANTITY",
  "volume": "QUANTITY",
  "quantity": "QUANTITY",
  "balance": "BALANCE",
  "modified by": "LAST_CHANGE_BY",
  "created by": "CREATED_BY"
};

function runSnowflakeQuery(connection: any, sqlText: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    console.log("Executing query:\n", sqlText);
    connection.execute({
      sqlText,
      complete: (err: Error | null, _stmt: any, rows: any[]) => {
        if (err) {
          console.error("Snowflake query error:", err);
          reject(err);
        } else {
          console.log(`Query successful. Rows returned: ${rows.length}`);
          resolve(rows || []);
        }
      },
    });
  });
}

async function getTableColumns(connection: any): Promise<string[]> {
  return runSnowflakeQuery(
    connection,
    `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'S3_GL' AND TABLE_SCHEMA = 'FINANCIAL'`
  ).then((rows) => rows.map((row: any) => row.COLUMN_NAME));
}

function sanitizeKeyword(keyword: string): string[] {
  return keyword.split(/\s+/).map((term) => term.trim()).filter(Boolean);
}

function buildSnowflakeQuery(interpretation: any, tableColumns: string[], isRaw: boolean = false): string {
  const group_by = (interpretation.group_by || [])
    .map((term: string) => columnMapping[term.toLowerCase()] || term)
    .filter((col: string) => tableColumns.includes(col));

  const filters = interpretation.filters || {};
  const resolvedFilters: Record<string, any> = {};
  const excludeFilters: Record<string, any> = {};

  for (const [key, val] of Object.entries(filters.exclude || {})) {
    const mapped = columnMapping[key.toLowerCase()] || key;
    if (tableColumns.includes(mapped)) excludeFilters[mapped] = val;
  }

  for (const [key, val] of Object.entries(filters)) {
    if (key === "exclude") continue;
    const mapped = columnMapping[key.toLowerCase()] || key;
    if (tableColumns.includes(mapped)) resolvedFilters[mapped] = val;
  }

  const conditions: string[] = [];

  if (filters.keyword) {
    const terms = sanitizeKeyword(filters.keyword);
    const likeClauses = terms.map(term => `(
      ACCTNAME ILIKE '%${term}%'
      OR DESCRIPTION ILIKE '%${term}%'
      OR ANNOTATION ILIKE '%${term}%'
    )`);
    conditions.push(`(${likeClauses.join(" OR ")})`);
  }

  for (const [field, condition] of Object.entries(resolvedFilters)) {
    conditions.push(`${field} = '${condition}'`);
  }

  for (const [field, value] of Object.entries(excludeFilters)) {
    conditions.push(`${field} != '${value}'`);
  }

  const whereClause = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

  const selectFields = group_by.length ? group_by.join(", ") + ", " : "";
  const groupByClause = group_by.length ? `GROUP BY ${group_by.join(", ")}` : "";
  const orderByClause = group_by.length ? `ORDER BY ${group_by.join(", ")}` : "";

  if (isRaw) {
    return `SELECT * FROM STICK_DB.FINANCIAL.S3_GL ${whereClause} LIMIT 100`;
  }

  return `
    SELECT ${selectFields}SUM(BALANCE) AS TOTAL
    FROM STICK_DB.FINANCIAL.S3_GL
    ${whereClause}
    ${groupByClause}
    ${orderByClause}
    LIMIT 100
  `.trim();
}

function formatResultsAsTable(rows: any[]): string {
  if (!rows.length) return "No results found.";
  const headers = Object.keys(rows[0]);
  const headerRow = `| ${headers.join(" | ")} |`;
  const separatorRow = `| ${headers.map(() => "---").join(" | ")} |`;

  const dataRows = rows.map((row) => {
    return `| ${headers.map((key) => {
      const val = row[key];
      if (val instanceof Date) {
        return `${val.getMonth() + 1}/${val.getDate()}/${val.getFullYear()}`;
      }
      return val;
    }).join(" | ")} |`;
  });

  return [headerRow, separatorRow, ...dataRows].join("\n");
}

async function interpretQuery(query: string): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `You are an expert in interpreting financial queries. Extract { data_type, group_by, filters, mode } from the prompt.`
      },
      { role: "user", content: query }
    ]
  });

  const rawContent = response.choices[0].message.content || "";
  console.log("Raw GPT interpretation output:", rawContent);

  try {
    return JSON.parse(rawContent);
  } catch (e) {
    throw new Error(`Failed to parse GPT interpretation:\n${rawContent}`);
  }
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
      warehouse: "STICK_WH"
    });

    await new Promise((resolve, reject) => {
      connection.connect((err: Error | null, conn: any) => (err ? reject(err) : resolve(conn)));
    });

    const tableColumns = await getTableColumns(connection);
    const sqlQuery = buildSnowflakeQuery(interpretation, tableColumns);
    const rows = await runSnowflakeQuery(connection, sqlQuery);

    const rawDataText = formatResultsAsTable(rows);

    const summaryPrompt = `
      You are a financial assistant. Based on the user's query: '${query}', and the aggregated data below, provide a concise summary (2–3 sentences) that directly answers the query.

      Aggregated Data:
      ${rawDataText}
    `.trim();

    const gptResponse = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        { role: "system", content: "You are a CPA assistant." },
        { role: "user", content: summaryPrompt }
      ]
    });

    return NextResponse.json({
      summary: gptResponse.choices[0].message.content,
      rawData: rawDataText
    });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ error: String(error) }, { status: 500 });
  } finally {
    if (connection) {
      connection.destroy((err: Error | null) => err && console.error("Disconnect error:", err));
    }
  }
}
