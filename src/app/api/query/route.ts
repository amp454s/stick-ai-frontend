// route.ts
import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

// Expanded column mapping
const columnMapping: { [key: string]: string } = {
  // Custom aliases
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
  "afe": "AFE_ID",
  "gas purchaser": "PURCH_ID",
  "purchaser number": "PURCH_ID",
  "purchaser": "PURCHNAME",
  "gl account": "ACCT_ID",
  "general ledger account": "ACCT_ID",
  "account number": "ACCT_ID",
  "account name": "ACCTNAME",
  "mcf": "QUANTITY",
  "volume": "QUANTITY",
  "quantity": "QUANTITY",
  "modified by": "LAST_CHANGE_BY",
  "created by": "CREATED_BY",
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

async function interpretQuery(query: string): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `You are an expert in interpreting financial queries. Given a user's query, extract:
        - data_type: 'expenses', 'balances', etc.
        - group_by: array of human-readable field names
        - filters: { keyword: ..., exclude: { ... } }
        - mode: 'summary' or 'search'
        Return valid JSON.`
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
  const data_type = interpretation.data_type || "balances";
  const group_by = (interpretation.group_by || [])
    .map((term: string) => columnMapping[term.toLowerCase()] || term)
    .filter((col: string) => tableColumns.includes(col));

  const filters = interpretation.filters || {};
  const includeFilters = filters || {};
  const excludeFilters = (filters.exclude || {}) as Record<string, any>;

  const resolvedIncludes = Object.entries(includeFilters).reduce((acc: Record<string, any>, [key, val]) => {
    if (key === "exclude") return acc;
    const mapped = columnMapping[key.toLowerCase()] || key;
    if (tableColumns.includes(mapped)) acc[mapped] = val;
    return acc;
  }, {});

  const resolvedExcludes = Object.entries(excludeFilters).reduce((acc: Record<string, any>, [key, val]) => {
    const mapped = columnMapping[key.toLowerCase()] || key;
    if (tableColumns.includes(mapped)) acc[mapped] = val;
    return acc;
  }, {});

  console.log("✅ Mapped include filters:", resolvedIncludes);
  console.log("🚫 Mapped exclude filters:", resolvedExcludes);

  let whereClause = "";

  const keywordClause = filters.keyword
    ? `(ACCTNAME ILIKE '%${filters.keyword}%' OR DESCRIPTION ILIKE '%${filters.keyword}%' OR ANNOTATION ILIKE '%${filters.keyword}%')`
    : "";

  const includeClause = Object.entries(resolvedIncludes)
    .map(([field, value]) => `${field} = '${value}'`)
    .join(" AND ");

  const excludeClause = Object.entries(resolvedExcludes)
    .map(([field, value]) => `${field} != '${value}'`)
    .join(" AND ");

  const whereParts = [keywordClause, includeClause, excludeClause].filter(Boolean);
  if (whereParts.length) whereClause = `WHERE ${whereParts.join(" AND ")}`;

  if (isRaw) {
    return `SELECT * FROM STICK_DB.FINANCIAL.S3_GL ${whereClause} LIMIT 100`;
  }

  const selectFields = group_by.length ? `${group_by.join(", ")}, ` : "";
  const groupClause = group_by.length ? `GROUP BY ${group_by.join(", ")}` : "";
  const orderClause = group_by.length ? `ORDER BY ${group_by.join(", ")}` : "";

  return `SELECT ${selectFields}SUM(BALANCE) AS TOTAL FROM STICK_DB.FINANCIAL.S3_GL ${whereClause} ${groupClause} ${orderClause} LIMIT 100`;
}

function formatResultsAsTable(rows: any[]): string {
  if (!rows.length) return "No results found.";

  const headers = Object.keys(rows[0]);
  const headerRow = `| ${headers.join(" | ")} |`;
  const separator = `| ${headers.map(() => "---").join(" | ")} |`;

  const dataRows = rows.map(row => `| ${headers.map(h => formatCell(row[h])).join(" | ")} |`);

  return [headerRow, separator, ...dataRows].join("\n");
}

function formatCell(value: any): string {
  if (value instanceof Date) {
    return value.toLocaleDateString("en-US");
  }
  if (typeof value === "string" && value.includes("GMT")) {
    const date = new Date(value);
    return isNaN(date.getTime()) ? value : date.toLocaleDateString("en-US");
  }
  return String(value);
}

async function fusionSmartRetrieval(query: string, interpretation: any, tableColumns: string[], connection: any) {
  const isSummary = interpretation.mode === "summary";

  const snowflakeAggQuery = buildSnowflakeQuery(interpretation, tableColumns, false);
  const snowflakeAggResults = await runSnowflakeQuery(connection, snowflakeAggQuery);
  const aggTable = formatResultsAsTable(snowflakeAggResults);

  if (isSummary) {
    return { combinedTextForSummary: aggTable, rawDataText: aggTable, sourceNote: "" };
  }

  const pineconeQuery = `${interpretation.data_type || "financial"} ${query}`;
  const embedding = await openai.embeddings.create({ input: pineconeQuery, model: "text-embedding-3-small" });
  const queryVector = embedding.data[0].embedding;

  const pineconeResults = await index.namespace("default").query({ vector: queryVector, topK: 3, includeMetadata: true });
  const pineconeText = pineconeResults.matches.map((m, i) => `Pinecone Result ${i + 1}:\n${Object.entries(m.metadata || {}).map(([k, v]) => `${k}: ${v}`).join("\n")}`).join("\n\n");

  const snowflakeRawQuery = buildSnowflakeQuery(interpretation, tableColumns, true);
  const snowflakeRawResults = await runSnowflakeQuery(connection, snowflakeRawQuery);
  const rawData = snowflakeRawResults.map((row, i) => `Snowflake Row ${i + 1}:\n${Object.entries(row).map(([k, v]) => `${k}: ${v}`).join("\n")}`).join("\n\n");

  return {
    combinedTextForSummary: [aggTable, pineconeText].join("\n\n"),
    rawDataText: [pineconeText, rawData].join("\n\n"),
    sourceNote: snowflakeAggResults.length ? "" : "Note: results based on semantic search only."
  };
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
    const { combinedTextForSummary, rawDataText, sourceNote } = await fusionSmartRetrieval(query, interpretation, tableColumns, connection);

    const summaryPrompt = `You are a financial assistant. Given this user query: '${query}', and this data:\n${combinedTextForSummary}\n\nNote: ${sourceNote}`;

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
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ error: String(error) }, { status: 500 });
  } finally {
    if (connection) {
      connection.destroy((err: Error | null) => err && console.error("Disconnect error:", err));
    }
  }
}
