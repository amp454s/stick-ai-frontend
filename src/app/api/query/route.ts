import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

// Initialize clients
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

// Column mapping for common terms to actual column names
const columnMapping: { [key: string]: string } = {
  "accounting period": "PER_END_DATE",
  "vendor": "VENDORNAME",
  "account name": "ACCTNAME",
};

// Helper to fetch column names from S3_GL table
async function getTableColumns(connection: any): Promise<string[]> {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: `
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'S3_GL' AND TABLE_SCHEMA = 'FINANCIAL'
      `,
      complete: (err: Error | null, stmt: any, rows: any[]) => {
        if (err) reject(err);
        else resolve((rows || []).map(row => row.COLUMN_NAME));
      },
    });
  });
}

// Helper to interpret the query using GPT-4
async function interpretQuery(query: string): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `
          You are an expert in interpreting financial queries.
          Given a user's query, extract the following:
          - data_type: The type of financial data (e.g., 'expenses', 'balances').
          - group_by: An array of fields to group by (e.g., ['accounting period']).
          - filters: An object with a 'keyword' for filtering (e.g., { keyword: 'electric' } if the query mentions 'electric expenses').
          Return the result as a JSON object. If unsure, provide default values like { data_type: 'balances', group_by: [], filters: {} }.
        `,
      },
      { role: "user", content: query },
    ],
  });
  return JSON.parse(response.choices[0].message.content || "{}");
}

// Helper to build Snowflake query dynamically
function buildSnowflakeQuery(interpretation: any, tableColumns: string[], isRaw: boolean = false): string {
  const data_type = interpretation.data_type || "balances";
  const group_by = (interpretation.group_by || []).map((term: string) => columnMapping[term.toLowerCase()] || term).filter((col: string) => tableColumns.includes(col));
  const filters = interpretation.filters || {};

  let whereClause = "";
  if (filters.keyword) {
    const keyword = filters.keyword;
    whereClause = `(ACCTNAME LIKE '%${keyword}%' OR DESCRIPTION LIKE '%${keyword}%' OR ANNOTATION LIKE '%${keyword}%')`;
  } else if (filters && typeof filters === "object") {
    whereClause = Object.entries(filters)
      .filter(([field, condition]) => tableColumns.includes(field) && condition)
      .map(([field, condition]) => {
        if (Array.isArray(condition) && condition.length === 2) {
          return `${field} ${condition[0]} '${condition[1]}'`;
        }
        return `${field} = '${condition}'`;
      })
      .join(" AND ");
  }

  if (isRaw) {
    return `
      SELECT *
      FROM STICK_DB.FINANCIAL.S3_GL
      ${whereClause ? `WHERE ${whereClause}` : ""}
      LIMIT 100
    `.trim();
  } else {
    const selectFields = group_by.length > 0 ? group_by.join(", ") + ", " : "";
    const aggregate = data_type === "expenses" ? "SUM(BALANCE)" : "BALANCE";
    const groupByClause = group_by.length > 0 ? `GROUP BY ${group_by.join(", ")}` : "";
    const orderByClause = group_by.length > 0 ? `ORDER BY ${group_by.join(", ")}` : "";
    return `
      SELECT ${selectFields}${aggregate} as TOTAL
      FROM STICK_DB.FINANCIAL.S3_GL
      ${whereClause ? `WHERE ${whereClause}` : ""}
      ${groupByClause}
      ${orderByClause}
      LIMIT 100
    `.trim();
  }
}

// Fusion Smart Retrieval Combiner
async function fusionSmartRetrieval(query: string, interpretation: any, tableColumns: string[], connection: any) {
  const pineconeQuery = `${interpretation.data_type || "financial"} ${query}`;
  const embeddingResponse = await openai.embeddings.create({
    input: pineconeQuery,
    model: "text-embedding-3-small",
  });
  const queryVector = embeddingResponse.data[0].embedding;

  const pineconeResults = await index.namespace("default").query({
    vector: queryVector,
    topK: 3,
    includeMetadata: true,
  });
  console.log("Pinecone matches:", pineconeResults.matches.length);
  const pineconeData = pineconeResults.matches.map((match, i) => {
    const metadata = match.metadata || {};
    return `Pinecone Result ${i + 1}:\n${Object.entries(metadata).map(([key, value]) => `${key}: ${value || "n/a"}`).join("\n")}`;
  });

  // Fetch aggregated data for summary
  const snowflakeAggQuery = buildSnowflakeQuery(interpretation, tableColumns, false);
  console.log("Snowflake Aggregated Query:", snowflakeAggQuery);
  const snowflakeAggResults = await new Promise<any[]>((resolve, reject) => {
    connection.execute({
      sqlText: snowflakeAggQuery,
      complete: (err: Error | null, stmt: any, rows: any[]) => {
        if (err) reject(err);
        else resolve(rows || []);
      },
    });
  });
  console.log("Snowflake aggregated rows returned:", snowflakeAggResults.length);
  if (snowflakeAggResults.length > 0) {
    console.log("First aggregated row:", snowflakeAggResults[0]);
  }
  const aggData = snowflakeAggResults.map((row, i) =>
    `Aggregated Result ${i + 1}:\n${Object.entries(row).map(([key, value]) => `${key}: ${value || "n/a"}`).join("\n")}`
  );

  // Fetch raw data
  const snowflakeRawQuery = buildSnowflakeQuery(interpretation, tableColumns, true);
  console.log("Snowflake Raw Query:", snowflakeRawQuery);
  const snowflakeRawResults = await new Promise<any[]>((resolve, reject) => {
    connection.execute({
      sqlText: snowflakeRawQuery,
      complete: (err: Error | null, stmt: any, rows: any[]) => {
        if (err) reject(err);
        else resolve(rows || []);
      },
    });
  });
  console.log("Snowflake raw rows returned:", snowflakeRawResults.length);
  if (snowflakeRawResults.length > 0) {
    console.log("First raw row:", snowflakeRawResults[0]);
  }
  const rawData = snowflakeRawResults.map((row, i) =>
    `Raw Result ${i + 1}:\n${Object.entries(row).map(([key, value]) => `${key}: ${value || "n/a"}`).join("\n")}`
  );

  // Combine for summary
  const combinedTextForSummary = [...aggData, ...(pineconeData.length > 0 ? [`Additional Context (Semantic):\n${pineconeData.join("\n\n")}`] : [])].join("\n\n");
  console.log("Combined Text for Summary:", combinedTextForSummary);

  // Raw data text
  const rawDataText = rawData.length > 0 ? rawData.join("\n\n") : "No raw data available from Snowflake.";

  return {
    combinedTextForSummary,
    rawDataText,
    sourceNote: aggData.length > 0 ? "" : "Note: Results based on semantic search only, as structured data was unavailable."
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

    const summaryPrompt = `
      You are a financial assistant. Based on the user's query: '${query}', and the aggregated data below, provide a concise summary (2-3 sentences) that directly answers the query. Focus on key aspects like totals by period, vendor, or account.

      Aggregated Data:
      ${combinedTextForSummary}

      Note: ${sourceNote}
    `.trim();

    const gptResponse = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        { role: "system", content: "You are a CPA assistant." },
        { role: "user", content: summaryPrompt },
      ],
    });

    const summary = gptResponse.choices[0].message.content;
    console.log("Summary:", summary);
    console.log("Raw Data:", rawDataText);
    return NextResponse.json({ summary, rawData: rawDataText });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ message: "Error processing query", error: String(error) }, { status: 500 });
  } finally {
    if (connection) {
      connection.destroy((err: Error | null) => err && console.error("Snowflake disconnect error:", err));
    }
  }
}