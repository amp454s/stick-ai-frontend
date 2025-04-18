import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";


// Suppressing deprecation warning for util._extend (NODE_OPTIONS=--no-deprecation)
// Initialize clients
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

// Initialize Snowflake connection
const snowflakeConnection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT!,
  username: process.env.SNOWFLAKE_USER!,
  password: process.env.SNOWFLAKE_PASSWORD!,
  database: "STICK_DB",
  schema: "FINANCIAL",
  role: "STICK_ROLE",
  warehouse: "STICK_WH",
});

// Helper to fetch column names from S3_GL table
async function getTableColumns(): Promise<string[]> {
  return new Promise((resolve, reject) => {
    snowflakeConnection.execute({
      sqlText: `
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'S3_GL' AND TABLE_SCHEMA = 'FINANCIAL'
      `,
      complete: (err, stmt, rows) => {
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
          - data_type: The type of financial data (e.g., expenses, balances).
          - group_by: An array of fields to group by (e.g., ["PER_END_DATE"]).
          - filters: An object with fields and conditions (e.g., {"ACCTNAME": ["LIKE '%electric%'"]}).

          Return the result as a JSON object.
        `,
      },
      { role: "user", content: query },
    ],
  });

  return JSON.parse(response.choices[0].message.content || "{}");
}

// Helper to build Snowflake query dynamically
function buildSnowflakeQuery(interpretation: any, tableColumns: string[]): string {
  const { data_type, group_by, filters } = interpretation;

  const selectFields = group_by.length > 0 ? group_by.join(", ") + ", " : "";
  const aggregate = data_type === "expenses" ? "SUM(BALANCE)" : "BALANCE";

  let whereClause = "";
  if (filters) {
    whereClause = Object.entries(filters)
      .map(([field, condition]) => {
        if (Array.isArray(condition)) return `${field} ${condition[0]} ${condition[1]}`;
        return `${field} = '${condition}'`;
      })
      .join(" AND ");
  }

  const groupByClause = group_by.length > 0 ? `GROUP BY ${group_by.join(", ")}` : "";
  const orderByClause = group_by.length > 0 ? `ORDER BY ${group_by.join(", ")}` : "";

  return `
    SELECT ${selectFields}${aggregate} as TOTAL
    FROM STICK_DB.FINANCIAL.S3_GL
    ${whereClause ? `WHERE ${whereClause}` : ""}
    ${groupByClause}
    ${orderByClause}
    LIMIT 100
  `;
}

// Fusion Smart Retrieval Combiner
async function fusionSmartRetrieval(query: string, interpretation: any, tableColumns: string[]) {
  // Pinecone retrieval
  const pineconeQuery = `${interpretation.data_type} ${query}`;
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
  const pineconeData = pineconeResults.matches.map(match => {
    const metadata = match.metadata || {};
    return Object.entries(metadata).map(([key, value]) => `${key}: ${value || "n/a"}`).join("\n");
  });

  // Snowflake retrieval
  const snowflakeQuery = buildSnowflakeQuery(interpretation, tableColumns);
  const snowflakeResults = await new Promise<any[]>((resolve, reject) => {
    snowflakeConnection.execute({
      sqlText: snowflakeQuery,
      complete: (err, stmt, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      },
    });
  });
  const snowflakeData = snowflakeResults.map(row =>
    Object.entries(row).map(([key, value]) => `${key}: ${value || "n/a"}`).join("\n")
  );

  // Fusion logic: Prioritize Snowflake, supplement with Pinecone
  const hasSnowflakeData = snowflakeData.length > 0;
  const combinedData = hasSnowflakeData
    ? [...snowflakeData, ...(pineconeData.length > 0 ? [`Additional Context (Semantic):\n${pineconeData.join("\n\n")}`] : [])]
    : pineconeData;

  return {
    combinedText: combinedData.join("\n\n"),
    sourceNote: hasSnowflakeData ? "" : "Note: Results based on semantic search only, as structured data was unavailable."
  };
}

export async function POST(req: NextRequest) {
  try {
    const { query } = await req.json();
    if (!query) throw new Error("Query is required");

    // Interpret query
    const interpretation = await interpretQuery(query);

    // Connect to Snowflake
    await new Promise((resolve, reject) => {
      snowflakeConnection.connect((err, conn) => (err ? reject(err) : resolve(conn)));
    });

    // Fetch table columns
    const tableColumns = await getTableColumns();

    // Fusion retrieval
    const { combinedText, sourceNote } = await fusionSmartRetrieval(query, interpretation, tableColumns);

    // Generate sharp summary
    const summaryPrompt = `
      You are a financial assistant. Based on the user's query: '${query}', and the data below, provide a concise summary (2-3 sentences) that directly answers the query. Focus on key aspects like totals by period, vendor, or account, and avoid asking the user to interpret raw data. ${sourceNote}

      Data:
      ${combinedText}
    `.trim();

    const gptResponse = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        { role: "system", content: "You are a CPA assistant." },
        { role: "user", content: summaryPrompt },
      ],
    });

    const summary = gptResponse.choices[0].message.content;

    return NextResponse.json({ summary });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ message: "Error processing query", error: String(error) }, { status: 500 });
  } finally {
    snowflakeConnection.destroy((err) => err && console.error("Snowflake disconnect error:", err));
  }
}