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

// Mapping of common terms to column names
const columnMapping: { [key: string]: string } = {
  "accounting period": "PER_END_DATE",
  "period": "PER_END_DATE",
  "vendor": "VENDORNAME",
  "account name": "ACCTNAME",
  "account": "ACCT_ID",
  "date": "POSTING_DATE",
  "well": "WELL_NAME",
  "company": "NAME",
};

// Helper to map query terms to column names
function mapQueryTermToColumn(term: string, columns: string[]): string {
  term = term.toLowerCase().trim();
  if (columnMapping[term]) return columnMapping[term];
  const column = columns.find(col => col.toLowerCase() === term);
  if (column) return column;
  const guessedColumn = term.replace(/\s+/g, "_").toUpperCase();
  return columns.includes(guessedColumn) ? guessedColumn : "";
}

export async function POST(req: NextRequest) {
  try {
    const { query } = await req.json();

    if (!query) {
      return NextResponse.json({ message: "Query is required" }, { status: 400 });
    }

    // Refine query input for Pinecone
    const refinedQuery = `electrical expenses ${query}`;
    const embeddingResponse = await openai.embeddings.create({
      input: refinedQuery,
      model: "text-embedding-3-small",
    });
    const queryVector = embeddingResponse.data[0].embedding;

    // Query Pinecone
    const namespaces = ["default"];
    const topK = 10;

    const pineconeResults = await Promise.all(
      namespaces.map(async (namespace) => {
        const result = await index.namespace(namespace).query({
          vector: queryVector,
          topK,
          includeMetadata: true,
        });
        return result.matches;
      })
    );

    const combinedPineconeResults = pineconeResults
      .flat()
      .sort((a, b) => (b.score || 0) - (a.score || 0))
      .slice(0, 3);

    const pineconeData = combinedPineconeResults.map((match) => {
      const metadata = match.metadata || {};
      return Object.entries(metadata)
        .map(([key, value]) => `${key}: ${value || "n/a"}`)
        .join("\n");
    });

    // Connect to Snowflake
    await new Promise((resolve, reject) => {
      snowflakeConnection.connect((err, conn) => {
        if (err) reject(err);
        else resolve(conn);
      });
    });

    // Fetch column names from S3_GL
    const tableColumns = await getTableColumns();

    // Parse query for aggregation
    const queryLower = query.toLowerCase();
    const needsAggregation =
      queryLower.includes("summarize") ||
      queryLower.includes("total") ||
      queryLower.includes("by");

    let snowflakeQuery;
    let snowflakeData: string[] = [];
    if (needsAggregation) {
      const byMatch = queryLower.match(/by\s+(.+)/);
      const groupByFields: string[] = [];

      if (byMatch) {
        const byClause = byMatch[1];
        const terms = byClause.split(/and|,/).map((term: string) => term.trim());
        for (const term of terms) {
          const column = mapQueryTermToColumn(term, tableColumns);
          if (column && !groupByFields.includes(column)) {
            groupByFields.push(column);
          }
        }
      }

      const groupByClause = groupByFields.length > 0 ? `GROUP BY ${groupByFields.join(", ")}` : "";
      const orderByClause = groupByFields.length > 0 ? `ORDER BY ${groupByFields.join(", ")}` : "";
      const whereClauses = `
        (ACCTNAME LIKE '%electric%' OR DESCRIPTION LIKE '%electric%' OR ANNOTATION LIKE '%electric%')
      `;

      snowflakeQuery = `
        SELECT ${groupByFields.join(", ")}${groupByFields.length > 0 ? ", " : ""}SUM(BALANCE) as TOTAL_BALANCE
        FROM S3_GL
        WHERE ${whereClauses}
        ${groupByClause}
        ${orderByClause}
      `;
    } else {
      const whereClauses = `
        (ACCTNAME LIKE '%electric%' OR DESCRIPTION LIKE '%electric%' OR ANNOTATION LIKE '%electric%')
      `;

      snowflakeQuery = `
        SELECT *
        FROM S3_GL
        WHERE ${whereClauses}
        LIMIT 10
      `;
    }

    console.log("Executing Snowflake Query:", snowflakeQuery);

    try {
      const snowflakeStartTime = Date.now();
      const snowflakeResults = await new Promise<any[]>((resolve, reject) => {
        snowflakeConnection.execute({
          sqlText: snowflakeQuery,
          complete: (err, stmt, rows) => {
            if (err) reject(err);
            else resolve(rows || []);
          },
        });
      });
      console.log("Snowflake Query Time:", Date.now() - snowflakeStartTime, "ms");

      snowflakeData = snowflakeResults.map((row) =>
        Object.entries(row)
          .map(([key, value]) => `${key}: ${value || "n/a"}`)
          .join("\n")
      );
    } catch (error) {
      console.error("Snowflake query failed, proceeding with Pinecone data:", error);
      snowflakeData = ["Snowflake query failed: " + String(error)];
    }

    // Combine Pinecone and Snowflake data
    const combinedData = [
      ...pineconeData.map((data, i) => `Pinecone Result ${i + 1}:\n${data}`),
      ...snowflakeData.map((data, i) => `Snowflake Result ${i + 1}:\n${data}`),
    ].join("\n\n");

    console.log("Combined Data for Query:", combinedData);

    // Generate GPT summary
    const summaryPrompt = `
You are reviewing accounting data based on this query: '${query}'.

The following data includes matches from Pinecone (semantic search) and Snowflake (structured data).
Write a very short summary (2–3 sentences max). Summarize only electrical expenses (e.g., accounts with "electric" in ACCTNAME, DESCRIPTION, or ANNOTATION) by accounting period (PER_END_DATE) when requested, providing total BALANCE per period. Ignore non-electrical expenses like "Field Equipment Expense" unless explicitly mentioned.

${combinedData}
    `.trim();

    const gptResponse = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        {
          role: "system",
          content:
            "You are a CPA assistant. Your job is to explain key accounting search results clearly, briefly, and professionally.",
        },
        { role: "user", content: summaryPrompt },
      ],
    });

    const summary = gptResponse.choices[0].message.content;

    return NextResponse.json({
      summary: summary,
      rawData: combinedData,
    });
  } catch (error) {
    console.error("Error processing query:", error);
    return NextResponse.json(
      { message: "Error processing query.", error: String(error) },
      { status: 500 }
    );
  } finally {
    snowflakeConnection.destroy((err: any) => {
      if (err) console.error("Error closing Snowflake connection:", err);
    });
  }
}