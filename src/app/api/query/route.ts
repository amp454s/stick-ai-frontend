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

export async function POST(req: NextRequest) {
  try {
    const { query } = await req.json();
    if (!query) {
      return NextResponse.json({ message: "Query is required" }, { status: 400 });
    }

    // Refine query for Pinecone
    const refinedQuery = `electrical expenses by accounting period ${query}`;
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
    console.log("Attempting Snowflake connection...");
    let snowflakeData: string[] = [];
    try {
      await new Promise((resolve, reject) => {
        snowflakeConnection.connect((err, conn) => {
          if (err) reject(err);
          else resolve(conn);
        });
      });
      console.log("Snowflake connection established.");
    } catch (error) {
      console.error("Snowflake connection failed:", error);
      snowflakeData = ["Snowflake connection failed: " + String(error)];
    }

    // Execute Snowflake query if connected
    if (snowflakeData.length === 0) {
      const snowflakeQuery = `
        SELECT PER_END_DATE, SUM(BALANCE) as TOTAL_BALANCE
        FROM S3_GL
        WHERE ACCTNAME LIKE '%electric%' OR DESCRIPTION LIKE '%electric%' OR ANNOTATION LIKE '%electric%'
        GROUP BY PER_END_DATE
        ORDER BY PER_END_DATE
        LIMIT 10
      `;
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
        console.error("Snowflake query failed:", error);
        snowflakeData = ["Snowflake query failed: " + String(error)];
      }
    }

    // Combine data
    const combinedData = [
      ...pineconeData.map((data, i) => `Pinecone Result ${i + 1}:\n${data}`),
      ...snowflakeData.map((data, i) => `Snowflake Result ${i + 1}:\n${data}`),
    ].join("\n\n");

    console.log("Combined Data for Query:", combinedData);

    // Handle no data case
    if (pineconeData.length === 0 && snowflakeData.length === 0) {
      return NextResponse.json({
        summary: "No matching data found for the query.",
        rawData: "No data available.",
      });
    }

    // Generate GPT summary
    const summaryPrompt = `
You are reviewing accounting data based on this query: '${query}'.
The following data includes matches from Pinecone (semantic search)${snowflakeData.length > 0 && !snowflakeData[0].startsWith("Snowflake") ? " and Snowflake (structured data)" : ""}.
Write a very short summary (2–3 sentences max). If only Pinecone data is available, note that results are based on semantic search and may not be comprehensive. Summarize only electrical expenses by accounting period (PER_END_DATE), providing total BALANCE per period, and ignore non-electrical expenses like "Field Equipment Expense" unless explicitly mentioned.
${combinedData}
    `.trim();

    const gptResponse = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        {
          role: "system",
          content: "You are a CPA assistant. Your job is to explain key accounting search results clearly, briefly, and professionally.",
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