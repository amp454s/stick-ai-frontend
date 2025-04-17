import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

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

    // Create embedding for query
    const embeddingResponse = await openai.embeddings.create({
      input: query,
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

    // Query Snowflake
    await new Promise((resolve, reject) => {
      snowflakeConnection.connect((err, conn) => {
        if (err) reject(err);
        else resolve(conn);
      });
    });

    const snowflakeQuery = `
      SELECT *
      FROM S3_GL
      WHERE DESCRIPTION LIKE '%${query}%'
         OR ACCTNAME LIKE '%${query}%'
         OR VENDORNAME LIKE '%${query}%'
      LIMIT 3
    `;

    const snowflakeResults = await new Promise<any[]>((resolve, reject) => {
      snowflakeConnection.execute({
        sqlText: snowflakeQuery,
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    const snowflakeData = snowflakeResults.map((row) =>
      Object.entries(row)
        .map(([key, value]) => `${key}: ${value || "n/a"}`)
        .join("\n")
    );

    // Combine Pinecone and Snowflake data
    const combinedData = [
      ...pineconeData.map((data, i) => `Pinecone Result ${i + 1}:\n${data}`),
      ...snowflakeData.map((data, i) => `Snowflake Result ${i + 1}:\n${data}`),
    ].join("\n\n");

    // Generate GPT summary
    const summaryPrompt = `
You are reviewing accounting data based on this query: '${query}'.

The following data includes matches from Pinecone (semantic search) and Snowflake (structured data).
Write a very short summary (2–3 sentences max). Highlight key details like account names, balances, dates, or vendors relevant to the query.

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

    return NextResponse.json({ message: summary });
  } catch (error) {
    console.error("Error processing query:", error);
    return NextResponse.json(
      { message: "Error processing query." },
      { status: 500 }
    );
  } finally {
    snowflakeConnection.destroy((err: any) => {
      if (err) console.error("Error closing Snowflake connection:", err);
    });
  }
}