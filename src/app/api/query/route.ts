import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";

// Initialize clients
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

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
    const results = await index.query({
      vector: queryVector,
      topK: 10,
      includeMetadata: true,
    });

    // Prepare top matches for summary
    const topMatches = results.matches.slice(0, 3).map((match) => ({
      row: match.metadata?.row || "n/a",
      acctName: match.metadata?.AcctName || "n/a",
      wellCode: match.metadata?.WellCode || "n/a",
      balance: match.metadata?.Balance || "n/a",
    }));

    // Generate GPT summary
    const summaryPrompt = `
You are reviewing accounting data based on this query: '${query}'.

The following rows are the top matches from the accounting records.
Write a very short summary (2–3 sentences max). Highlight key accounts or balances related to the query. Be clear and skip anything unnecessary.

${topMatches
  .map(
    (m) => `
Row ${m.row}
AcctName: ${m.acctName}
WellCode: ${m.wellCode}
Balance: ${m.balance}
`
  )
  .join("\n")}
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
  }
}