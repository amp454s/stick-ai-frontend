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

    // Query Pinecone across multiple namespaces
    const namespaces = ["gl", "procount", "ownerpay", "default"];
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

    // Combine and sort results by score
    const combinedResults = pineconeResults
      .flat()
      .sort((a, b) => (b.score || 0) - (a.score || 0))
      .slice(0, 3);

    // Prepare top matches for summary with all metadata fields
    const topMatches = combinedResults.map((match) => {
      const metadata = match.metadata || {};
      // Convert all metadata fields into a string representation
      const metadataFields = Object.entries(metadata)
        .map(([key, value]) => `${key}: ${value || "n/a"}`)
        .join("\n");
      return metadataFields;
    });

    // Generate GPT summary
    const summaryPrompt = `
You are reviewing accounting data based on this query: '${query}'.

The following rows are the top matches from the accounting records, including all available column data.
Write a very short summary (2–3 sentences max). Highlight key details relevant to the query, such as account names, balances, dates, or other significant fields. Be clear and skip anything unnecessary.

${topMatches.join("\n\n")}
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