// src/lib/pineconeSearch/search.ts

import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

export async function runPineconeSearch(query: string): Promise<string> {
  const embeddingResponse = await openai.embeddings.create({
    input: query,
    model: "text-embedding-3-small",
  });

  const vector = embeddingResponse.data[0].embedding;

  const results = await index.namespace("default").query({
    vector,
    topK: 5,
    includeMetadata: true,
  });

  const formattedMatches = results.matches.map((match, i) => {
    const meta = match.metadata || {};
    const line = Object.entries(meta)
      .map(([k, v]) => `${k}: ${v}`)
      .join(" | ");
    return `Pinecone Match ${i + 1} → ${line}`;
  });

  const finalString = formattedMatches.join("\n\n");

  console.log("✅ Pinecone summary string generated:\n", finalString);

  return finalString;
}
