// src/lib/summarize/summarize.ts

import OpenAI from "openai";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });

export async function summarizeResults(userQuery: string, aggregated: string, sourceNote = ""): Promise<string> {
  const summaryPrompt = `
You are a financial assistant. Based on the user's query: '${userQuery}', and the aggregated data below, provide a concise summary (2–3 sentences).

Aggregated Data:
${aggregated}

${sourceNote}
  `.trim();

  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      { role: "system", content: "You are a CPA assistant." },
      { role: "user", content: summaryPrompt }
    ]
  });

  return response.choices[0].message.content ?? "";
}
