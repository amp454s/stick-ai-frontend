// src/lib/queryInterpreter/interpret.ts

import OpenAI from "openai";
import { columnMapping, extractExcludeClauses, safeMapFields } from "@/lib/sqlBuilder/columns";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });

export async function interpretQuery(query: string, columns: string[]): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `You are an expert in interpreting financial queries. Given a user's query, extract:
- data_type: 'expenses', 'balances', etc.
- group_by: array of human-readable field names
- filters: keyword-based or explicit column filters (can include exclude subobject)
- mode: 'summary' or 'search'
Return a valid JSON object.`,
      },
      { role: "user", content: query },
    ],
  });

  const content = response.choices[0].message.content || "";
  const parsed = JSON.parse(content);

  return {
    ...parsed,
    group_by: safeMapFields(parsed.group_by, "group_by", columns),
    filters: parsed.filters,
    exclude: extractExcludeClauses(parsed.filters, columns),
  };
}
