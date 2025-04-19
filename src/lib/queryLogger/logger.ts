// src/lib/queryLogger/logger.ts

export function buildDebugLog(params: {
  userQuery: string;
  gptInterpretation: any;
  sqlAgg: string;
  sqlRaw: string;
  pineconeChunks: string;
}) {
  const { userQuery, gptInterpretation, sqlAgg, sqlRaw, pineconeChunks } = params;

  const interpretationBlock = `
**GPT Interpretation**
\`\`\`json
${JSON.stringify(gptInterpretation, null, 2)}
\`\`\`
`;

  const sqlBlock = `
**SQL Queries**
- Aggregated:
\`\`\`sql
${sqlAgg}
\`\`\`

- Raw:
\`\`\`sql
${sqlRaw}
\`\`\`
`;

  const pineconeBlock = `
**Top Pinecone Matches**
${pineconeChunks || "None found."}
`;

  return [interpretationBlock, sqlBlock, pineconeBlock].join("\n\n");
}
