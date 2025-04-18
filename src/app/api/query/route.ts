import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

const columnMapping: { [key: string]: string } = {
  "accounting period": "PER_END_DATE",
  "vendor": "VENDORNAME",
  "account name": "ACCTNAME",
};

function runSnowflakeQuery(connection: any, sqlText: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    console.log("Executing query:\n", sqlText);
    connection.execute({
      sqlText,
      complete: (err: Error | null, _stmt: any, rows: any[]) => {
        if (err) {
          console.error("Snowflake query error:", err);
          reject(err);
        } else {
          console.log(`Query successful. Rows returned: ${rows.length}`);
          resolve(rows || []);
        }
      },
    });
  });
}

async function getTableColumns(connection: any): Promise<string[]> {
  return runSnowflakeQuery(
    connection,
    `
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'S3_GL' AND TABLE_SCHEMA = 'FINANCIAL'
  `
  ).then((rows) => rows.map((row: any) => row.COLUMN_NAME));
}

async function interpretQuery(query: string): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `
          You are an expert in interpreting financial queries.
          Given a user's query, extract:
          - data_type: 'expenses', 'balances', etc.
          - group_by: array of human-readable field names
          - filters: e.g., { keyword: 'electric' } or { vendor: 'Exxon' }
          - mode: 'summary' if the user wants a summarized report; otherwise 'search'
          Return a valid JSON object.
        `,
      },
      { role: "user", content: query },
    ],
  });

  const rawContent = response.choices[0].message.content || "";
  console.log("Raw GPT interpretation output:", rawContent);

  try {
    return JSON.parse(rawContent);
  } catch (e) {
    throw new Error(`Failed to parse GPT interpretation:\n${rawContent}`);
  }
}

function buildSnowflakeQuery(interpretation: any, tableColumns: string[], isRaw: boolean = false): string {
  const data_type = interpretation.data_type || "balances";
  const group_by = (interpretation.group_by || [])
    .map((term: string) => columnMapping[term.toLowerCase()] || term)
    .filter((col: string) => tableColumns.includes(col));

  const filters = interpretation.filters || {};
  const resolvedFilters = Object.entries(filters).reduce((acc: Record<string, any>, [key, val]) => {
    const mapped = columnMapping[key.toLowerCase()] || key;
    if (tableColumns.includes(mapped)) acc[mapped] = val;
    return acc;
  }, {});

  let whereClause = "";
  if (filters.keyword) {
    const keyword = filters.keyword === "electricity" ? "electric" : filters.keyword;
    whereClause = `(ACCTNAME ILIKE '%${keyword}%' OR DESCRIPTION ILIKE '%${keyword}%' OR ANNOTATION ILIKE '%${keyword}%')`;
  } else if (Object.keys(resolvedFilters).length) {
    whereClause = Object.entries(resolvedFilters)
      .map(([field, condition]) => {
        if (Array.isArray(condition) && condition.length === 2) {
          return `${field} ${condition[0]} '${condition[1]}'`;
        }
        return `${field} = '${condition}'`;
      })
      .join(" AND ");
  }

  if (isRaw) {
    return `
      SELECT *
      FROM STICK_DB.FINANCIAL.S3_GL
      ${whereClause ? `WHERE ${whereClause}` : ""}
      LIMIT 100
    `.trim();
  }

  const selectFields = group_by.length ? group_by.join(", ") + ", " : "";
  const groupByClause = group_by.length ? `GROUP BY ${group_by.join(", ")}` : "";
  const orderByClause = group_by.length ? `ORDER BY ${group_by.join(", ")}` : "";

  return `
    SELECT ${selectFields}SUM(BALANCE) AS TOTAL
    FROM STICK_DB.FINANCIAL.S3_GL
    ${whereClause ? `WHERE ${whereClause}` : ""}
    ${groupByClause}
    ${orderByClause}
    LIMIT 100
  `.trim();
}

function formatResultsAsTable(rows: any[]): string {
  if (!rows.length) return "No results found.";

  const headers = Object.keys(rows[0]);
  const headerRow = `| ${headers.join(" | ")} |`;
  const separatorRow = `| ${headers.map(() => "---").join(" | ")} |`;

  const dataRows = rows.map((row) => {
    return `| ${headers
      .map((key) => {
        const value = row[key];
        if (value instanceof Date || (typeof value === "string" && /GMT/.test(value))) {
          const d = new Date(value);
          return isNaN(d.getTime()) ? value : d.toISOString().split("T")[0];
        }
        return `${value ?? ""}`;
      })
      .join(" | ")} |`;
  });

  return [headerRow, separatorRow, ...dataRows].join("\n");
}

async function fusionSmartRetrieval(query: string, interpretation: any, tableColumns: string[], connection: any) {
  const isSummary = interpretation.mode === "summary";

  const snowflakeAggQuery = buildSnowflakeQuery(interpretation, tableColumns, false);
  const snowflakeAggResults = await runSnowflakeQuery(connection, snowflakeAggQuery);
  const aggTable = formatResultsAsTable(snowflakeAggResults);

  if (isSummary) {
    return {
      combinedTextForSummary: aggTable,
      rawDataText: aggTable,
      sourceNote: "",
    };
  }

  const pineconeQuery = `${interpretation.data_type || "financial"} ${query}`;
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

  const pineconeData = pineconeResults.matches.map((match, i) => {
    const metadata = match.metadata || {};
    return `Pinecone Raw Result ${i + 1}:\n${Object.entries(metadata).map(([k, v]) => `${k}: ${v ?? "n/a"}`).join("\n")}`;
  });

  const snowflakeRawQuery = buildSnowflakeQuery(interpretation, tableColumns, true);
  const snowflakeRawResults = await runSnowflakeQuery(connection, snowflakeRawQuery);

  const rawData = snowflakeRawResults.map((row, i) =>
    `Snowflake Raw Result ${i + 1}:\n${Object.entries(row).map(([k, v]) => `${k}: ${v ?? "n/a"}`).join("\n")}`
  );

  return {
    combinedTextForSummary: [aggTable, ...pineconeData].join("\n\n"),
    rawDataText: [...pineconeData, ...rawData].join("\n\n"),
    sourceNote: snowflakeAggResults.length ? "" : "Note: results based on semantic search only.",
  };
}

export async function POST(req: NextRequest) {
  let connection: any;
  try {
    const { query } = await req.json();
    if (!query) throw new Error("Query is required");

    const interpretation = await interpretQuery(query);
    console.log("Interpretation:", interpretation);

    connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT!,
      username: process.env.SNOWFLAKE_USER!,
      password: process.env.SNOWFLAKE_PASSWORD!,
      database: "STICK_DB",
      schema: "FINANCIAL",
      role: "STICK_ROLE",
      warehouse: "STICK_WH",
    });

    await new Promise((resolve, reject) => {
      connection.connect((err: Error | null, conn: any) => (err ? reject(err) : resolve(conn)));
    });

    const tableColumns = await getTableColumns(connection);
    const { combinedTextForSummary, rawDataText, sourceNote } = await fusionSmartRetrieval(
      query,
      interpretation,
      tableColumns,
      connection
    );

    const summaryPrompt = `
      You are a financial assistant. Based on the user's query: '${query}', and the aggregated data below, provide a concise summary (2–3 sentences) that directly answers the query.

      Aggregated Data:
      ${combinedTextForSummary}

      Note: ${sourceNote}
    `.trim();

    const gptResponse = await openai.chat.completions.create({
      model: "gpt-4",
      messages: [
        { role: "system", content: "You are a CPA assistant." },
        { role: "user", content: summaryPrompt },
      ],
    });

    return NextResponse.json({
      summary: gptResponse.choices[0].message.content,
      rawData: rawDataText,
    });
  } catch (error) {
    console.error("Error:", error);
    return NextResponse.json({ error: String(error) }, { status: 500 });
  } finally {
    if (connection) {
      connection.destroy((err: Error | null) => err && console.error("Disconnect error:", err));
    }
  }
}