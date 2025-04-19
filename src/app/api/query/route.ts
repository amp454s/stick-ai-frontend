import { NextRequest, NextResponse } from "next/server";
import snowflake from "snowflake-sdk";

import { interpretQuery } from "@/lib/queryInterpreter/interpret";
import { getTableColumns } from "@/lib/sqlBuilder/columns";
import { buildSQL } from "@/lib/sqlBuilder/builder";
import { runPineconeSearch } from "@/lib/pineconeSearch/search";
import { summarizeResults } from "@/lib/summarize/summarize";
import { buildDebugLog } from "@/lib/queryLogger/logger";

export async function POST(req: NextRequest) {
  let conn: any;

  try {
    const { query } = await req.json();
    if (!query) throw new Error("Missing 'query' in request.");

    // Create Snowflake connection
    conn = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT!,
      username: process.env.SNOWFLAKE_USER!,
      password: process.env.SNOWFLAKE_PASSWORD!,
      database: "STICK_DB",
      schema: "FINANCIAL",
      role: "STICK_ROLE",
      warehouse: "STICK_WH",
    });

    // Connect to Snowflake
    await new Promise<void>((res, rej) =>
      conn.connect((err: unknown) => (err ? rej(err) : res()))
    );

    // Fetch columns and interpret the query
    const columns = await getTableColumns(conn);
    const interpretation = await interpretQuery(query, columns);
    const { sqlAgg, sqlRaw } = buildSQL(interpretation, columns);

    // Run Pinecone vector search
    const pineconeChunks = await runPineconeSearch(query);

    // Run Snowflake queries
    const [aggResults, rawResults] = await Promise.all([
      runSnowflakeQuery(conn, sqlAgg),
      runSnowflakeQuery(conn, sqlRaw),
    ]);

    // Format aggResults as a readable string
    const aggResultsString = aggResults
      .map((row, i) => {
        const rowString = Object.entries(row)
          .map(([key, value]) => `${key}: ${value}`)
          .join(", ");
        return `Result ${i + 1}: ${rowString}`;
      })
      .join("\n");

    // Generate summary
    const summary = await summarizeResults(query, aggResultsString, pineconeChunks);

    // Compile debug output
    const debug = buildDebugLog({
      userQuery: query,
      gptInterpretation: interpretation,
      sqlAgg,
      sqlRaw,
      pineconeChunks,
    });

    return NextResponse.json({
      summary,
      rawData: rawResults,
      debug,
    });
  } catch (err) {
    console.error("Route error:", err);
    return NextResponse.json({ error: String(err) }, { status: 500 });
  } finally {
    if (conn) {
      conn.destroy((err: unknown) => {
        if (err) console.error("Snowflake disconnect error:", err);
      });
    }
  }
}

// Run a Snowflake query and return the rows
function runSnowflakeQuery(conn: any, sqlText: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    conn.execute({
      sqlText,
      complete: (err: unknown, _stmt: any, rows: any[]) => {
        if (err) reject(err);
        else resolve(rows);
      },
    });
  });
}
