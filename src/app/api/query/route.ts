import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import OpenAI from "openai";

// Initialize Supabase client
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!;
const supabase = createClient(supabaseUrl, supabaseKey);

// Initialize OpenAI client
const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY!,
});

// Define column mapping
const columnMapping: { [key: string]: string } = {
  "gl identifier": "UTM_ID",
  "company": "CO_ID",
  "company name": "NAME",
  "well code": "WELLCODE",
  "accountingid": "WELLCODE",
  "cost center": "WELLCODE",
  "cost center name": "WELL_NAME",
  "well name": "WELL_NAME",
  "accounting period": "PER_END_DATE",
  "month end": "PER_END_DATE",
  "posting date": "POSTING_DATE",
  "accounting date": "LOS_PRODUCTIONDATE",
  "activity date": "LOS_PRODUCTIONDATE",
  "excalibur module": "SYSTEM_CODE",
  "journal entry": "VOUCHER",
  "vendor": "VENDORNAME",
  "vendor name": "VENDORNAME",
  "afe": "AFE_ID",
  "gas purchaser": "PURCH_ID",
  "purchaser": "PURCHNAME",
  "purchaser number": "PURCH_ID",
  "gl account": "ACCT_ID",
  "general ledger account": "ACCT_ID",
  "account number": "ACCT_ID",
  "account id": "ACCT_ID",
  "accounting id": "ACCT_ID", // Added to map "accounting id" to "ACCT_ID"
  "account code": "ACCT_ID",
  "account name": "ACCTNAME",
  "volume": "QUANTITY",
  "quantity": "QUANTITY",
  "mcf": "QUANTITY",
  "balance": "BALANCE",
  "modified by": "LAST_CHANGE_BY",
  "created by": "CREATED_BY",
  "invoice type": "DESCRIPTION",
};

// Extract keywords from filters
function extractKeywords(filters: any): string[] {
  if (filters && filters.keyword) {
    return Array.isArray(filters.keyword) ? filters.keyword : [filters.keyword];
  }
  return [];
}

// Updated extractExcludeClauses to handle "exclude" key in filters
function extractExcludeClauses(filters: any, columns: string[]): string[] {
  const clauses: string[] = [];
  for (const [key, val] of Object.entries(filters || {})) {
    if (val && typeof val === "object") {
      if ("exclude" in val) {
        const field = columnMapping[key.toLowerCase()] || key;
        if (columns.map(c => c.toLowerCase()).includes(field.toLowerCase())) {
          const values = Array.isArray(val.exclude) ? val.exclude : [val.exclude];
          clauses.push(...values.map((v) => `${field} != '${v}'`));
        }
      } else if (key === "exclude") {
        for (const [excludeKey, excludeValue] of Object.entries(val)) {
          const field = columnMapping[excludeKey.toLowerCase()] || excludeKey;
          if (columns.map(c => c.toLowerCase()).includes(field.toLowerCase())) {
            const values = Array.isArray(excludeValue) ? excludeValue : [excludeValue];
            clauses.push(...values.map((v) => `${field} != '${v}'`));
          }
        }
      }
    }
  }
  return clauses;
}

// Map fields safely with logging
function safeMapFields(terms: string[], type: string, columns: string[]): string[] {
  return terms.map((term) => {
    const key = term.toLowerCase().trim();
    const mappedValue = columnMapping[key] || key;
    if (!columns.map(c => c.toLowerCase()).includes(mappedValue.toLowerCase())) {
      console.warn(`⚠️ Unmapped ${type} field fallback: '${term}' → '${mappedValue}'`);
    }
    return mappedValue;
  });
}

// Updated interpretQuery with enhanced GPT prompt
async function interpretQuery(query: string, columns: string[]): Promise<any> {
  const response = await openai.chat.completions.create({
    model: "gpt-4",
    messages: [
      {
        role: "system",
        content: `You are an expert in interpreting financial queries. Given a user's query, extract:
- data_type: 'expenses', 'balances', etc.
- group_by: array of human-readable field names
- filters: keyword-based or explicit column filters. If the query specifies a type of data, like 'electric expenses', include the type as a keyword (e.g., 'keyword': ['electric']). Filters can also include an 'exclude' subobject for fields to exclude.
- mode: 'summary' or 'search'
Return a valid JSON object.`,
      },
      { role: "user", content: query },
    ],
  });

  const content = response.choices[0].message.content || "";
  console.log("Raw GPT interpretation output:", content);
  const parsed = JSON.parse(content);

  return {
    ...parsed,
    group_by: safeMapFields(parsed.group_by, "group_by", columns),
    filters: parsed.filters,
    exclude: extractExcludeClauses(parsed.filters, columns),
  };
}

// Build Snowflake query
function buildSnowflakeQuery(interpretation: any, columns: string[]): string {
  const { data_type, group_by, exclude, filters, mode } = interpretation;
  const keywords = extractKeywords(filters);

  const whereClauses: string[] = [];
  if (keywords.length > 0) {
    const keywordSearch = keywords.map((k) =>
      `(DESCRIPTION ILIKE '%${k}%' OR VENDORNAME ILIKE '%${k}%' OR ACCTNAME ILIKE '%${k}%' OR ANNOTATION ILIKE '%${k}%')`
    ).join(" AND ");
    whereClauses.push(keywordSearch);
  }
  if (exclude && exclude.length > 0) {
    whereClauses.push(...exclude);
  }

  const whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

  if (mode === "summary") {
    const groupByClause = group_by.length > 0 ? `GROUP BY ${group_by.join(", ")}` : "";
    return `
      SELECT ${group_by.join(", ")}, SUM(BALANCE) AS TOTAL
      FROM STICK_DB.FINANCIAL.S3_GL
      ${whereClause}
      ${groupByClause}
      ORDER BY ${group_by.join(", ")}
      LIMIT 100
    `;
  } else {
    return `
      SELECT *
      FROM STICK_DB.FINANCIAL.S3_GL
      ${whereClause}
      LIMIT 100
    `;
  }
}

// POST handler
export async function POST(req: NextRequest) {
  try {
    const { query } = await req.json();

    // Fetch columns from Supabase (assuming metadata table exists)
    const { data: columnData, error: columnError } = await supabase
      .from("metadata")
      .select("column_name")
      .eq("table_name", "S3_GL");

    if (columnError || !columnData) {
      throw new Error("Failed to fetch columns");
    }

    const columns = columnData.map((row: any) => row.column_name);

    // Interpret query
    const interpretation = await interpretQuery(query, columns);
    console.log("Query interpretation:", interpretation);

    // Build and execute query
    const snowflakeQuery = buildSnowflakeQuery(interpretation, columns);
    console.log("Generated Snowflake query:", snowflakeQuery);

    // Mock Snowflake response (replace with actual Snowflake SDK call)
    const { data, error } = await supabase.rpc("execute_snowflake_query", {
      query_text: snowflakeQuery,
    });

    if (error) {
      throw new Error(`Snowflake query failed: ${error.message}`);
    }

    return NextResponse.json({
      summary: interpretation.mode === "summary" ? data : null,
      raw_data: interpretation.mode === "search" ? data : null,
      interpretation,
    });
  } catch (error: any) {
    console.error("Error processing request:", error);
    return NextResponse.json(
      { error: error.message || "Internal Server Error" },
      { status: 500 }
    );
  }
}