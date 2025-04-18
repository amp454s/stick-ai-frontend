import { NextRequest, NextResponse } from "next/server";
import OpenAI from "openai";
import { Pinecone } from "@pinecone-database/pinecone";
import snowflake from "snowflake-sdk";

const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY! });
const pc = new Pinecone({ apiKey: process.env.PINECONE_API_KEY! });
const index = pc.Index(process.env.PINECONE_INDEX!);

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
  "account code": "ACCT_ID",
  "account name": "ACCTNAME",
  "volume": "QUANTITY",
  "quantity": "QUANTITY",
  "mcf": "QUANTITY",
  "balance": "BALANCE",
  "modified by": "LAST_CHANGE_BY",
  "created by": "CREATED_BY",
  "invoice type": "DESCRIPTION"
};

function safeMapFields(terms: any, type: string, columns: string[]): string[] {
  if (!terms) return [];
  const arr = Array.isArray(terms) ? terms : [terms];
  const mapped: string[] = [];
  arr.forEach((term) => {
    if (typeof term === "string") {
      const key = term.toLowerCase().trim();
      const mappedValue = columnMapping[key] || key;
      if (!columns.includes(mappedValue)) {
        console.warn(`⚠️ Unmapped ${type} field fallback: '${term}' → '${mappedValue}'`);
      } else {
        mapped.push(mappedValue);
      }
    }
  });
  return mapped;
}

function formatDate(value: any): string {
  const date = new Date(value);
  return isNaN(date.getTime()) ? value : `${date.getMonth() + 1}/${date.getDate()}/${date.getFullYear()}`;
}

function formatResultsAsTable(rows: any[]): string {
  if (!rows.length) return "No results found.";
  const headers = Object.keys(rows[0]);
  const headerRow = `| ${headers.join(" | ")} |`;
  const separatorRow = `| ${headers.map(() => "---").join(" | ")} |`;
  const dataRows = rows.map((row) => `| ${headers.map((key) => {
    const val = row[key];
    return val instanceof Date || key.toLowerCase().includes("date") ? formatDate(val) : val ?? "";
  }).join(" | ")} |`);
  return [headerRow, separatorRow, ...dataRows].join("\n");
}

async function interpretQuery(query: string, columns: string[]): Promise<any> {
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
  console.log("Raw GPT interpretation output:", content);
  const parsed = JSON.parse(content);

  // Add keyword fallback if missing
  if (!parsed.filters?.keyword && query.toLowerCase().includes("electric")) {
    parsed.filters = parsed.filters || {};
    parsed.filters.keyword = "electric";
  }

  // Flatten malformed excludes
  let excludeNormalized: Record<string, string | string[]> = {};
  const rawExclude = parsed.filters?.exclude;
  if (rawExclude) {
    if (rawExclude.field && rawExclude.value) {
      excludeNormalized = { [rawExclude.field]: rawExclude.value };
    } else if (typeof rawExclude === "object" && !Array.isArray(rawExclude)) {
      excludeNormalized = rawExclude;
    }
  }

  return {
    ...parsed,
    group_by: safeMapFields(parsed.group_by, "group_by", columns),
    filters: {
      ...parsed.filters,
      exclude: excludeNormalized
    },
    exclude: safeMapFields(excludeNormalized, "exclude", columns),
  };
}

// ... rest of file remains unchanged ...
