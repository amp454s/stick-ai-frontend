// src/lib/sqlBuilder/builder.ts

import { extractKeywords } from "./columns";

export function buildSQL(interpretation: any, columns: string[]): { sqlAgg: string; sqlRaw: string } {
  const groupBy = (interpretation.group_by || []).filter((col: string) => columns.includes(col));
  const filters = interpretation.filters || {};
  const keywords = extractKeywords(filters);
  const excludeClauses = interpretation.exclude || [];

  let whereClause = "";
  if (keywords.length) {
    const keywordSearch = keywords.map((k) =>
      `(DESCRIPTION ILIKE '%${k}%' OR VENDORNAME ILIKE '%${k}%' OR ACCTNAME ILIKE '%${k}%' OR ANNOTATION ILIKE '%${k}%')`
    ).join(" AND ");
    whereClause += keywordSearch;
  }

  if (excludeClauses.length) {
    whereClause += (whereClause ? " AND " : "") + excludeClauses.join(" AND ");
  }

  const base = `FROM STICK_DB.FINANCIAL.S3_GL`;
  const where = whereClause ? `WHERE ${whereClause}` : "";
  const selectFields = groupBy.length ? `${groupBy.join(", ")}, ` : "";
  const groupByClause = groupBy.length ? `GROUP BY ${groupBy.join(", ")}` : "";
  const orderByClause = groupBy.length ? `ORDER BY ${groupBy.join(", ")}` : "";

  const sqlAgg = `SELECT ${selectFields}SUM(BALANCE) AS TOTAL ${base} ${where} ${groupByClause} ${orderByClause} LIMIT 100`;
  const sqlRaw = `SELECT * ${base} ${where} LIMIT 100`;

  return { sqlAgg, sqlRaw };
}
