"use client";

import { useState } from "react";

export default function Home() {
  const [query, setQuery] = useState("");
  const [summary, setSummary] = useState("");
  const [rawData, setRawData] = useState("");
  const [debug, setDebug] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const runQuery = async () => {
    setLoading(true);
    setError("");
    setSummary("");
    setRawData("");
    setDebug("");

    try {
      const res = await fetch("/api/query", {
        method: "POST",
        body: JSON.stringify({ query }),
        headers: { "Content-Type": "application/json" },
      });

      if (!res.ok) throw new Error(await res.text());
      const { summary, rawData, debug } = await res.json();
      setSummary(summary);
      setRawData(rawData);
      setDebug(debug);
    } catch (err: any) {
      setError(err.message || "Unexpected error.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <main className="max-w-5xl mx-auto p-8">
      <h1 className="text-3xl font-bold mb-4">Stick AI - Financial Query Assistant</h1>

      <textarea
        rows={3}
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="e.g. summarize LOE expenses by month for company 1"
        className="w-full p-3 border rounded mb-4"
      />

      <button
        onClick={runQuery}
        disabled={loading}
        className="bg-blue-600 text-white px-6 py-2 rounded disabled:opacity-50"
      >
        {loading ? "Thinking..." : "Submit"}
      </button>

      {error && <pre className="text-red-600 mt-4 whitespace-pre-wrap">{error}</pre>}

      {summary && (
        <section className="mt-8">
          <h2 className="text-xl font-semibold mb-2">Summary:</h2>
          <p className="whitespace-pre-wrap">{summary}</p>
        </section>
      )}

      {rawData && (
        <section className="mt-8">
          <h2 className="text-xl font-semibold mb-2">Raw Data:</h2>
          <pre className="bg-gray-100 p-4 rounded whitespace-pre overflow-auto">{rawData}</pre>
        </section>
      )}

      {debug && (
        <section className="mt-8">
          <h2 className="text-xl font-semibold mb-2">Query Debug:</h2>
          <pre className="bg-yellow-100 p-4 rounded whitespace-pre overflow-auto">
            {typeof debug === "string" ? debug : JSON.stringify(debug, null, 2)}
          </pre>
        </section>
      )}
    </main>
  );
}
