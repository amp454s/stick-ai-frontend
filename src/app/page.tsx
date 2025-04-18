"use client";

import { useState } from "react";

export default function Home() {
  const [query, setQuery] = useState("");
  const [summary, setSummary] = useState("");
  const [rawData, setRawData] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");
    setSummary("");
    setRawData("");

    try {
      const res = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });

      if (!res.ok) throw new Error("API request failed");

      const data = await res.json();
      setSummary(data.summary || "No summary available.");
      setRawData(data.rawData || "No raw data available.");
    } catch (err) {
      console.error("Error:", err);
      setError("An error occurred while processing your request.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-white text-gray-800 flex flex-col items-center justify-center p-4">
      <h1 className="text-3xl font-bold mb-6 text-gray-800">
        Stick AI - Financial Query Assistant
      </h1>
      <form onSubmit={handleSubmit} className="w-full max-w-md bg-blue-900 p-4 rounded-lg">
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Ask a financial question (e.g., 'What’s the balance for vendor XYZ?')"
          className="w-full p-2 mb-4 bg-white text-gray-800 border-teal-200 rounded focus:ring-blue-500 resize-y min-h-[80px] placeholder-gray-500"
          rows={3}
        />
        <button
          type="submit"
          disabled={loading}
          className="w-full p-2 bg-blue-500 text-white rounded hover:bg-blue-600 disabled:bg-gray-300"
        >
          {loading ? "Processing..." : "Submit"}
        </button>
      </form>
      {error && (
        <div className="mt-6 w-full max-w-md p-4 bg-red-100 rounded-lg text-red-800">
          <h2 className="text-xl font-semibold mb-2">Error:</h2>
          <p>{error}</p>
        </div>
      )}
      {summary && (
        <div className="mt-6 w-full max-w-md p-4 bg-white rounded-lg text-gray-800">
          <h2 className="text-xl font-semibold mb-2">Summary:</h2>
          <p>{summary}</p>
        </div>
      )}
      {rawData && (
        <div className="mt-6 w-full max-w-md p-4 bg-white rounded-lg text-gray-800">
          <h2 className="text-xl font-semibold mb-2">Raw Data:</h2>
          <pre className="whitespace-pre-wrap">{rawData}</pre>
        </div>
      )}
    </div>
  );
}