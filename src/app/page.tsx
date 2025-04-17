"use client";

import { useState } from "react";

export default function Home() {
  const [query, setQuery] = useState("");
  const [response, setResponse] = useState("");
  const [rawData, setRawData] = useState("");

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setResponse("Processing...");
    setRawData("");
    try {
      const res = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });
      const data = await res.json();
      setResponse(data.summary || data.message);
      setRawData(data.rawData || "");
    } catch (error) {
      setResponse("Error processing query.");
    }
  };

  return (
    <div className="min-h-screen bg-gray-100 flex flex-col items-center justify-center p-4">
      <h1 className="text-3xl font-bold mb-6 text-gray-900">
        Stick AI - Financial Query Assistant
      </h1>
      <form onSubmit={handleSubmit} className="w-full max-w-md">
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Ask a financial question (e.g., 'What’s the balance for vendor XYZ?')"
          className="w-full p-2 mb-4 border rounded text-gray-900 placeholder-gray-500 resize-y min-h-[80px]"
          rows={3}
        />
        <button
          type="submit"
          className="w-full p-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Submit
        </button>
      </form>
      {response && (
        <div className="mt-6 w-full max-w-md p-4 bg-white rounded shadow">
          <h2 className="text-xl font-semibold mb-2 text-gray-900">Summary:</h2>
          <p className="text-gray-800">{response}</p>
        </div>
      )}
      {rawData && (
        <div className="mt-6 w-full max-w-md p-4 bg-white rounded shadow">
          <h2 className="text-xl font-semibold mb-2 text-gray-900">Raw Data:</h2>
          <pre className="text-gray-800 whitespace-pre-wrap">{rawData}</pre>
        </div>
      )}
    </div>
  );
}