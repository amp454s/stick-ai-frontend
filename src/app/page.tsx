import { useState } from 'react';

export default function Home() {
  const [query, setQuery] = useState('');
  const [summary, setSummary] = useState('');
  const [rawData, setRawData] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError('');
    setSummary('');
    setRawData('');

    try {
      const res = await fetch('/api/summarize', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      });

      if (!res.ok) throw new Error('API request failed');

      const data = await res.json();
      setSummary(data.summary || 'No summary returned');
      setRawData(JSON.stringify(data.rawData, null, 2) || 'No raw data returned');
    } catch (err) {
      setError('An error occurred while processing your request.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 text-gray-100 p-6">
      <h1 className="text-3xl font-bold mb-6 text-center">Financial Query Summarizer</h1>
      
      <form onSubmit={handleSubmit} className="max-w-2xl mx-auto mb-8">
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Enter your financial query here..."
          className="w-full p-4 bg-gray-700 text-gray-100 rounded-lg border border-gray-600 focus:outline-none focus:ring-2 focus:ring-blue-500 mb-4"
          rows={4}
        />
        <button
          type="submit"
          disabled={loading}
          className="w-full py-2 bg-blue-600 hover:bg-blue-700 rounded-lg text-white font-semibold disabled:bg-gray-600"
        >
          {loading ? 'Processing...' : 'Submit'}
        </button>
      </form>

      {error && (
        <div className="max-w-2xl mx-auto p-4 bg-red-800 text-red-100 rounded-lg mb-6">
          {error}
        </div>
      )}

      {summary && (
        <div className="max-w-2xl mx-auto mb-6">
          <h2 className="text-2xl font-semibold mb-2">Summary</h2>
          <p className="p-4 bg-gray-800 rounded-lg">{summary}</p>
        </div>
      )}

      {rawData && (
        <div className="max-w-2xl mx-auto">
          <h2 className="text-2xl font-semibold mb-2">Raw Data</h2>
          <pre className="p-4 bg-gray-800 rounded-lg overflow-auto">{rawData}</pre>
        </div>
      )}
    </div>
  );
}