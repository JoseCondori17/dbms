import React from 'react';

interface QueryResult {
  [key: string]: unknown;
}

interface ResultsDisplayProps {
  results: QueryResult | QueryResult[] | null;
  loading: boolean;
  error: string | null;
}

export function ResultsDisplay({ results, loading, error }: ResultsDisplayProps) {
  const renderTable = (data: QueryResult[]) => {
    if (!data || data.length === 0) return null;

    const keys = Object.keys(data[0]);
    
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full bg-white border border-gray-300">
          <thead className="bg-gray-100">
            <tr>
              {keys.map((key) => (
                <th
                  key={key}
                  className="px-4 py-2 text-left text-xs font-medium text-gray-700 uppercase tracking-wider border-b"
                >
                  {key}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {data.map((row, index) => (
              <tr key={index} className="hover:bg-gray-50">
                {keys.map((key) => (
                  <td
                    key={key}
                    className="px-4 py-2 text-sm text-gray-900 border-b"
                  >
                    {row[key] !== null && row[key] !== undefined 
                      ? String(row[key]) 
                      : 'NULL'
                    }
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  };

  const renderResults = () => {
    if (!results) return null;

    // If it's an array, render as table
    if (Array.isArray(results)) {
      if (results.length === 0) {
        return (
          <div className="text-gray-500 italic">
            Query executed successfully. No rows returned.
          </div>
        );
      }
      return renderTable(results);
    }

    // If it's a single object, check if it has array properties
    if (typeof results === 'object') {
      // Check for common array properties that might contain table data
      const arrayProps = ['data', 'rows', 'result', 'records'];
      
      for (const prop of arrayProps) {
        if (results[prop] && Array.isArray(results[prop])) {
          return renderTable(results[prop] as QueryResult[]);
        }
      }

      // Check if it's a single row result
      if (Object.keys(results).length > 0) {
        return renderTable([results]);
      }
    }

    // If it's a string message, show it
    if (typeof results === 'string') {
      return (
        <div className="text-green-600 font-medium">
          {results}
        </div>
      );
    }

    return null;
  };

  return (
    <div className="flex-1 p-4 bg-white">
      <div className="flex items-center justify-between mb-4">
        <p className="text-gray-600 font-medium">Results</p>
        <div className="flex gap-2">
          <button
            className="px-3 py-1 text-xs bg-blue-100 text-blue-800 rounded hover:bg-blue-200"
            onClick={() => {
              const element = document.getElementById('table-view');
              if (element) element.scrollIntoView({ behavior: 'smooth' });
            }}
          >
            Table View
          </button>
          <button
            className="px-3 py-1 text-xs bg-gray-100 text-gray-800 rounded hover:bg-gray-200"
            onClick={() => {
              const element = document.getElementById('json-view');
              if (element) element.scrollIntoView({ behavior: 'smooth' });
            }}
          >
            JSON View
          </button>
        </div>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded">
          <div className="text-red-600 font-medium">Error:</div>
          <div className="text-red-800">{error}</div>
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center py-8">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          <span className="ml-2 text-gray-600">Executing query...</span>
        </div>
      ) : (
        <div className="space-y-6">
          {/* Table View */}
          <div id="table-view">
            <h3 className="text-sm font-medium text-gray-700 mb-2">Table View</h3>
            <div className="border rounded-lg overflow-hidden">
              {results ? (
                renderResults()
              ) : (
                <div className="p-4 text-gray-500 text-center">
                  No results to display
                </div>
              )}
            </div>
          </div>

          {/* JSON View */}
          <div id="json-view">
            <h3 className="text-sm font-medium text-gray-700 mb-2">JSON View</h3>
            <pre className="bg-gray-50 p-4 rounded-lg overflow-auto text-xs border">
              {results ? JSON.stringify(results, null, 2) : 'No results to display'}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
