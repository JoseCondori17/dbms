'use client';

import { CodeiumEditor } from '@codeium/react-code-editor';
import { Play } from 'lucide-react';
import { useState } from 'react';
import { Button } from './ui/button';
import { ResultsDisplay } from './ResultsDisplay';

interface QueryResult {
  [key: string]: unknown;
}

export function CodeEditor() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleExecute = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const response = await fetch('http://127.0.0.1:8000/execute', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      });

      if (!response.ok) {
        throw new Error('Error al ejecutar la consulta');
      }

      const data = await response.json();
      setResults(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error desconocido');
    } finally {
      setLoading(false);
    }
  };

  const handleQueryChange = (value: string | undefined) => {
    setQuery(value || '');
  };

  return (
    <div className="flex flex-col h-full">
      <div className='flex items-center justify-between pl-4 pr-4 h-16 border-b border-gray-300'>
        <span className='font-bold text-2xl'>Query</span>
        <Button 
          size="sm" 
          onClick={handleExecute}
          disabled={loading}
        >
          <Play className={loading ? 'animate-spin' : ''}/>
          {loading ? 'Loading...' : 'Run'}
        </Button>
      </div>
      <div className="flex-1 border-b border-gray-300">
        <CodeiumEditor
          language="pgsql"
          theme="light"
          value={query}
          onChange={handleQueryChange}
          options={{
            fontSize: 14,
            fontFamily: '"Fira Code", "JetBrains Mono", Consolas, Menlo, monospace',
            fontLigatures: true,
            lineNumbersMinChars: 3,
            lineDecorationsWidth: 35,
            lineHeight: 25,
            lineNumbers: 'on',
            autoClosingBrackets: "always",
            autoClosingQuotes: "always",
            renderLineHighlight: "none",
            minimap: { enabled: true },
            scrollBeyondLastLine: false,
            automaticLayout: true,
            tabSize: 4,
            scrollbar: {
              verticalScrollbarSize: 12,
              horizontalScrollbarSize: 12
            },
            suggest: {
              showKeywords: true,
              showSnippets: true,
              preview: true,
              filterGraceful: true,
              snippetsPreventQuickSuggestions: false
            },
            parameterHints: { enabled: true },
            folding: true,
            wordWrap: 'on',
            fixedOverflowWidgets: true
          }}
          height="100%"
          className="w-full h-full flex-1"
        />
      </div>

      <ResultsDisplay results={results} loading={loading} error={error} />
    </div>
  );
}
