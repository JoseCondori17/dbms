'use client';

import { CodeiumEditor } from '@codeium/react-code-editor';
import { Button } from './ui/button';
import { Play } from 'lucide-react';

export function CodeEditor() {
  return (
    <div className="flex flex-col h-full">
      <div className='flex items-center justify-between pl-4 pr-4 h-16 border-b border-gray-300'>
        <span className='font-bold text-2xl'>Query</span>
        <Button size="sm">
          <Play/>
          Run
        </Button>
      </div>
      <div className="flex-1 border-b border-gray-300">
        <CodeiumEditor
          language="pgsql"
          theme="light"
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
          className="w-full h-full"
        />
      </div>

      <div className="flex-1 overflow-auto p-4 bg-white">
        <p className="text-gray-600">Results</p>
        <div>
          data
        </div>
      </div>
    </div>
  );
}
