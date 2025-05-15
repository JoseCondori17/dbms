'use client';

import { configureSQLLanguage, defaultSqlExample } from '@/lib/sql';
import { CodeiumEditor } from '@codeium/react-code-editor';
import type * as monaco from 'monaco-editor';
import type { editor } from 'monaco-editor';

export function CodeEditor( ) {

  return (
    <div className=''>
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
      />
    </div>
  );
}