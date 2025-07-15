'use client';

import { CodeiumEditor } from '@codeium/react-code-editor';
import { Play } from 'lucide-react';
import { useState, useEffect } from 'react';
import { Button } from './ui/button';
import { ResultsDisplay } from './ResultsDisplay';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';

interface QueryResult {
  [key: string]: unknown;
}

// Componente para el Proyecto 2
function Proyecto2Component() {
  const [textQuery, setTextQuery] = useState("");
  const [audioQuery, setAudioQuery] = useState("");
  const [topK, setTopK] = useState(5);
  const [searchType, setSearchType] = useState<"text" | "audio">("text");
  const [results, setResults] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [indexStatus, setIndexStatus] = useState({
    text: false,
    audio: false,
    system: false
  });

  // Función para construir índice de texto
  const buildTextIndex = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://127.0.0.1:8000/api/build-text-index', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({})
      });
      if (response.ok) {
        const data = await response.json();
        alert('Índice de texto construido exitosamente');
        checkIndexStatus();
      } else {
        alert('Error construyendo índice de texto');
      }
    } catch (error) {
      console.error('Error:', error);
      alert('Error de conexión al servidor');
    }
    setLoading(false);
  };

  // Función para construir índice de audio
  const buildAudioIndex = async () => {
    setLoading(true);
    try {
      const response = await fetch('http://127.0.0.1:8000/api/build-audio-index', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({})
      });
      if (response.ok) {
        const data = await response.json();
        alert('Índice de audio construido exitosamente');
        checkIndexStatus();
      } else {
        alert('Error construyendo índice de audio');
      }
    } catch (error) {
      console.error('Error:', error);
      alert('Error de conexión al servidor');
    }
    setLoading(false);
  };

  // Función para verificar estado de índices
  const checkIndexStatus = async () => {
    try {
      const response = await fetch('http://127.0.0.1:8000/api/status');
      if (response.ok) {
        const data = await response.json();
        setIndexStatus({
          text: data.data.text_collections > 0,
          audio: data.data.audio_collections.sequential > 0,
          system: data.data.system_status === 'running'
        });
      }
    } catch (error) {
      console.error('Error checking status:', error);
    }
  };

  // Función para búsqueda de texto
  const searchText = async () => {
    if (!textQuery.trim()) {
      alert('Por favor ingresa una consulta de texto');
      return;
    }
    
    setLoading(true);
    try {
      const response = await fetch('http://127.0.0.1:8000/api/search-text', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: textQuery,
          k: topK
        })
      });
      if (response.ok) {
        const data = await response.json();
        setResults(data.results || []);
      } else {
        alert('Error en la búsqueda de texto');
      }
    } catch (error) {
      console.error('Error:', error);
      alert('Error de conexión al servidor');
    }
    setLoading(false);
  };

  // Función para búsqueda de audio
  const searchAudio = async () => {
    if (!audioQuery.trim()) {
      alert('Por favor ingresa un nombre de archivo de audio');
      return;
    }
    
    setLoading(true);
    try {
      const response = await fetch('http://127.0.0.1:8000/api/search-audio', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query_audio: audioQuery,
          k: topK
        })
      });
      if (response.ok) {
        const data = await response.json();
        setResults(data.results || []);
      } else {
        alert('Error en la búsqueda de audio');
      }
    } catch (error) {
      console.error('Error:', error);
      alert('Error de conexión al servidor');
    }
    setLoading(false);
  };

  // Verificar estado al cargar
  useEffect(() => {
    checkIndexStatus();
  }, []);

  return (
    <div className="space-y-4 p-4">
      {/* Estado del sistema */}
      <Card>
        <CardHeader>
          <CardTitle>Estado del Sistema</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex gap-4 mb-4">
            <Button 
              onClick={buildTextIndex} 
              disabled={loading}
              className="bg-green-600 hover:bg-green-700"
            >
              🔧 Construir Índice Texto
            </Button>
            <Button 
              onClick={buildAudioIndex} 
              disabled={loading}
              className="bg-green-600 hover:bg-green-700"
            >
              🔧 Construir Índice Audio
            </Button>
            <Button 
              onClick={checkIndexStatus} 
              disabled={loading}
              variant="outline"
            >
              🔄 Actualizar Estado
            </Button>
          </div>
          <div className="grid grid-cols-3 gap-4 text-sm">
            <div className={`p-2 rounded ${indexStatus.system ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
              Sistema: {indexStatus.system ? '✅ Listo' : '❌ No listo'}
            </div>
            <div className={`p-2 rounded ${indexStatus.text ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
              Índice Texto: {indexStatus.text ? '✅ Construido' : '❌ No construido'}
            </div>
            <div className={`p-2 rounded ${indexStatus.audio ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
              Índice Audio: {indexStatus.audio ? '✅ Construido' : '❌ No construido'}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Búsqueda */}
      <Card>
        <CardHeader>
          <CardTitle>Búsqueda</CardTitle>
        </CardHeader>
        <CardContent>
          <Tabs value={searchType} onValueChange={(value) => setSearchType(value as "text" | "audio")}>
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="text">Búsqueda Textual</TabsTrigger>
              <TabsTrigger value="audio">Búsqueda Audio</TabsTrigger>
            </TabsList>
            
            <TabsContent value="text" className="space-y-4 mt-4">
              <div>
                <label className="block text-sm font-medium mb-2">Consulta de Texto:</label>
                <input
                  type="text"
                  value={textQuery}
                  onChange={(e) => setTextQuery(e.target.value)}
                  placeholder="Ej: love war time peace"
                  className="w-full p-2 border rounded"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-2">Top-K resultados:</label>
                <input
                  type="number"
                  value={topK}
                  onChange={(e) => setTopK(Number(e.target.value))}
                  min="1"
                  max="20"
                  className="w-20 p-2 border rounded"
                />
              </div>
              <Button onClick={searchText} disabled={loading || !indexStatus.text}>
                🔍 Buscar en Texto
              </Button>
            </TabsContent>
            
            <TabsContent value="audio" className="space-y-4 mt-4">
              <div>
                <label className="block text-sm font-medium mb-2">Nombre del archivo de audio:</label>
                <input
                  type="text"
                  value={audioQuery}
                  onChange={(e) => setAudioQuery(e.target.value)}
                  placeholder="Ej: test.wav"
                  className="w-full p-2 border rounded"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-2">Top-K resultados:</label>
                <input
                  type="number"
                  value={topK}
                  onChange={(e) => setTopK(Number(e.target.value))}
                  min="1"
                  max="20"
                  className="w-20 p-2 border rounded"
                />
              </div>
              <Button onClick={searchAudio} disabled={loading || !indexStatus.audio}>
                🔍 Buscar Similares
              </Button>
            </TabsContent>
          </Tabs>
        </CardContent>
      </Card>

      {/* Resultados */}
      <Card>
        <CardHeader>
          <CardTitle>Resultados</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && <div className="text-center py-4">Cargando...</div>}
          {!loading && results.length === 0 && (
            <div className="text-center py-4 text-gray-500">
              No hay resultados. Realiza una búsqueda para ver resultados.
            </div>
          )}
          {!loading && results.length > 0 && (
            <div className="space-y-2">
              {results.map((result, index) => (
                <div key={index} className="p-3 border rounded bg-gray-50">
                  <div className="font-medium">
                    {result.title || result.metadata?.audio_file || `Documento ${result.doc_id || index}`}
                  </div>
                  <div className="text-sm text-gray-600">
                    {result.artist && `Artista: ${result.artist}`}
                    {result.genre && ` | Género: ${result.genre}`}
                    {result.score && ` | Puntuación TF-IDF: ${result.score.toFixed(4)}`}
                    {result.similarity && ` | Similitud: ${result.similarity.toFixed(4)}`}
                    {result.distance && ` | Distancia: ${result.distance.toFixed(4)}`}
                  </div>
                  {result.lyrics && (
                    <div className="text-xs text-gray-500 mt-1">
                      <strong>Letras:</strong> {result.lyrics.substring(0, 200)}...
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
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
      <Tabs defaultValue="proyecto1" className="w-full h-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="proyecto1">Proyecto 1: SQL</TabsTrigger>
          <TabsTrigger value="proyecto2">Proyecto 2: Recomendación</TabsTrigger>
        </TabsList>
        
        <TabsContent value="proyecto1" className="h-full">
          <div className="flex flex-col h-full">
            {/* SQL Editor */}
            <div className="flex-1 p-4">
              <div className="border rounded-lg overflow-hidden h-full">
                <CodeiumEditor
                  value={query}
                  onChange={handleQueryChange}
                  language="sql"
                  theme="dark"
                  className="h-80"
                />
              </div>
            </div>

            {/* Execute Button */}
            <div className="p-4 border-t">
              <Button 
                onClick={handleExecute} 
                disabled={loading}
                className="flex items-center gap-2"
              >
                <Play size={16} />
                {loading ? 'Ejecutando...' : 'Ejecutar Consulta'}
              </Button>
            </div>

            {/* Results */}
            <div className="flex-1 p-4">
              {error && (
                <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-4">
                  <p className="text-red-800">{error}</p>
                </div>
              )}
              <ResultsDisplay results={results} loading={loading} error={error} />
            </div>
          </div>
        </TabsContent>
        
        <TabsContent value="proyecto2" className="h-full overflow-y-auto">
          <Proyecto2Component />
        </TabsContent>
      </Tabs>
    </div>
  );
}
