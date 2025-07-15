"""
SPIMI (Single-Pass In-Memory Indexing) Implementation
Para búsqueda textual con ranking TF-IDF
"""

import os
import json
import math
import pickle
from collections import defaultdict, Counter
from typing import Dict, List, Tuple, Any
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SPIMIIndexBuilder:
    """Construye índices SPIMI para búsqueda textual"""
    
    def __init__(self, index_dir: str, block_size: int = 10000):
        self.index_dir = index_dir
        self.block_size = block_size
        self.stemmer = PorterStemmer()
        
        # Crear directorio de índices
        os.makedirs(index_dir, exist_ok=True)
        
        # Descargar recursos NLTK si no existen
        try:
            nltk.data.find('tokenizers/punkt')
        except LookupError:
            nltk.download('punkt')
        
        try:
            nltk.data.find('corpora/stopwords')
        except LookupError:
            nltk.download('stopwords')
        
        self.stop_words = set(stopwords.words('english'))
    
    def preprocess_text(self, text: str) -> List[str]:
        """Preprocesa texto: tokenización, stemming, eliminación de stopwords"""
        if not text or pd.isna(text):
            return []
        
        # Tokenización
        tokens = word_tokenize(text.lower())
        
        # Filtrar tokens válidos
        tokens = [token for token in tokens if token.isalpha() and len(token) > 2]
        
        # Eliminar stopwords y aplicar stemming
        tokens = [self.stemmer.stem(token) for token in tokens if token not in self.stop_words]
        
        return tokens
    
    def build_index(self, dataset_path: str) -> None:
        """Construye el índice SPIMI desde un dataset"""
        logger.info(f"Construyendo índice SPIMI desde: {dataset_path}")
        
        # Leer dataset
        df = pd.read_csv(dataset_path)
        
        # Combinar columnas de texto relevantes
        text_columns = ['track_name', 'artist_name', 'track_genre']
        if 'lyrics' in df.columns:
            text_columns.append('lyrics')
        
        # Construir índice
        inverted_index = defaultdict(list)
        document_frequencies = defaultdict(int)
        total_documents = 0
        
        for idx, row in df.iterrows():
            # Combinar texto de todas las columnas
            combined_text = ' '.join([str(row.get(col, '')) for col in text_columns])
            
            # Preprocesar texto
            terms = self.preprocess_text(combined_text)
            
            if not terms:
                continue
            
            # Contar frecuencias de términos en el documento
            term_counts = Counter(terms)
            
            # Agregar al índice invertido
            for term, count in term_counts.items():
                inverted_index[term].append({
                    'doc_id': idx,
                    'tf': count,
                    'title': row.get('track_name', f'Document {idx}'),
                    'artist': row.get('artist_name', 'Unknown'),
                    'genre': row.get('track_genre', 'Unknown')
                })
                document_frequencies[term] += 1
            
            total_documents += 1
        
        # Calcular TF-IDF
        logger.info("Calculando TF-IDF...")
        for term in inverted_index:
            idf = math.log(total_documents / document_frequencies[term])
            for doc_entry in inverted_index[term]:
                doc_entry['tfidf'] = doc_entry['tf'] * idf
        
        # Guardar índice
        index_file = os.path.join(self.index_dir, 'spimi_index.pkl')
        with open(index_file, 'wb') as f:
            pickle.dump(dict(inverted_index), f)
        
        # Guardar metadatos
        metadata = {
            'total_documents': total_documents,
            'total_terms': len(inverted_index),
            'document_frequencies': dict(document_frequencies)
        }
        
        metadata_file = os.path.join(self.index_dir, 'metadata.json')
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Índice SPIMI construido: {len(inverted_index)} términos, {total_documents} documentos")


class TextSearchEngine:
    """Motor de búsqueda textual usando índices SPIMI"""
    
    def __init__(self, index_dir: str):
        self.index_dir = index_dir
        self.stemmer = PorterStemmer()
        self.stop_words = set(stopwords.words('english'))
        
        # Inicializar estructuras vacías
        self.inverted_index = {}
        self.metadata = {}
        
        # Cargar índice si existe
        try:
            self.load_index()
        except FileNotFoundError:
            # Si no hay índice existente, continuar con índices vacíos
            pass
    
    def load_index(self) -> None:
        """Carga el índice SPIMI desde disco"""
        index_file = os.path.join(self.index_dir, 'spimi_index.pkl')
        metadata_file = os.path.join(self.index_dir, 'metadata.json')
        
        if not os.path.exists(index_file) or not os.path.exists(metadata_file):
            raise FileNotFoundError("Índice SPIMI no encontrado. Construye el índice primero.")
        
        with open(index_file, 'rb') as f:
            self.inverted_index = pickle.load(f)
        
        with open(metadata_file, 'r') as f:
            self.metadata = json.load(f)
        
        logger.info(f"Índice cargado: {self.metadata['total_terms']} términos")
    
    def preprocess_query(self, query: str) -> List[str]:
        """Preprocesa la consulta de búsqueda"""
        tokens = word_tokenize(query.lower())
        tokens = [token for token in tokens if token.isalpha() and len(token) > 2]
        tokens = [self.stemmer.stem(token) for token in tokens if token not in self.stop_words]
        return tokens
    
    def search(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Realiza búsqueda textual con ranking TF-IDF"""
        if not query.strip():
            return []
        
        # Preprocesar consulta
        query_terms = self.preprocess_query(query)
        
        if not query_terms:
            return []
        
        # Calcular puntuaciones de documentos
        doc_scores = defaultdict(float)
        
        for term in query_terms:
            if term in self.inverted_index:
                for doc_entry in self.inverted_index[term]:
                    doc_scores[doc_entry['doc_id']] += doc_entry['tfidf']
        
        # Ordenar por puntuación
        sorted_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Construir resultados
        results = []
        for doc_id, score in sorted_docs[:top_k]:
            # Obtener información del documento desde el índice
            doc_info = None
            for term in query_terms:
                if term in self.inverted_index:
                    for doc_entry in self.inverted_index[term]:
                        if doc_entry['doc_id'] == doc_id:
                            doc_info = doc_entry
                            break
                    if doc_info:
                        break
            
            if doc_info:
                results.append({
                    'doc_id': doc_id,
                    'score': score,
                    'title': doc_info['title'],
                    'artist': doc_info['artist'],
                    'genre': doc_info['genre']
                })
        
        return results
    
    def get_available_collections(self) -> List[str]:
        """Obtiene lista de colecciones disponibles"""
        if not os.path.exists(self.index_dir):
            return []
        
        collections = []
        for filename in os.listdir(self.index_dir):
            if filename.endswith('_spimi_index.pkl'):
                collections.append(filename.replace('_spimi_index.pkl', ''))
            elif filename == 'spimi_index.pkl':
                collections.append('default')
        
        return collections
