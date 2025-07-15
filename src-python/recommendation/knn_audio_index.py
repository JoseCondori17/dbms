import numpy as np
import os
import pickle
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AudioFeatureExtractor:
    """Extrae características de audio usando MFCC simuladas"""
    
    def __init__(self, n_mfcc: int = 13, n_samples: int = 1000):
        self.n_mfcc = n_mfcc
        self.n_samples = n_samples
    
    def extract_mfcc_features(self, audio_features: Dict[str, float]) -> np.ndarray:
        """
        Simula extracción de características MFCC usando features de Spotify
        En un caso real, esto usaría librosa para procesar archivos de audio
        """
        # Simular MFCC basado en features de Spotify
        mfcc = np.random.random(self.n_mfcc)
        
        # Usar algunas características reales de Spotify para hacer más realista
        if 'danceability' in audio_features:
            mfcc[0] = audio_features['danceability']
        if 'energy' in audio_features:
            mfcc[1] = audio_features['energy']
        if 'valence' in audio_features:
            mfcc[2] = audio_features['valence']
        if 'tempo' in audio_features:
            mfcc[3] = audio_features['tempo'] / 200.0  # Normalizar tempo
        
        return mfcc
    
    def extract_features_from_file(self, audio_file: str) -> np.ndarray:
        """
        Extrae características de un archivo de audio
        En un caso real, esto usaría librosa para procesar el archivo
        """
        logger.info(f"Extrayendo características de {audio_file}")
        
        # Simular extracción de características
        return np.random.random(self.n_mfcc)
    
    def extract_batch_features(self, audio_files: List[str]) -> Dict[int, np.ndarray]:
        """Extrae características de múltiples archivos"""
        features = {}
        
        for i, audio_file in enumerate(audio_files):
            features[i] = self.extract_features_from_file(audio_file)
        
        return features

class AcousticCodebook:
    """Codebook acústico usando K-Means para Bag of Acoustic Words"""
    
    def __init__(self, k: int = 100):
        self.k = k
        self.kmeans = None
        self.codebook = None
    
    def build_codebook(self, features: Dict[int, np.ndarray]) -> None:
        """Construye codebook usando K-Means"""
        logger.info(f"Construyendo codebook acústico con k={self.k}")
        
        # Preparar datos para clustering
        feature_vectors = np.array(list(features.values()))
        
        # Aplicar K-Means
        self.kmeans = KMeans(n_clusters=self.k, random_state=42, n_init=10)
        self.kmeans.fit(feature_vectors)
        
        # Guardar codebook
        self.codebook = self.kmeans.cluster_centers_
        
        logger.info(f"Codebook construido con {self.k} palabras acústicas")
    
    def get_acoustic_words(self, feature_vector: np.ndarray) -> np.ndarray:
        """Convierte vector de características a palabras acústicas"""
        if self.kmeans is None:
            raise ValueError("Codebook no construido. Llama a build_codebook primero.")
        
        # Obtener cluster más cercano
        cluster_id = self.kmeans.predict([feature_vector])[0]
        
        # Crear representación bag-of-words
        bow = np.zeros(self.k)
        bow[cluster_id] = 1.0
        
        return bow
    
    def save_codebook(self, filepath: str) -> None:
        """Guarda codebook en disco"""
        data = {
            'k': self.k,
            'kmeans': self.kmeans,
            'codebook': self.codebook
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
    
    def load_codebook(self, filepath: str) -> None:
        """Carga codebook desde disco"""
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        self.k = data['k']
        self.kmeans = data['kmeans']
        self.codebook = data['codebook']


class KNNSequentialSearch:
    """Búsqueda KNN secuencial para audio"""
    
    def __init__(self, index_dir: str):
        self.index_dir = index_dir
        self.features = {}
        self.codebook = None
        self.acoustic_words = {}
    
    def build_index(self, audio_files: List[str], collection_name: str) -> bool:
        """Construye índice KNN secuencial"""
        try:
            # Crear directorio si no existe
            os.makedirs(self.index_dir, exist_ok=True)
            
            # Simular extracción de características
            extractor = AudioFeatureExtractor()
            self.features = {}
            
            for i, audio_file in enumerate(audio_files):
                # Simular features de audio
                features = {
                    'danceability': np.random.random(),
                    'energy': np.random.random(),
                    'valence': np.random.random(),
                    'tempo': np.random.random() * 200
                }
                self.features[i] = extractor.extract_mfcc_features(features)
            
            # Construir codebook
            self.codebook = AcousticCodebook()
            self.codebook.build_codebook(self.features)
            
            # Construir representaciones acústicas
            self.acoustic_words = self._build_acoustic_representations()
            
            # Guardar índice
            self._save_index(collection_name)
            
            return True
        except Exception as e:
            logger.error(f"Error al construir índice KNN: {e}")
            return False
    
    def _build_acoustic_representations(self) -> Dict[int, np.ndarray]:
        """Construye representaciones de palabras acústicas"""
        acoustic_words = {}
        
        for doc_id, feature_vector in self.features.items():
            acoustic_words[doc_id] = self.codebook.get_acoustic_words(feature_vector)
        
        return acoustic_words
    
    def search(self, query_audio: str, collection_name: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Búsqueda KNN secuencial"""
        try:
            # Cargar índice si no está cargado
            if not self.features:
                self._load_index(collection_name)
            
            # Simular query
            query_features = {
                'danceability': np.random.random(),
                'energy': np.random.random(),
                'valence': np.random.random(),
                'tempo': np.random.random() * 200
            }
            
            extractor = AudioFeatureExtractor()
            query_vector = extractor.extract_mfcc_features(query_features)
            query_words = self.codebook.get_acoustic_words(query_vector)
            
            # Calcular similitudes
            similarities = []
            for doc_id, acoustic_words in self.acoustic_words.items():
                similarity = self._calculate_similarity(query_words, acoustic_words)
                similarities.append({
                    'doc_id': doc_id,
                    'similarity': similarity,
                    'metadata': {'audio_file': f'audio_{doc_id}.wav'}
                })
            
            # Ordenar por similitud
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            
            return similarities[:top_k]
        except Exception as e:
            logger.error(f"Error en búsqueda KNN: {e}")
            return []
    
    def _calculate_similarity(self, query_words: np.ndarray, doc_words: np.ndarray) -> float:
        """Calcula similitud entre palabras acústicas"""
        # Usar similitud coseno
        dot_product = np.dot(query_words, doc_words)
        norm_query = np.linalg.norm(query_words)
        norm_doc = np.linalg.norm(doc_words)
        
        if norm_query == 0 or norm_doc == 0:
            return 0.0
        
        return dot_product / (norm_query * norm_doc)
    
    def _save_index(self, collection_name: str) -> None:
        """Guarda índice en disco"""
        index_file = os.path.join(self.index_dir, f'{collection_name}_sequential.pkl')
        
        data = {
            'features': self.features,
            'codebook': self.codebook,
            'acoustic_words': self.acoustic_words
        }
        
        with open(index_file, 'wb') as f:
            pickle.dump(data, f)
    
    def _load_index(self, collection_name: str) -> None:
        """Carga índice desde disco"""
        index_file = os.path.join(self.index_dir, f'{collection_name}_sequential.pkl')
        
        if not os.path.exists(index_file):
            raise FileNotFoundError(f"Índice {collection_name} no encontrado")
        
        with open(index_file, 'rb') as f:
            data = pickle.load(f)
        
        self.features = data['features']
        self.codebook = data['codebook']
        self.acoustic_words = data['acoustic_words']
    
    def get_available_collections(self) -> List[str]:
        """Obtiene lista de colecciones disponibles"""
        if not os.path.exists(self.index_dir):
            return []
        
        collections = []
        for filename in os.listdir(self.index_dir):
            if filename.endswith('_sequential.pkl'):
                collections.append(filename.replace('_sequential.pkl', ''))
        
        return collections


class KNNInvertedIndex:
    """Índice invertido KNN para audio"""
    
    def __init__(self, index_dir: str):
        self.index_dir = index_dir
        self.features = {}
        self.codebook = None
        self.inverted_index = {}
    
    def build_index(self, audio_files: List[str], collection_name: str) -> bool:
        """Construye índice invertido KNN"""
        try:
            # Crear directorio si no existe
            os.makedirs(self.index_dir, exist_ok=True)
            
            # Simular extracción de características
            extractor = AudioFeatureExtractor()
            self.features = {}
            
            for i, audio_file in enumerate(audio_files):
                # Simular features de audio
                features = {
                    'danceability': np.random.random(),
                    'energy': np.random.random(),
                    'valence': np.random.random(),
                    'tempo': np.random.random() * 200
                }
                self.features[i] = extractor.extract_mfcc_features(features)
            
            # Construir codebook
            self.codebook = AcousticCodebook()
            self.codebook.build_codebook(self.features)
            
            # Construir índice invertido
            self.inverted_index = self._build_inverted_index()
            
            # Guardar índice
            self._save_index(collection_name)
            
            return True
        except Exception as e:
            logger.error(f"Error al construir índice invertido KNN: {e}")
            return False
    
    def _build_inverted_index(self) -> Dict[int, List[int]]:
        """Construye índice invertido"""
        inverted_index = {}
        
        for doc_id, feature_vector in self.features.items():
            acoustic_words = self.codebook.get_acoustic_words(feature_vector)
            
            # Para cada palabra acústica, añadir documento al índice invertido
            for word_id in range(len(acoustic_words)):
                if acoustic_words[word_id] > 0:  # Solo palabras presentes
                    if word_id not in inverted_index:
                        inverted_index[word_id] = []
                    inverted_index[word_id].append(doc_id)
        
        return inverted_index
    
    def search(self, query_audio: str, collection_name: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """Búsqueda usando índice invertido"""
        try:
            # Cargar índice si no está cargado
            if not self.features:
                self._load_index(collection_name)
            
            # Simular query
            query_features = {
                'danceability': np.random.random(),
                'energy': np.random.random(),
                'valence': np.random.random(),
                'tempo': np.random.random() * 200
            }
            
            extractor = AudioFeatureExtractor()
            query_vector = extractor.extract_mfcc_features(query_features)
            query_words = self.codebook.get_acoustic_words(query_vector)
            
            # Usar índice invertido para encontrar documentos candidatos
            candidates = set()
            for word_id in range(len(query_words)):
                if query_words[word_id] > 0 and word_id in self.inverted_index:
                    candidates.update(self.inverted_index[word_id])
            
            # Calcular similitudes solo para candidatos
            similarities = []
            for doc_id in candidates:
                doc_words = self.codebook.get_acoustic_words(self.features[doc_id])
                similarity = self._calculate_similarity(query_words, doc_words)
                similarities.append({
                    'doc_id': doc_id,
                    'similarity': similarity,
                    'metadata': {'audio_file': f'audio_{doc_id}.wav'}
                })
            
            # Ordenar por similitud
            similarities.sort(key=lambda x: x['similarity'], reverse=True)
            
            return similarities[:top_k]
        except Exception as e:
            logger.error(f"Error en búsqueda invertida KNN: {e}")
            return []
    
    def _calculate_similarity(self, query_words: np.ndarray, doc_words: np.ndarray) -> float:
        """Calcula similitud entre palabras acústicas"""
        # Usar similitud coseno
        dot_product = np.dot(query_words, doc_words)
        norm_query = np.linalg.norm(query_words)
        norm_doc = np.linalg.norm(doc_words)
        
        if norm_query == 0 or norm_doc == 0:
            return 0.0
        
        return dot_product / (norm_query * norm_doc)
    
    def _save_index(self, collection_name: str) -> None:
        """Guarda índice en disco"""
        index_file = os.path.join(self.index_dir, f'{collection_name}_inverted.pkl')
        
        data = {
            'features': self.features,
            'codebook': self.codebook,
            'inverted_index': self.inverted_index
        }
        
        with open(index_file, 'wb') as f:
            pickle.dump(data, f)
    
    def _load_index(self, collection_name: str) -> None:
        """Carga índice desde disco"""
        index_file = os.path.join(self.index_dir, f'{collection_name}_inverted.pkl')
        
        if not os.path.exists(index_file):
            raise FileNotFoundError(f"Índice {collection_name} no encontrado")
        
        with open(index_file, 'rb') as f:
            data = pickle.load(f)
        
        self.features = data['features']
        self.codebook = data['codebook']
        self.inverted_index = data['inverted_index']
    
    def get_available_collections(self) -> List[str]:
        """Obtiene lista de colecciones disponibles"""
        if not os.path.exists(self.index_dir):
            return []
        
        collections = []
        for filename in os.listdir(self.index_dir):
            if filename.endswith('_inverted.pkl'):
                collections.append(filename.replace('_inverted.pkl', ''))
        
        return collections
