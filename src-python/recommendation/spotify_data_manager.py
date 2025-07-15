"""
Gestor de datos de Spotify para el Proyecto 2
Maneja descarga, preprocesamiento y gestión de datasets
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyDataManager:
    """Gestor de datos de Spotify para el sistema de recomendación"""
    
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Crear directorios
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Gestor de datos inicializado en: {self.data_dir}")
    
    def download_dataset(self, dataset_name: str = "imuhammad/audio-features-and-lyrics-of-spotify-songs") -> str:
        """Descarga dataset de Kaggle o crea datos sintéticos"""
        dataset_path = self.raw_dir / "spotify_songs.csv"
        
        if dataset_path.exists():
            logger.info(f"Dataset ya existe: {dataset_path}")
            return str(dataset_path)
        
        try:
            # Intentar descargar con kaggle
            import kaggle
            logger.info(f"Descargando dataset de Kaggle: {dataset_name}")
            
            # Configurar kaggle
            os.environ['KAGGLE_CONFIG_DIR'] = str(self.data_dir)
            
            # Descargar dataset
            kaggle.api.dataset_download_files(
                dataset_name,
                path=str(self.raw_dir),
                unzip=True
            )
            
            # Buscar archivo CSV descargado
            csv_files = list(self.raw_dir.glob("*.csv"))
            if csv_files:
                # Renombrar el primer archivo CSV encontrado
                csv_files[0].rename(dataset_path)
                logger.info(f"Dataset descargado: {dataset_path}")
            else:
                raise FileNotFoundError("No se encontró archivo CSV en el dataset descargado")
                
        except Exception as e:
            logger.warning(f"Error descargando dataset: {e}")
            logger.info("Generando dataset sintético...")
            
            # Crear dataset sintético
            dataset_path = self.create_synthetic_dataset()
        
        return str(dataset_path)
    
    def create_synthetic_dataset(self) -> str:
        """Crea un dataset sintético de Spotify para pruebas"""
        logger.info("Creando dataset sintético de Spotify...")
        
        # Géneros musicales
        genres = ['pop', 'rock', 'hip-hop', 'electronic', 'jazz', 'classical', 'country', 'reggae']
        
        # Generar datos sintéticos
        n_songs = 1000
        data = {
            'track_id': [f"track_{i:04d}" for i in range(n_songs)],
            'track_name': [f"Song {i}" for i in range(n_songs)],
            'artist_name': [f"Artist {i // 10}" for i in range(n_songs)],
            'track_genre': np.random.choice(genres, n_songs),
            'danceability': np.random.uniform(0, 1, n_songs),
            'energy': np.random.uniform(0, 1, n_songs),
            'key': np.random.randint(0, 12, n_songs),
            'loudness': np.random.uniform(-60, 0, n_songs),
            'mode': np.random.randint(0, 2, n_songs),
            'speechiness': np.random.uniform(0, 1, n_songs),
            'acousticness': np.random.uniform(0, 1, n_songs),
            'instrumentalness': np.random.uniform(0, 1, n_songs),
            'liveness': np.random.uniform(0, 1, n_songs),
            'valence': np.random.uniform(0, 1, n_songs),
            'tempo': np.random.uniform(50, 200, n_songs),
            'duration_ms': np.random.randint(60000, 300000, n_songs),
            'time_signature': np.random.choice([3, 4, 5], n_songs),
            'popularity': np.random.randint(0, 100, n_songs)
        }
        
        # Agregar letras sintéticas
        sample_lyrics = [
            "love me tender love me true all my dreams fulfill",
            "yesterday all my troubles seemed so far away",
            "imagine all the people living life in peace",
            "we are the champions my friends",
            "bohemian rhapsody is this the real life",
            "stairway to heaven and she's buying a stairway to heaven",
            "hotel california welcome to the hotel california",
            "sweet child of mine she's got eyes of the bluest skies",
            "nothing else matters nothing else matters",
            "smells like teen spirit with the lights out"
        ]
        
        data['lyrics'] = [np.random.choice(sample_lyrics) for _ in range(n_songs)]
        
        # Crear DataFrame
        df = pd.DataFrame(data)
        
        # Guardar dataset
        dataset_path = self.raw_dir / "spotify_songs.csv"
        df.to_csv(dataset_path, index=False)
        
        logger.info(f"Dataset sintético creado: {dataset_path} ({len(df)} canciones)")
        return str(dataset_path)
    
    def preprocess_dataset(self, dataset_path: str) -> str:
        """Preprocesa el dataset para el sistema de recomendación"""
        logger.info(f"Preprocesando dataset: {dataset_path}")
        
        # Leer dataset
        df = pd.read_csv(dataset_path)
        
        # Limpiar datos
        df = df.dropna(subset=['track_name', 'artist_name'])
        
        # Normalizar características numéricas
        numeric_columns = [
            'danceability', 'energy', 'loudness', 'speechiness',
            'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(df[col].median())
        
        # Limpiar texto
        text_columns = ['track_name', 'artist_name', 'track_genre']
        if 'lyrics' in df.columns:
            text_columns.append('lyrics')
        
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
                df[col] = df[col].astype(str)
        
        # Guardar dataset procesado
        processed_path = self.processed_dir / "spotify_songs_processed.csv"
        df.to_csv(processed_path, index=False)
        
        logger.info(f"Dataset procesado: {processed_path} ({len(df)} canciones)")
        return str(processed_path)
    
    def prepare_dataset(self) -> str:
        """Prepara el dataset completo (descarga + preprocesamiento)"""
        # Descargar dataset
        raw_dataset_path = self.download_dataset()
        
        # Preprocesar dataset
        processed_dataset_path = self.preprocess_dataset(raw_dataset_path)
        
        return processed_dataset_path
    
    def generate_synthetic_dataset(self, dataset_name: str) -> str:
        """Genera un dataset sintético y lo guarda"""
        return self.create_synthetic_dataset()
    
    def list_available_datasets(self) -> List[str]:
        """Lista los datasets disponibles"""
        datasets = []
        
        # Buscar en directorio raw
        if self.raw_dir.exists():
            for file in self.raw_dir.glob("*.csv"):
                datasets.append(file.stem)
        
        # Buscar en directorio processed
        if self.processed_dir.exists():
            for file in self.processed_dir.glob("*.csv"):
                datasets.append(f"{file.stem}_processed")
        
        return datasets
    
    def get_dataset_info(self, dataset_name: str = "spotify_songs") -> Dict:
        """Obtiene información de un dataset específico"""
        processed_path = self.processed_dir / "spotify_songs_processed.csv"
        raw_path = self.raw_dir / "spotify_songs.csv"
        
        # Intentar cargar dataset procesado primero
        dataset_path = processed_path if processed_path.exists() else raw_path
        
        if not dataset_path.exists():
            return {"error": "Dataset no encontrado"}
        
        df = pd.read_csv(dataset_path)
        
        return {
            "total_songs": len(df),
            "artists": df['artist_name'].nunique() if 'artist_name' in df.columns else 0,
            "genres": df['track_genre'].nunique() if 'track_genre' in df.columns else 0,
            "columns": list(df.columns),
            "sample_songs": df[['track_name', 'artist_name']].head(5).to_dict('records') if 'track_name' in df.columns and 'artist_name' in df.columns else []
        }


class ExperimentManager:
    """Gestor de experimentos para evaluar el sistema de recomendación"""
    
    def __init__(self, data_manager: SpotifyDataManager):
        self.data_manager = data_manager
        self.results = []
    
    def run_text_search_experiment(self, queries: List[str], top_k: int = 5) -> Dict:
        """Ejecuta experimento de búsqueda textual"""
        # Implementar en el futuro
        pass
    
    def run_audio_search_experiment(self, query_ids: List[int], top_k: int = 5) -> Dict:
        """Ejecuta experimento de búsqueda de audio"""
        # Implementar en el futuro
        pass
