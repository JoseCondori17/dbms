from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
from typing import Annotated, Optional, List
import os
import shutil
import json

from engine.executor import PKAdmin
from recommendation.spimi_index import SPIMIIndexBuilder, TextSearchEngine
from recommendation.knn_audio_index import (
    AudioFeatureExtractor, KNNSequentialSearch, 
    KNNInvertedIndex, AcousticCodebook
)
from recommendation.spotify_data_manager import SpotifyDataManager

pk_admin_instance = None
# Instancias para el proyecto 2 (inicializadas en lifespan)
spimi_builder = None
text_search_engine = None
audio_extractor = None
knn_sequential = None
knn_inverted = None
acoustic_codebook = None
spotify_manager = None

# start
@asynccontextmanager
async def lifespan(app: FastAPI):
    global pk_admin_instance, spimi_builder, text_search_engine, audio_extractor
    global knn_sequential, knn_inverted, acoustic_codebook, spotify_manager
    
    pk_admin_instance = PKAdmin()
    
    # Crear directorios necesarios para el proyecto 2
    os.makedirs("data/recommendation/text_index", exist_ok=True)
    os.makedirs("data/recommendation/audio_index", exist_ok=True)
    os.makedirs("data/recommendation/datasets", exist_ok=True)
    
    # Inicializar instancias del proyecto 2
    spimi_builder = SPIMIIndexBuilder("data/recommendation/text_index")
    text_search_engine = TextSearchEngine("data/recommendation/text_index")
    audio_extractor = AudioFeatureExtractor()
    knn_sequential = KNNSequentialSearch("data/recommendation/audio_index")
    knn_inverted = KNNInvertedIndex("data/recommendation/audio_index")
    acoustic_codebook = AcousticCodebook()
    spotify_manager = SpotifyDataManager("data/recommendation/datasets")
    
    yield

# app
app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# dep
def get_pkadmin() -> PKAdmin:
    if pk_admin_instance is None:
        raise RuntimeError("Error: PKAdmin not initialized")
    return pk_admin_instance

PKAdminDep = Annotated[PKAdmin, Depends(get_pkadmin)]

# //////////////////////////////////////////////////////////////////////
# Modelos para el Proyecto 1
class Query(BaseModel):
    query: str

# Modelos para el Proyecto 2
class TextIndexRequest(BaseModel):
    collection_name: str
    documents: List[str]
    memory_limit: Optional[int] = 1024

class TextSearchRequest(BaseModel):
    collection_name: str
    query: str
    k: Optional[int] = 10

class AudioIndexRequest(BaseModel):
    collection_name: str
    audio_files: List[str]
    use_inverted: Optional[bool] = False

class AudioSearchRequest(BaseModel):
    collection_name: str
    query_audio: str
    k: Optional[int] = 10
    use_inverted: Optional[bool] = False

class DatasetRequest(BaseModel):
    dataset_name: str
    use_synthetic: Optional[bool] = False

# //////////////////////////////////////////////////////////////////////
# API Endpoints para el Proyecto 1 - SQL
@app.get("/databases")
async def get_databases(pk_admin: PKAdminDep):
    return pk_admin.catalog.get_databases_json()

@app.get("/{db_name}/schemas")
async def get_schemas(pk_admin: PKAdminDep, db_name: str):
    return pk_admin.catalog.get_schemas_json(db_name)

@app.get("/{db_name}/{schema_name}/tables")
async def get_schemas(pk_admin: PKAdminDep, db_name: str, schema_name: str):
    return pk_admin.catalog.get_tables_json(db_name, schema_name)

@app.post("/execute")
async def execute_query(pk_admin: PKAdminDep, body: Query):
    result = pk_admin.execute(body.query)
    return result

# //////////////////////////////////////////////////////////////////////
# API Endpoints para el Proyecto 2 - Recomendación Multimedia

# Endpoints para búsqueda de texto con SPIMI
@app.post("/recommendation/text/build-index")
async def build_text_index(request: TextIndexRequest):
    """Construir índice de texto usando SPIMI"""
    try:
        success = spimi_builder.build_index(
            request.documents,
            request.collection_name,
            request.memory_limit
        )
        if success:
            return {"message": f"Índice de texto '{request.collection_name}' construido exitosamente"}
        else:
            raise HTTPException(status_code=500, detail="Error al construir el índice")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recommendation/text/search")
async def search_text(request: TextSearchRequest):
    """Buscar en el índice de texto"""
    try:
        results = text_search_engine.search(
            request.query,
            request.collection_name,
            request.k
        )
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendation/text/collections")
async def get_text_collections():
    """Obtener lista de colecciones de texto"""
    try:
        collections = text_search_engine.get_available_collections()
        return {"collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoints para búsqueda de audio con KNN
@app.post("/recommendation/audio/build-index")
async def build_audio_index(request: AudioIndexRequest):
    """Construir índice de audio usando KNN"""
    try:
        if request.use_inverted:
            # Construir codebook acústico primero
            acoustic_codebook.build_codebook(request.audio_files)
            success = knn_inverted.build_index(request.audio_files, request.collection_name)
        else:
            success = knn_sequential.build_index(request.audio_files, request.collection_name)
        
        if success:
            index_type = "invertido" if request.use_inverted else "secuencial"
            return {"message": f"Índice de audio {index_type} '{request.collection_name}' construido exitosamente"}
        else:
            raise HTTPException(status_code=500, detail="Error al construir el índice")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/recommendation/audio/search")
async def search_audio(request: AudioSearchRequest):
    """Buscar en el índice de audio"""
    try:
        if request.use_inverted:
            results = knn_inverted.search(request.query_audio, request.collection_name, request.k)
        else:
            results = knn_sequential.search(request.query_audio, request.collection_name, request.k)
        
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendation/audio/collections")
async def get_audio_collections():
    """Obtener lista de colecciones de audio"""
    try:
        seq_collections = knn_sequential.get_available_collections()
        inv_collections = knn_inverted.get_available_collections()
        return {
            "sequential": seq_collections,
            "inverted": inv_collections
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoints para gestión de datasets
@app.post("/recommendation/dataset/download")
async def download_dataset(request: DatasetRequest):
    """Descargar dataset desde Kaggle o generar sintético"""
    try:
        if request.use_synthetic:
            dataset_path = spotify_manager.generate_synthetic_dataset(request.dataset_name)
        else:
            dataset_path = spotify_manager.download_dataset(request.dataset_name)
        
        return {"message": f"Dataset '{request.dataset_name}' descargado/generado exitosamente", "path": dataset_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendation/dataset/info/{dataset_name}")
async def get_dataset_info(dataset_name: str):
    """Obtener información del dataset"""
    try:
        info = spotify_manager.get_dataset_info(dataset_name)
        return {"dataset_info": info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/recommendation/dataset/list")
async def list_datasets():
    """Listar datasets disponibles"""
    try:
        datasets = spotify_manager.list_available_datasets()
        return {"datasets": datasets}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint para obtener estadísticas del sistema
@app.get("/recommendation/stats")
async def get_system_stats():
    """Obtener estadísticas del sistema de recomendación"""
    try:
        stats = {
            "text_collections": len(text_search_engine.get_available_collections()),
            "audio_collections": {
                "sequential": len(knn_sequential.get_available_collections()),
                "inverted": len(knn_inverted.get_available_collections())
            },
            "datasets": len(spotify_manager.list_available_datasets())
        }
        return {"stats": stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint para upload de archivos de audio
@app.post("/recommendation/audio/upload")
async def upload_audio_file(file: UploadFile = File(...)):
    """Subir archivo de audio para procesamiento"""
    try:
        upload_dir = "data/recommendation/audio_uploads"
        os.makedirs(upload_dir, exist_ok=True)
        
        file_path = os.path.join(upload_dir, file.filename)
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        return {"message": f"Archivo '{file.filename}' subido exitosamente", "path": file_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint para upload de documentos de texto
@app.post("/recommendation/text/upload")
async def upload_text_documents(files: List[UploadFile] = File(...)):
    """Subir documentos de texto para indexación"""
    try:
        upload_dir = "data/recommendation/text_uploads"
        os.makedirs(upload_dir, exist_ok=True)
        
        uploaded_files = []
        for file in files:
            if file.content_type.startswith("text/"):
                file_path = os.path.join(upload_dir, file.filename)
                with open(file_path, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                uploaded_files.append(file_path)
        
        return {"message": f"{len(uploaded_files)} documentos subidos exitosamente", "files": uploaded_files}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# //////////////////////////////////////////////////////////////////////
# API Endpoints compatibles con el frontend (rutas /api/)
@app.get("/api/status")
async def get_status():
    """Endpoint de estado compatible con frontend"""
    try:
        stats = {
            "system_status": "running",
            "text_collections": len(text_search_engine.get_available_collections()),
            "audio_collections": {
                "sequential": len(knn_sequential.get_available_collections()),
                "inverted": len(knn_inverted.get_available_collections())
            },
            "datasets": len(spotify_manager.list_available_datasets())
        }
        return {"status": "success", "data": stats}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/build-text-index")
async def build_text_index_api():
    """Construir índice de texto usando datos sintéticos"""
    try:
        # Generar dataset sintético
        dataset_path = spotify_manager.generate_synthetic_dataset("spotify_songs")
        
        # Construir índice usando el dataset
        spimi_builder.build_index(dataset_path)
        
        return {"status": "success", "message": "Índice de texto construido exitosamente"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/build-audio-index")
async def build_audio_index_api():
    """Construir índice de audio usando datos sintéticos"""
    try:
        # Generar dataset sintético
        dataset_path = spotify_manager.generate_synthetic_dataset("spotify_songs")
        
        # Leer dataset
        import pandas as pd
        df = pd.read_csv(dataset_path)
        
        # Crear archivos de audio simulados
        audio_files = [f"audio_{i}.wav" for i in range(len(df))]
        
        # Construir índice KNN secuencial
        success = knn_sequential.build_index(audio_files, "spotify_audio_collection")
        
        if success:
            return {"status": "success", "message": "Índice de audio construido exitosamente"}
        else:
            return {"status": "error", "message": "Error al construir índice"}
            
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/search-text")
async def search_text_api(request: dict):
    """Buscar en el índice de texto"""
    try:
        query = request.get("query", "")
        k = request.get("k", 5)
        
        if not query.strip():
            return {"status": "error", "message": "Query no puede estar vacío"}
        
        # Usar directamente el motor de búsqueda sin especificar colección
        results = text_search_engine.search(query, k)
        
        # Formatear resultados para el frontend
        formatted_results = []
        for result in results:
            formatted_results.append({
                "id": result.get("doc_id", "unknown"),
                "title": result.get("title", "Sin título"),
                "artist": result.get("artist", "Artista desconocido"),
                "genre": result.get("genre", "Género desconocido"),
                "score": round(result.get("score", 0.0), 4),
                "similarity": round(result.get("score", 0.0), 4)
            })
        
        return {"status": "success", "results": formatted_results}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/api/search-audio")
async def search_audio_api(request: dict):
    """Buscar en el índice de audio"""
    try:
        query_audio = request.get("query_audio", "random_audio.wav")
        k = request.get("k", 5)
        
        results = knn_sequential.search(
            query_audio,
            "spotify_audio_collection",
            k
        )
        
        return {"status": "success", "results": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}