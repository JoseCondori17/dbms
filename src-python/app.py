from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Annotated

from engine.executor import PKAdmin

pk_admin_instance = None

# start
@asynccontextmanager
async def lifespan(app: FastAPI):
    global pk_admin_instance
    pk_admin_instance = PKAdmin()
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
# api.pk_admin
@app.get("/databases")
async def get_catalog_info(pk_admin: PKAdminDep):
    databases = pk_admin.catalog.get_database_names()
    return { 'data': databases }

#@app.get("/databases")
