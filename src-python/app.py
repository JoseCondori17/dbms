import streamlit as st
from engine.executor import PKAdmin
import pandas as pd

st.set_page_config(page_title="DBMS Admin", layout="wide")

st.title("DBMS Admin - Interfaz tipo Supabase")

# Inicializar el admin
if 'admin' not in st.session_state:
    st.session_state.admin = PKAdmin()
admin = st.session_state.admin

# --- Panel lateral: Explorador de catálogo ---
st.sidebar.header("Catálogo de Bases de Datos")
db_names = admin.catalog.get_database_names()
selected_db = st.sidebar.selectbox("Base de datos", db_names) if db_names else None

schemas = admin.catalog.get_schemas_dict(selected_db) if selected_db else {}
selected_schema = st.sidebar.selectbox("Esquema", list(schemas.keys())) if schemas else None

tables = admin.catalog.get_tables(selected_db, selected_schema) if selected_db and selected_schema else []
table_names = [t.tab_name for t in tables]
selected_table = st.sidebar.selectbox("Tabla", table_names) if table_names else None

st.sidebar.markdown("---")
if st.sidebar.button("Crear nueva base de datos"):
    st.session_state['show_create_db'] = True

# --- Panel principal ---
tabs = st.tabs(["Editor SQL", "Vista de Tabla"])

# --- Editor SQL ---
with tabs[0]:
    st.subheader("Editor SQL")
    sql_query = st.text_area("Escribe tu consulta SQL", height=150)
    if st.button("Ejecutar consulta"):
        try:
            admin.execute(sql_query)
            st.success("Consulta ejecutada correctamente.")
        except Exception as e:
            st.error(f"Error: {e}")

# --- Vista de Tabla ---
with tabs[1]:
    st.subheader(f"Vista de Tabla: {selected_table}" if selected_table else "Selecciona una tabla")
    if selected_table:
        # Intentar leer los datos de la tabla (esto depende de tu implementación interna)
        try:
            # Aquí deberías implementar la lógica para leer los datos de la tabla seleccionada
            st.info("Funcionalidad de vista de datos pendiente de implementación.")
        except Exception as e:
            st.error(f"Error al cargar datos: {e}")

# --- Crear nueva base de datos ---
if st.session_state.get('show_create_db', False):
    with st.form("Crear base de datos", clear_on_submit=True):
        new_db_name = st.text_input("Nombre de la nueva base de datos")
        submitted = st.form_submit_button("Crear")
        if submitted and new_db_name:
            try:
                admin.catalog.create_database(new_db_name)
                st.success(f"Base de datos '{new_db_name}' creada.")
                st.session_state['show_create_db'] = False
            except Exception as e:
                st.error(f"Error: {e}")