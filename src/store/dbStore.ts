import { create } from 'zustand';

interface DbStoreState {
    selectedDb: string | null;
    selectedSchema: string | null;
    tables: string[];
    setSelectedDb: (db: string | null) => void;
    setSelectedSchema: (schema: string | null) => void;
    setTables: (tables: string[]) => void;
}

export const useDbStore = create<DbStoreState>((set) => ({
    selectedDb: null,
    selectedSchema: null,
    tables: [],
    setSelectedDb: (db) => set({ selectedDb: db, selectedSchema: null, tables: [] }),
    setSelectedSchema: (schema) => set({ selectedSchema: schema, tables: [] }),
    setTables: (tables) => set({ tables }),
})); 