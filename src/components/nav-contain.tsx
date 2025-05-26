"use client"
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { useDbStore } from "@/store/dbStore";
import { Layers } from "lucide-react";
import * as React from "react";
import { buttonVariants } from "./ui/button";

interface Schema {
  sch_id: number;
  sch_name: string;
  sch_db_id: number;
  sch_tables: Record<string, number>;
  sch_functions: object;
}

interface TableColumn {
  att_name: string;
  att_type_id: number;
  att_len: number;
  att_not_null: boolean;
  att_has_def: boolean;
}

interface TableIndex {
  idx_id: number;
  idx_type: number;
  idx_name: string;
  idx_file: string;
  idx_tuples: number;
  idx_columns: number[];
  idx_is_primary: boolean;
}

interface Table {
  tab_id: number;
  tab_name: string;
  tab_namespace: number;
  tab_tuples: number;
  tab_pages: number;
  tab_page_size: number;
  tab_columns: TableColumn[];
  tab_indexes: TableIndex[];
}

function TableColumnsAccordion({ columns }: { columns: TableColumn[] }) {
  return (
    <ul className="pl-4 flex flex-col gap-y-1">
      {columns.map((col) => (
        <li key={col.att_name} className="flex justify-between text-xs uppercase">
          <span>{col.att_name}</span>
          <span className="text-muted-foreground ml-2">{col.att_len}</span>
        </li>
      ))}
    </ul>
  );
}

function TableIndexesAccordion({ indexes }: { indexes: TableIndex[] }) {
  return (
    <ul className="pl-4 flex flex-col gap-y-1">
      {indexes.map((idx) => (
        <li key={idx.idx_id} className="flex justify-between text-xs">
          <span className="truncate max-w-[120px]">{idx.idx_name}</span>
          <span className="text-muted-foreground ml-2">{idx.idx_type}</span>
        </li>
      ))}
    </ul>
  );
}

function TablesAndIndexes({ tables }: { tables: Table[] }) {
  return (
    <Accordion type="multiple" className="w-full">
      <AccordionItem value="tables" className="border-b-0">
        <AccordionTrigger className="text-sm font-semibold">Tablas</AccordionTrigger>
        <AccordionContent>
          <Accordion type="multiple" className="w-full">
            {tables.map((table) => (
              <AccordionItem value={table.tab_name} key={table.tab_id} className="border-b-0">
                <AccordionTrigger className="text-xs uppercase font-medium">{table.tab_name}</AccordionTrigger>
                <AccordionContent>
                  <TableColumnsAccordion columns={table.tab_columns} />
                </AccordionContent>
              </AccordionItem>
            ))}
          </Accordion>
        </AccordionContent>
      </AccordionItem>
      <AccordionItem value="indexes" className="border-b-0">
        <AccordionTrigger className="text-sm font-semibold">Índices</AccordionTrigger>
        <AccordionContent>
          <TableIndexesAccordion indexes={tables.flatMap((t) => t.tab_indexes)} />
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}

function TablesList({ dbName, schemaName }: { dbName: string; schemaName: string }) {
  const [tables, setTables] = React.useState<Table[]>([]);
  const [loading, setLoading] = React.useState(false);
  const setTablesStore = useDbStore((state) => state.setTables);
  const selectedSchema = useDbStore((state) => state.selectedSchema);

  React.useEffect(() => {
    if (!dbName || !schemaName) return;
    setLoading(true);
    fetch(`http://127.0.0.1:8000/${dbName}/${schemaName}/tables`)
      .then((res) => {
        if (!res.ok) throw new Error("Error al obtener las tablas");
        return res.json();
      })
      .then((data) => {
        setTables(data);
        setTablesStore(data.map((t: Table) => t.tab_name));
        setLoading(false);
      })
      .catch(() => {
        setTables([]);
        setTablesStore([]);
        setLoading(false);
      });
  }, [dbName, schemaName, setTablesStore]);

  if (!schemaName || selectedSchema !== schemaName) return null;

  return (
    <div className="mt-2">
      {loading ? (
        <div className="pl-4 ml-1 text-xs text-muted-foreground">Cargando tablas...</div>
      ) : tables.length === 0 ? (
        <div className="pl-4 ml-1 text-xs text-muted-foreground">No hay tablas</div>
      ) : (
        <TablesAndIndexes tables={tables} />
      )}
    </div>
  );
}

export function NavContain() {
  const selectedDb = useDbStore((state) => state.selectedDb);
  const setSelectedSchema = useDbStore((state) => state.setSelectedSchema);
  const selectedSchema = useDbStore((state) => state.selectedSchema);
  const [schemas, setSchemas] = React.useState<Schema[]>([]);
  const [loading, setLoading] = React.useState(false);

  React.useEffect(() => {
    if (!selectedDb) {
      setSchemas([]);
      return;
    }
    setLoading(true);
    fetch(`http://127.0.0.1:8000/${selectedDb}/schemas`)
      .then((res) => {
        if (!res.ok) throw new Error("Error al obtener los schemas");
        return res.json();
      })
      .then((data) => {
        setSchemas(data);
        setLoading(false);
      })
      .catch(() => {
        setSchemas([]);
        setLoading(false);
      });
  }, [selectedDb]);

  return (
    <Accordion type="single" collapsible className="w-full" value={selectedSchema || undefined}>
      {loading ? (
        <div className="p-4 text-xs text-muted-foreground">Cargando schemas...</div>
      ) : schemas.length === 0 ? (
        <div className="p-4 text-xs text-muted-foreground">No hay schemas</div>
      ) : (
        schemas.map((schema) => (
          <AccordionItem value={schema.sch_name} key={schema.sch_id} className="border-b-0">
            <AccordionTrigger
              className={buttonVariants({
                variant: "ghost",
                size: "sm",
                className: "justify-between hover:no-underline"
              })}
              onClick={() => setSelectedSchema(schema.sch_name)}
            >
              <div className="flex items-center gap-x-2">
                <Layers className="w-5 h-5 text-muted-foreground" />
                <span className="text-sm leading-none">{schema.sch_name}</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="relative pb-2">
              {selectedDb && (
                <TablesList dbName={selectedDb} schemaName={schema.sch_name} />
              )}
            </AccordionContent>
          </AccordionItem>
        ))
      )}
    </Accordion>
  );
}