"use client"
import { useDbStore } from "@/store/dbStore";
import { Check, ChevronsUpDown } from "lucide-react";
import * as React from "react";

import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { cn } from "@/lib/utils";

export function DbSwitcher() {
  type Database = { db_id: string; db_name: string, db_schemas: object, db_created_at: string };
  const [databases, setDatabases] = React.useState<Database[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [open, setOpen] = React.useState(false);
  const [value, setValue] = React.useState("");

  const setSelectedDb = useDbStore((state) => state.setSelectedDb);

  React.useEffect(() => {
    setLoading(true);
    fetch("http://127.0.0.1:8000/databases")
      .then((res) => {
        if (!res.ok) throw new Error("Error al obtener las bases de datos");
        return res.json();
      })
      .then((data) => {
        setDatabases(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  return (
    <Popover 
      open={open} 
      onOpenChange={setOpen}
    >
    <PopoverTrigger asChild>
        <Button
        variant="outline"
        size="lg"
        aria-expanded={open}
        className="w-full justify-between"
        >
        {loading ? (
          "Cargando..."
        ) : error ? (
          `Error: ${error}`
        ) : value
            ? databases.find((db) => db.db_name === value)?.db_name
            : "Database..."}
        <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0">
        <Command>
        <CommandInput placeholder="Buscar base de datos..." />
          <CommandList>
            <CommandEmpty>{loading ? "Cargando..." : error ? `Error: ${error}` : "No se encontró ninguna base de datos."}</CommandEmpty>
            <CommandGroup>
            {databases.map((database) => (
                <CommandItem
                key={database.db_id}
                value={database.db_name}
                onSelect={(currentValue) => {
                    setValue(currentValue === value ? "" : currentValue)
                    setSelectedDb(currentValue === value ? null : currentValue)
                    setOpen(false)
                }}
                >
                <Check
                    className={cn(
                    "mr-2 h-4 w-4",
                    value === database.db_name ? "opacity-100" : "opacity-0"
                    )}
                />
                {database.db_name}
                </CommandItem>
            ))}
            </CommandGroup>
        </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

