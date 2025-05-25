"use client"
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
  type Database = { value: string; label: string };
  const [databases, setDatabases] = React.useState<Database[]>([]);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState<string | null>(null);
  const [open, setOpen] = React.useState(false);
  const [value, setValue] = React.useState("");

  React.useEffect(() => {
    setLoading(true);
    fetch("http://localhost/databases")
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
            ? databases.find((framework) => framework.value === value)?.label
            : "Select database..."}
        <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0">
        <Command>
        <CommandInput placeholder="Search database..." />
          <CommandList>
            <CommandEmpty>{loading ? "Cargando..." : error ? `Error: ${error}` : "No database found."}</CommandEmpty>
            <CommandGroup>
            {databases.map((database) => (
                <CommandItem
                key={database.value}
                value={database.value}
                onSelect={(currentValue) => {
                    setValue(currentValue === value ? "" : currentValue)
                    setOpen(false)
                }}
                >
                <Check
                    className={cn(
                    "mr-2 h-4 w-4",
                    value === database.value ? "opacity-100" : "opacity-0"
                    )}
                />
                {database.label}
                </CommandItem>
            ))}
            </CommandGroup>
        </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}