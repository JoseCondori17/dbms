"use client"
import * as React from "react"
import { Check, ChevronsUpDown } from "lucide-react"
 
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
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

const databases = [
  {
    value: "next.js",
    label: "Next.js",
  },
  {
    value: "sveltekit",
    label: "SvelteKit",
  },
  {
    value: "nuxt.js",
    label: "Nuxt.js",
  },
  {
    value: "remix",
    label: "Remix",
  },
  {
    value: "astro",
    label: "Astro",
  },
]

export function DbSwitcher() {
  const [open, setOpen] = React.useState(false)
  const [value, setValue] = React.useState("")

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
        {value
            ? databases.find((framework) => framework.value === value)?.label
            : "Select database..."}
        <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-full p-0">
        <Command>
        <CommandInput placeholder="Search database..." />
          <CommandList>
            <CommandEmpty>No database found.</CommandEmpty>
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