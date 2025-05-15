import { Tabs, TabsList, TabsTrigger } from "./ui/tabs";
import { cn } from "@/lib/utils"

export function TabFile() {
  return (
    <div className="flex flex-col h-full border rounded-md overflow-hidden bg-background">
      <div className="flex items-center border-b">
        <Tabs>
          <TabsList className="flex h-10 bg-muted/50 rounded-none w-full justify-start">
            <TabsTrigger
              className={cn(
                "flex items-center gap-2 px-4 py-1.5 data-[state=active]:bg-background relative h-full",
                "border-r border-border"
              )}
            >
              query.sql
            </TabsTrigger>
            <TabsTrigger
              className={cn(
                "flex items-center gap-2 px-4 py-1.5 data-[state=active]:bg-background relative h-full",
                "border-r border-border"
              )}
            >
              query.sql
            </TabsTrigger>
          </TabsList>
        </Tabs>
      </div>
    </div>
  );
}