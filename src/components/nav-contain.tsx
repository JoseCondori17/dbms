import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Layers, Plus } from "lucide-react";
import { Button, buttonVariants } from "./ui/button";

const schemas = [
  { id: 1, label: "Public", icon: Layers },
  { id: 2, label: "Crunchy", icon: Layers },
  { id: 3, label: "Ecommerce", icon: Layers },
]

export function NavContain() {
  return (
    <Accordion type="single" collapsible className="w-full">
      {
        schemas.map((schema) => (
          <AccordionItem value={schema.label} key={schema.id} className="border-b-0">
            <AccordionTrigger className={buttonVariants({
              variant: "ghost", 
              size: "sm", 
              className: "justify-between hover:no-underline"
            })}>
              <div className="flex items-center gap-x-2">
                <schema.icon className="w-5 h-5 text-muted-foreground" />
                <span className="text-sm leading-none">{schema.label}</span>
              </div>
            </AccordionTrigger>
            <AccordionContent className="relative pb-2">
              {/* <div className="border-l h-full z-10 absolute left-4"> </div>
              <ul className="border-muted flex flex-col gap-y-1">
                <li className={buttonVariants({variant: "ghost", size: "sm", className: "relative justify-start"})}>
                  <Button variant="outline" size="icon" className="h-4 w-4 rounded-sm absolute left-2 z-10 top-2">
                    <Plus className="size-3"/>
                  </Button>
                  <span className="pl-4 ml-1">user</span>
                </li>
              </ul> */}
              
            </AccordionContent>
          </AccordionItem>
        ))
      }
    </Accordion>
  );
}