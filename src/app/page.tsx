import { Sidebar } from "@/components/Sidebar";
import { CodeEditor } from "@/components/code-editor";
import { TabFile } from "@/components/tab-file";

export default function Home() {
  return (
    <div className="flex min-h-screen w-full">
      <Sidebar />
      <main className="flex-1 w-full">
        {/* <TabFile/> */}
        
        <CodeEditor/>
      </main>
    </div>    
  );
}
