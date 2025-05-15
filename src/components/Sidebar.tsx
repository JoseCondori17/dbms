import { DbSwitcher } from "./db-switcher";
import { NavContain } from "./nav-contain";
 
export function Sidebar() {
  return (
    <aside className="flex flex-col items-center w-64 gap-y-6 p-5 border-r">
      <DbSwitcher />
      {/* tree options */}
      <NavContain />
    </aside>
  );
}