import { NavLink, useNavigate } from "react-router-dom";
import { useAuth } from "../hooks/useAuth";

const NAV_ITEMS = [
  { to: "/alerts", label: "Alerts" },
  { to: "/events", label: "Events" },
  { to: "/audit", label: "Data Audit" },
  { to: "/config", label: "Client Config" },
  { to: "/tasks", label: "Tasks" },
];

export default function Layout({ children }: { children: React.ReactNode }) {
  const { user, logout } = useAuth();
  const navigate = useNavigate();

  const handleLogout = async () => {
    await logout();
    navigate("/login");
  };

  return (
    <div className="min-h-screen flex flex-col">
      <header className="bg-header text-white px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-8">
          <h1 className="text-lg font-bold tracking-wide">DE Platform</h1>
          <nav className="flex gap-1">
            {NAV_ITEMS.map(({ to, label }) => (
              <NavLink
                key={to}
                to={to}
                className={({ isActive }) =>
                  `px-3 py-1.5 rounded text-sm transition-colors ${
                    isActive
                      ? "bg-primary text-white"
                      : "text-gray-300 hover:text-white hover:bg-white/10"
                  }`
                }
              >
                {label}
              </NavLink>
            ))}
          </nav>
        </div>
        <div className="flex items-center gap-4 text-sm">
          {user && (
            <span className="text-gray-400">
              {user.tenant_id} / {user.role}
            </span>
          )}
          <button
            onClick={handleLogout}
            className="text-gray-300 hover:text-white transition-colors"
          >
            Logout
          </button>
        </div>
      </header>
      <main className="flex-1 p-6">{children}</main>
    </div>
  );
}
