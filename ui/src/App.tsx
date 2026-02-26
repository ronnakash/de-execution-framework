import { Routes, Route, Navigate } from "react-router-dom";
import { useAuth } from "./hooks/useAuth";
import Layout from "./components/Layout";
import LoginPage from "./pages/LoginPage";
import AlertsPage from "./pages/AlertsPage";
import EventsPage from "./pages/EventsPage";
import DataAuditPage from "./pages/DataAuditPage";
import ClientConfigPage from "./pages/ClientConfigPage";
import TasksPage from "./pages/TasksPage";

function ProtectedRoutes() {
  return (
    <Layout>
      <Routes>
        <Route path="/alerts" element={<AlertsPage />} />
        <Route path="/events" element={<EventsPage />} />
        <Route path="/audit" element={<DataAuditPage />} />
        <Route path="/config" element={<ClientConfigPage />} />
        <Route path="/tasks" element={<TasksPage />} />
        <Route path="*" element={<Navigate to="/alerts" replace />} />
      </Routes>
    </Layout>
  );
}

export default function App() {
  const { token } = useAuth();

  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        path="/*"
        element={
          token ? <ProtectedRoutes /> : <Navigate to="/login" replace />
        }
      />
    </Routes>
  );
}
