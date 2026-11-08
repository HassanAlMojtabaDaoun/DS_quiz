import { createContext, useCallback, useContext, useEffect, useState } from "react";
import { settings, whoami } from "./api.js";
import Dashboard from "./Dashboard.jsx";
import Browser from "./Browser.jsx";
import Upload from "./Upload.jsx";
import Search from "./Search.jsx";
import Settings from "./Settings.jsx";

const ToastContext = createContext(() => {});
export const useToast = () => useContext(ToastContext);

const PAGES = [
  { id: "dashboard", label: "Cluster", icon: "◉" },
  { id: "browse", label: "Browse", icon: "▤" },
  { id: "upload", label: "Upload", icon: "⇪" },
  { id: "search", label: "Search", icon: "⌕" },
  { id: "settings", label: "Settings", icon: "⚙" },
];

function pageFromHash() {
  const id = window.location.hash.replace("#", "");
  return PAGES.some((p) => p.id === id) ? id : "dashboard";
}

export default function App() {
  const [page, setPage] = useState(pageFromHash);
  const [me, setMe] = useState(null);
  const [authError, setAuthError] = useState(null);
  const [toasts, setToasts] = useState([]);
  const [keyInput, setKeyInput] = useState("");

  const toast = useCallback((text, kind = "info") => {
    const id = Math.random();
    setToasts((t) => [...t, { id, text, kind }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 5000);
  }, []);

  const checkKey = useCallback(async () => {
    if (!settings.apiKey) { setMe(null); setAuthError("No API key set"); return; }
    try { setMe(await whoami()); setAuthError(null); }
    catch (e) { setMe(null); setAuthError(e.message); }
  }, []);

  useEffect(() => { checkKey(); }, [checkKey]);
  useEffect(() => {
    const onHash = () => setPage(pageFromHash());
    window.addEventListener("hashchange", onHash);
    return () => window.removeEventListener("hashchange", onHash);
  }, []);

  const go = (id) => { window.location.hash = id; setPage(id); };

  if (!me) {
    return (
      <div className="login">
        <form className="card login-card" onSubmit={(e) => {
          e.preventDefault(); settings.apiKey = keyInput.trim(); checkKey();
        }}>
          <div className="brand big">DS<span>Store</span></div>
          <p className="muted">Distributed object storage — enter an API key to continue.</p>
          <input autoFocus type="password" placeholder="API key (X-API-Key)"
            value={keyInput} onChange={(e) => setKeyInput(e.target.value)} />
          <button className="primary" type="submit">Connect</button>
          {authError && settings.apiKey && <p className="error">{authError}</p>}
          <p className="hint">Development key: <code>dev-admin-key-change-me</code></p>
        </form>
      </div>
    );
  }

  const Page = { dashboard: Dashboard, browse: Browser, upload: Upload, search: Search,
    settings: Settings }[page];

  return (
    <ToastContext.Provider value={toast}>
      <div className="layout">
        <aside className="sidebar">
          <div className="brand">DS<span>Store</span></div>
          <nav>
            {PAGES.map((p) => (
              <button key={p.id} className={page === p.id ? "active" : ""} onClick={() => go(p.id)}>
                <span className="icon">{p.icon}</span>{p.label}
              </button>
            ))}
          </nav>
          <div className="whoami">
            <div className="muted small">Signed in as</div>
            <div><b>{me.name}</b> <span className={`role role-${me.role}`}>{me.role}</span></div>
            <button className="link" onClick={() => { settings.apiKey = ""; setKeyInput(""); checkKey(); }}>
              Sign out
            </button>
          </div>
        </aside>
        <main className="content">
          <Page me={me} go={go} />
        </main>
        <div className="toasts">
          {toasts.map((t) => <div key={t.id} className={`toast ${t.kind}`}>{t.text}</div>)}
        </div>
      </div>
    </ToastContext.Provider>
  );
}
