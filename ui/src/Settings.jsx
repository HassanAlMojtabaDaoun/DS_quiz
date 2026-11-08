import { useEffect, useState } from "react";
import { createKey, listKeys, revokeKey, searchIndexStatus, settings, adminAction } from "./api.js";
import { fmtDate } from "./util.js";
import { useToast } from "./App.jsx";

export default function Settings({ me }) {
  const toast = useToast();
  const [base, setBase] = useState(settings.base);
  const [keys, setKeys] = useState([]);
  const [newKey, setNewKey] = useState(null);
  const [form, setForm] = useState({ name: "", role: "readwrite", rate_limit_rps: "" });
  const [es, setEs] = useState(null);
  const isAdmin = me.role === "admin";

  const load = () => {
    if (isAdmin) listKeys().then((r) => setKeys(r.keys)).catch((e) => toast(e.message, "error"));
    searchIndexStatus().then(setEs).catch(() => {});
  };
  useEffect(load, []); // eslint-disable-line react-hooks/exhaustive-deps

  const create = async (e) => {
    e.preventDefault();
    try {
      const k = await createKey({ name: form.name, role: form.role,
        rate_limit_rps: form.rate_limit_rps ? Number(form.rate_limit_rps) : null });
      setNewKey(k); setForm({ name: "", role: "readwrite", rate_limit_rps: "" }); load();
    } catch (err) { toast(err.message, "error"); }
  };

  return (
    <div className="page">
      <div className="page-head"><h1>Settings</h1></div>
      <section className="card">
        <h2>Connection</h2>
        <div className="inline-form">
          <label>Gateway URL<input value={base} onChange={(e) => setBase(e.target.value)} /></label>
          <button onClick={() => { settings.base = base; toast("Saved — reload the page", "success"); }}>Save</button>
        </div>
        <p className="muted small">Key: <b>{me.name}</b> · role <b>{me.role}</b> ·
          rate limit {me.rate_limit_rps ? `${me.rate_limit_rps}/s` : "gateway default"}</p>
      </section>

      <section className="card">
        <h2>Search index (Elasticsearch)</h2>
        {es ? (
          <p className="small">{es.enabled ? <>
            {es.ready ? "Ready" : "Starting"} · index <code>{es.index}</code> · {es.documents_indexed} documents
            indexed · {es.pending_changes} pending changes
            {es.last_error && <span className="error"> · {es.last_error}</span>}</> : "Not configured (ES_URL)."}</p>
        ) : <p className="muted small">…</p>}
        {isAdmin && es?.enabled && <button onClick={() => adminAction("reindex")
          .then((r) => { toast(`Reindexed ${r.reindexed} objects`, "success"); load(); })
          .catch((e) => toast(e.message, "error"))}>Rebuild index from PostgreSQL</button>}
      </section>

      {isAdmin && (
        <section className="card">
          <h2>API keys</h2>
          <form className="inline-form" onSubmit={create}>
            <label>Name<input required value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} /></label>
            <label>Role<select value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}>
              <option>readonly</option><option>readwrite</option><option>admin</option></select></label>
            <label>Rate limit (req/s)<input type="number" step="any" placeholder="default"
              value={form.rate_limit_rps} onChange={(e) => setForm({ ...form, rate_limit_rps: e.target.value })} /></label>
            <button className="primary" type="submit">Create key</button>
          </form>
          {newKey && (
            <div className="new-key">
              New key for <b>{newKey.name}</b> (shown only once): <code>{newKey.api_key}</code>
              <button className="link" onClick={() => navigator.clipboard?.writeText(newKey.api_key)}>copy</button>
            </div>
          )}
          <table className="table">
            <thead><tr><th>Name</th><th>Role</th><th>Rate limit</th><th>Created</th><th>Last used</th><th /></tr></thead>
            <tbody>{keys.map((k) => (
              <tr key={k.key_id} className={k.revoked ? "revoked" : ""}>
                <td>{k.name}</td><td><span className={`role role-${k.role}`}>{k.role}</span></td>
                <td>{k.rate_limit_rps ? `${k.rate_limit_rps}/s` : "default"}</td>
                <td className="small">{fmtDate(k.created_at)}</td>
                <td className="small">{fmtDate(k.last_used_at)}</td>
                <td>{k.revoked ? <span className="muted">revoked</span> :
                  <button className="danger" onClick={() => revokeKey(k.key_id).then(load)
                    .catch((e) => toast(e.message, "error"))}>Revoke</button>}</td>
              </tr>))}</tbody>
          </table>
        </section>
      )}
    </div>
  );
}
