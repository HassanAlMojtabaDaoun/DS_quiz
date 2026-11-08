import { useCallback, useEffect, useMemo, useState } from "react";
import { createBucket, deleteBucket, deleteObject, download, listBuckets, listObjects } from "./api.js";
import { fmtBytes, fmtDate } from "./util.js";
import { useToast } from "./App.jsx";
import ObjectDetails from "./ObjectDetails.jsx";

export function Tags({ tags }) {
  const entries = Object.entries(tags || {});
  if (!entries.length) return <span className="muted">—</span>;
  return <span className="tags">{entries.map(([k, v]) => <span key={k} className="tag">{k}={v}</span>)}</span>;
}

function NewBucket({ onCreated }) {
  const toast = useToast();
  const [name, setName] = useState("");
  const [redundancy, setRedundancy] = useState("replication");
  const [k, setK] = useState(4);
  const [m, setM] = useState(2);
  const submit = async (e) => {
    e.preventDefault();
    try {
      await createBucket({ name, redundancy, k: Number(k), m: Number(m) });
      toast(`Bucket ${name} created`, "success");
      setName("");
      onCreated(name);
    } catch (err) { toast(err.message, "error"); }
  };
  return (
    <form className="new-bucket" onSubmit={submit}>
      <input placeholder="new-bucket-name" value={name} onChange={(e) => setName(e.target.value)} required />
      <select value={redundancy} onChange={(e) => setRedundancy(e.target.value)}>
        <option value="replication">Replication (3 copies)</option>
        <option value="ec">Erasure coding (k+m)</option>
      </select>
      {redundancy === "ec" && (
        <div className="km">
          k <input type="number" min="1" max="16" value={k} onChange={(e) => setK(e.target.value)} />
          m <input type="number" min="1" max="8" value={m} onChange={(e) => setM(e.target.value)} />
        </div>
      )}
      <button className="primary" type="submit">Create bucket</button>
    </form>
  );
}

export default function Browser({ me }) {
  const toast = useToast();
  const [buckets, setBuckets] = useState([]);
  const [bucket, setBucket] = useState(() => new URLSearchParams(location.search).get("bucket") || "");
  const [prefix, setPrefix] = useState("");
  const [objects, setObjects] = useState([]);
  const [next, setNext] = useState(null);
  const [loading, setLoading] = useState(false);
  const [detail, setDetail] = useState(null);
  const canWrite = me.role !== "readonly";

  const loadBuckets = useCallback(async () => {
    try {
      const r = await listBuckets();
      setBuckets(r.buckets);
      setBucket((b) => b || r.buckets.find((x) => x.objects)?.name || r.buckets[0]?.name || "");
    } catch (e) { toast(e.message, "error"); }
  }, [toast]);

  const loadObjects = useCallback(async (after) => {
    if (!bucket) return;
    setLoading(true);
    try {
      const r = await listObjects(bucket, { prefix, after, limit: 200 });
      setObjects((old) => (after ? [...old, ...r.objects] : r.objects));
      setNext(r.next_after);
    } catch (e) { toast(e.message, "error"); setObjects([]); }
    setLoading(false);
  }, [bucket, prefix, toast]);

  useEffect(() => { loadBuckets(); }, [loadBuckets]);
  useEffect(() => { loadObjects(); }, [loadObjects]);

  // Show "folders": the next path segment after the current prefix.
  const { folders, files } = useMemo(() => {
    const f = new Map();
    const fl = [];
    for (const o of objects) {
      const rest = o.key.slice(prefix.length);
      const i = rest.indexOf("/");
      if (i >= 0) {
        const name = rest.slice(0, i + 1);
        f.set(name, (f.get(name) || 0) + 1);
      } else fl.push(o);
    }
    return { folders: [...f.entries()], files: fl };
  }, [objects, prefix]);

  const crumbs = prefix.split("/").filter(Boolean);
  const current = buckets.find((b) => b.name === bucket);

  const remove = async (o) => {
    if (!confirm(`Delete ${o.key} (all versions)?`)) return;
    try {
      const r = await deleteObject(bucket, o.key);
      toast(`Deleted ${o.key}: ${r.blobs_removed} blobs garbage-collected`, "success");
      loadObjects(); loadBuckets();
    } catch (e) { toast(e.message, "error"); }
  };

  const removeBucket = async () => {
    if (!confirm(`Delete bucket ${bucket} and ALL its objects?`)) return;
    try { await deleteBucket(bucket, true); toast(`Bucket ${bucket} deleted`, "success"); setBucket(""); loadBuckets(); }
    catch (e) { toast(e.message, "error"); }
  };

  return (
    <div className="page browse">
      <div className="page-head"><h1>Browse</h1></div>
      <div className="browse-grid">
        <section className="card bucket-list">
          <h2>Buckets</h2>
          <ul>
            {buckets.map((b) => (
              <li key={b.name} className={b.name === bucket ? "active" : ""}
                onClick={() => { setBucket(b.name); setPrefix(""); }}>
                <div className="bucket-name">{b.name}</div>
                <div className="small muted">
                  <span className={`badge ${b.redundancy}`}>{b.redundancy === "ec" ? `EC ${b.k}+${b.m}` : `${b.replicas}× replica`}</span>
                  {" "}{b.objects} obj · {fmtBytes(b.logical_bytes)}
                </div>
              </li>
            ))}
          </ul>
          {canWrite && <NewBucket onCreated={(n) => { loadBuckets(); setBucket(n); }} />}
        </section>

        <section className="card">
          {!bucket ? <p className="muted">Create or select a bucket.</p> : (
            <>
              <div className="obj-head">
                <div className="crumbs">
                  <button className="link" onClick={() => setPrefix("")}>{bucket}</button>
                  {crumbs.map((c, i) => (
                    <span key={i}> / <button className="link"
                      onClick={() => setPrefix(crumbs.slice(0, i + 1).join("/") + "/")}>{c}</button></span>
                  ))}
                </div>
                {current && <span className="muted small">overhead ×{current.storage_overhead} ·
                  tolerates {current.redundancy === "ec" ? `${current.tolerates_lost_shards} lost shards` : `${current.tolerates_lost_copies} lost copies`}</span>}
                <div className="actions">
                  <input className="filter" placeholder="Filter by prefix…" value={prefix}
                    onChange={(e) => setPrefix(e.target.value)} />
                  <button onClick={() => loadObjects()}>Refresh</button>
                  {canWrite && bucket !== "default" && <button className="danger" onClick={removeBucket}>Delete bucket</button>}
                </div>
              </div>
              <table className="table objects">
                <thead><tr><th>Key</th><th>Size</th><th>Ver.</th><th>Tags</th><th>Created</th><th /></tr></thead>
                <tbody>
                  {folders.map(([name, count]) => (
                    <tr key={name} className="folder" onClick={() => setPrefix(prefix + name)}>
                      <td>📁 {name}</td><td className="muted">{count} obj</td><td /><td /><td /><td />
                    </tr>
                  ))}
                  {files.map((o) => (
                    <tr key={o.key}>
                      <td><button className="link key" onClick={() => setDetail(o)}>{o.key.slice(prefix.length)}</button></td>
                      <td>{fmtBytes(o.size)}</td>
                      <td>{o.version}</td>
                      <td><Tags tags={o.tags} /></td>
                      <td className="small">{fmtDate(o.created_at)}</td>
                      <td className="row-actions">
                        <button title="Download" onClick={() => download(bucket, o.key).catch((e) => toast(e.message, "error"))}>⬇</button>
                        <button title="Details" onClick={() => setDetail(o)}>ⓘ</button>
                        {canWrite && <button title="Delete" className="danger" onClick={() => remove(o)}>✕</button>}
                      </td>
                    </tr>
                  ))}
                  {!loading && !folders.length && !files.length && (
                    <tr><td colSpan={6} className="muted">No objects{prefix ? ` under "${prefix}"` : ""}.</td></tr>
                  )}
                </tbody>
              </table>
              {next && <button onClick={() => loadObjects(next)} disabled={loading}>Load more</button>}
              {loading && <p className="muted small">Loading…</p>}
            </>
          )}
        </section>
      </div>
      {detail && <ObjectDetails bucket={bucket} objectKey={detail.key} me={me}
        onClose={() => setDetail(null)} onChanged={() => loadObjects()} />}
    </div>
  );
}
