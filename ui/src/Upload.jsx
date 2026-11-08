import { useEffect, useRef, useState } from "react";
import { listBuckets, MULTIPART_THRESHOLD, PART_SIZE, uploadFile } from "./api.js";
import { fmtBytes, pct } from "./util.js";
import { useToast } from "./App.jsx";

export default function Upload({ me, go }) {
  const toast = useToast();
  const [buckets, setBuckets] = useState([]);
  const [bucket, setBucket] = useState("");
  const [prefix, setPrefix] = useState("");
  const [tags, setTags] = useState("");
  const [chunking, setChunking] = useState("fixed");
  const [queue, setQueue] = useState([]);
  const [drag, setDrag] = useState(false);
  const input = useRef();

  useEffect(() => {
    listBuckets().then((r) => {
      setBuckets(r.buckets);
      setBucket((b) => b || r.buckets[0]?.name || "");
    }).catch((e) => toast(e.message, "error"));
  }, [toast]);

  const update = (id, patch) => setQueue((q) => q.map((x) => (x.id === id ? { ...x, ...patch } : x)));

  const start = async (files) => {
    if (!bucket) { toast("Choose a bucket first", "error"); return; }
    const items = [...files].map((f) => ({
      id: Math.random(), file: f, key: prefix + f.name, sent: 0, state: "queued",
      mode: f.size >= MULTIPART_THRESHOLD ? `multipart (${Math.ceil(f.size / PART_SIZE)} parts)` : "single PUT",
    }));
    setQueue((q) => [...items, ...q]);
    for (const it of items) {                      // one file at a time
      update(it.id, { state: "uploading" });
      const t0 = performance.now();
      try {
        const r = await uploadFile(it.file, bucket, it.key, { tags, chunking },
          (sent) => update(it.id, { sent }));
        const secs = (performance.now() - t0) / 1000;
        update(it.id, { state: "done", sent: it.file.size, result: r,
          speed: it.file.size / Math.max(secs, 0.001) });
      } catch (e) {
        update(it.id, { state: "error", error: e.message });
        toast(`${it.file.name}: ${e.message}`, "error");
      }
    }
  };

  if (me.role === "readonly") return <div className="card">Your key is read-only: uploads are not allowed.</div>;

  return (
    <div className="page">
      <div className="page-head"><h1>Upload</h1></div>
      <section className="card upload-form">
        <label>Bucket
          <select value={bucket} onChange={(e) => setBucket(e.target.value)}>
            {buckets.map((b) => <option key={b.name} value={b.name}>
              {b.name} — {b.redundancy === "ec" ? `EC ${b.k}+${b.m}` : `${b.replicas}× replication`}</option>)}
          </select>
        </label>
        <label>Key prefix (folder)
          <input placeholder="e.g. photos/2026/" value={prefix} onChange={(e) => setPrefix(e.target.value)} />
        </label>
        <label>Tags
          <input placeholder="project=midterm,owner=me" value={tags} onChange={(e) => setTags(e.target.value)} />
        </label>
        <label>Chunking
          <select value={chunking} onChange={(e) => setChunking(e.target.value)}>
            <option value="fixed">Fixed 4 MiB</option>
            <option value="cdc">Content-defined (FastCDC)</option>
          </select>
        </label>
      </section>

      <div className={`dropzone ${drag ? "drag" : ""}`}
        onClick={() => input.current.click()}
        onDragOver={(e) => { e.preventDefault(); setDrag(true); }}
        onDragLeave={() => setDrag(false)}
        onDrop={(e) => { e.preventDefault(); setDrag(false); start(e.dataTransfer.files); }}>
        <div className="drop-icon">⇪</div>
        <div><b>Drop files here</b> or click to choose</div>
        <div className="muted small">Files ≥ {fmtBytes(MULTIPART_THRESHOLD)} are sent as a multipart upload
          ({fmtBytes(PART_SIZE)} parts, retried independently).</div>
        <input ref={input} type="file" multiple hidden onChange={(e) => { start(e.target.files); e.target.value = ""; }} />
      </div>

      {queue.length > 0 && (
        <section className="card">
          <h2>Uploads</h2>
          <table className="table">
            <thead><tr><th>File</th><th>Size</th><th>Mode</th><th>Progress</th><th>Result</th></tr></thead>
            <tbody>{queue.map((it) => (
              <tr key={it.id}>
                <td>{it.key}</td>
                <td>{fmtBytes(it.file.size)}</td>
                <td className="small">{it.mode}</td>
                <td style={{ minWidth: 180 }}>
                  <div className={`progress ${it.state}`}><div style={{ width: `${pct(it.sent, it.file.size)}%` }} /></div>
                  <div className="small muted">{it.state} {it.state === "uploading" && `${pct(it.sent, it.file.size)}%`}</div>
                </td>
                <td className="small">
                  {it.state === "done" && <>v{it.result.version} · {it.result.chunk_count} chunks
                    {it.result.deduplicated_chunks ? ` · ${it.result.deduplicated_chunks} dedup` : ""}
                    {it.file.size > 1024 * 1024 && <>{" · "}{fmtBytes(it.speed)}/s</>}</>}
                  {it.state === "error" && <span className="error">{it.error}</span>}
                </td>
              </tr>))}</tbody>
          </table>
          <button className="link" onClick={() => go("browse")}>Open in browser →</button>
        </section>
      )}
    </div>
  );
}
