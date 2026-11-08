import { useEffect, useState } from "react";
import { clusterStatus, deleteObject, download, objectMetadata, putTags } from "./api.js";
import { fmtBytes, fmtDate, nodeColor, parseTagString, tagString } from "./util.js";
import { useToast } from "./App.jsx";

function NodePill({ node, failed, label }) {
  return (
    <span className={`pill ${failed ? "pill-failed" : ""}`} style={{ borderColor: nodeColor(node) }}
      title={failed ? `${node} is failed` : node}>
      {label !== undefined && <b>{label}</b>}
      <span className="dot" style={{ background: nodeColor(node) }} />{node}
    </span>
  );
}

// Where every chunk (replication) or shard (erasure coding) is stored.
function ChunkMap({ chunks, failed }) {
  return (
    <table className="table chunk-map">
      <thead><tr><th>#</th><th>Chunk (SHA-256)</th><th>Size</th><th>Placement</th></tr></thead>
      <tbody>
        {chunks.map((c, i) => (
          <tr key={i}>
            <td>{i}</td>
            <td><code title={c.sha256}>{c.sha256.slice(0, 12)}…</code></td>
            <td>{fmtBytes(c.size)}</td>
            <td>
              {c.scheme === "ec" ? (
                <div className="shards">
                  {[...c.shards].sort((a, b) => a.index - b.index).map((s) => (
                    <span key={s.index} className={s.index < c.ec_k ? "shard data" : "shard parity"}>
                      <NodePill node={s.node} failed={failed.includes(s.node)}
                        label={s.index < c.ec_k ? `d${s.index}` : `p${s.index - c.ec_k}`} />
                    </span>
                  ))}
                </div>
              ) : (
                <div className="shards">{c.nodes.map((n) => <NodePill key={n} node={n} failed={failed.includes(n)} />)}</div>
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default function ObjectDetails({ bucket, objectKey, me, onClose, onChanged }) {
  const toast = useToast();
  const [meta, setMeta] = useState(null);
  const [failed, setFailed] = useState([]);
  const [tags, setTags] = useState("");
  const [version, setVersion] = useState(undefined);

  useEffect(() => {
    let alive = true;
    objectMetadata(bucket, objectKey, version).then((m) => {
      if (alive) { setMeta(m); setTags(tagString(m.tags)); }
    }).catch((e) => toast(e.message, "error"));
    clusterStatus().then((s) => alive && setFailed(s.failed_nodes)).catch(() => {});
    return () => { alive = false; };
  }, [bucket, objectKey, version, toast]);

  const saveTags = async () => {
    try { await putTags(bucket, objectKey, parseTagString(tags)); toast("Tags saved", "success"); onChanged(); }
    catch (e) { toast(e.message, "error"); }
  };

  const removeVersion = async (v) => {
    if (!confirm(`Delete version ${v}?`)) return;
    try {
      await deleteObject(bucket, objectKey, v); toast(`Version ${v} deleted`, "success");
      onChanged(); setVersion(undefined); if ((meta?.versions?.length || 0) <= 1) onClose();
      else setMeta(await objectMetadata(bucket, objectKey));
    } catch (e) { toast(e.message, "error"); }
  };

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <h2>{objectKey}</h2>
          <button className="link" onClick={onClose}>✕</button>
        </div>
        {!meta ? <p className="muted">Loading…</p> : (
          <>
            <div className="meta-grid">
              <div><span>Bucket</span><b>{meta.bucket}</b></div>
              <div><span>Version</span><b>{meta.version}</b></div>
              <div><span>Size</span><b>{fmtBytes(meta.size)}</b></div>
              <div><span>Content type</span><b>{meta.content_type || "—"}</b></div>
              <div><span>Created</span><b>{fmtDate(meta.created_at)}</b></div>
              <div><span>Chunking</span><b>{meta.chunking}</b></div>
              <div><span>Redundancy</span><b>{meta.redundancy.redundancy === "ec"
                ? `Erasure coding ${meta.redundancy.k}+${meta.redundancy.m}` : `${meta.redundancy.replicas} replicas`}</b></div>
              <div><span>Health</span><b className={meta.degraded_chunks ? "warn-text" : "ok-text"}>
                {meta.degraded_chunks ? `${meta.degraded_chunks} degraded chunks` : "fully redundant"}</b></div>
              <div className="wide"><span>ETag / SHA-256</span><code>{meta.etag}</code></div>
            </div>

            <div className="row-actions spaced">
              <button className="primary" onClick={() => download(bucket, objectKey, meta.version).catch((e) => toast(e.message, "error"))}>
                Download v{meta.version}</button>
            </div>

            <h3>Tags</h3>
            <div className="tag-edit">
              <input value={tags} onChange={(e) => setTags(e.target.value)} placeholder="key=value,key2=value2"
                disabled={me.role === "readonly"} />
              {me.role !== "readonly" && <button onClick={saveTags}>Save tags</button>}
            </div>

            <h3>Versions</h3>
            <table className="table">
              <thead><tr><th>Version</th><th>Size</th><th>Created</th><th /></tr></thead>
              <tbody>{meta.versions.map((v) => (
                <tr key={v.version} className={v.version === meta.version ? "selected" : ""}>
                  <td>{v.version}</td><td>{fmtBytes(v.size)}</td><td>{fmtDate(v.created_at)}</td>
                  <td className="row-actions">
                    <button onClick={() => setVersion(v.version)}>View</button>
                    <button onClick={() => download(bucket, objectKey, v.version)}>⬇</button>
                    {me.role !== "readonly" && <button className="danger" onClick={() => removeVersion(v.version)}>✕</button>}
                  </td>
                </tr>))}</tbody>
            </table>

            <h3>Chunks and placement <span className="muted small">({meta.chunk_count} chunks)</span></h3>
            {meta.redundancy.redundancy === "ec" && (
              <p className="muted small">d = data shard, p = parity shard. Any {meta.redundancy.k} of the
                {" "}{meta.redundancy.k + meta.redundancy.m} shards rebuild the chunk.</p>
            )}
            <ChunkMap chunks={meta.chunks} failed={failed} />
          </>
        )}
      </div>
    </div>
  );
}
