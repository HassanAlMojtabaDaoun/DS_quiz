import { useState } from "react";
import { adminAction, clusterStatus, storageStats } from "./api.js";
import { fmtBytes, nodeColor, pct, usePoll } from "./util.js";
import { useToast } from "./App.jsx";

function Tile({ label, value, sub, tone }) {
  return (
    <div className={`tile ${tone || ""}`}>
      <div className="tile-label">{label}</div>
      <div className="tile-value">{value}</div>
      {sub && <div className="tile-sub">{sub}</div>}
    </div>
  );
}

function NodeCard({ node }) {
  const used = (node.total_bytes || 0) - (node.free_bytes || 0);
  const status = node.status;
  return (
    <div className={`node-card ${status}`}>
      <div className="node-head">
        <span className="dot" style={{ background: nodeColor(node.node_id) }} />
        <b>{node.node_id}</b>
        <span className={`badge ${status}`}>{status}</span>
        {!node.in_ring && <span className="badge standby">standby</span>}
      </div>
      <div className="node-row"><span>Blobs</span><b>{node.chunk_count ?? "—"}</b></div>
      <div className="node-row"><span>Stored</span><b>{fmtBytes(node.stored_bytes)}</b></div>
      <div className="node-row"><span>Ring share</span>
        <b>{node.ring_share !== undefined ? `${(node.ring_share * 100).toFixed(1)}%` : "—"}</b></div>
      <div className="node-row"><span>Last heartbeat</span>
        <b>{node.heartbeat_age_seconds !== undefined ? `${node.heartbeat_age_seconds.toFixed(0)} s ago` : "—"}</b></div>
      <div className="disk">
        <div className="disk-bar"><div style={{ width: `${pct(used, node.total_bytes)}%` }} /></div>
        <div className="small muted">{fmtBytes(node.free_bytes)} free of {fmtBytes(node.total_bytes)}</div>
      </div>
    </div>
  );
}

function DistributionChart({ counts, bytes, failed }) {
  const nodes = Object.keys({ ...counts, ...bytes }).sort();
  const maxCount = Math.max(1, ...nodes.map((n) => counts[n] || 0));
  const total = nodes.reduce((a, n) => a + (counts[n] || 0), 0);
  if (!nodes.length) return <p className="muted">No data stored yet.</p>;
  return (
    <div className="bars">
      {nodes.map((n) => (
        <div className="bar-row" key={n}>
          <div className="bar-label">
            <span className="dot" style={{ background: nodeColor(n) }} />{n}
            {failed.includes(n) && <span className="badge failed">failed</span>}
          </div>
          <div className="bar-track">
            <div className="bar-fill" style={{ width: `${(100 * (counts[n] || 0)) / maxCount}%`,
              background: nodeColor(n), opacity: failed.includes(n) ? 0.35 : 1 }} />
          </div>
          <div className="bar-value">
            {counts[n] || 0} <span className="muted">({pct(counts[n] || 0, total)}%) · {fmtBytes(bytes[n])}</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function RepairPanel({ repair }) {
  const job = repair?.current_job;
  const last = repair?.last_job;
  return (
    <div>
      <div className="repair-state">
        State: <span className={`badge ${repair?.state === "running" ? "running" : "healthy"}`}>
          {repair?.state || "idle"}</span>
      </div>
      {job && (
        <div className="progress-block">
          <div className="small">{job.reason} — {job.chunks_done} / {job.chunks_to_check} chunks</div>
          <div className="progress"><div style={{ width: `${pct(job.chunks_done, job.chunks_to_check)}%` }} /></div>
        </div>
      )}
      {last ? (
        <table className="mini">
          <thead><tr><th>Last repair</th><th>Replication</th><th>Erasure coding</th></tr></thead>
          <tbody>
            <tr><td>Reason</td><td colSpan={2}>{last.reason} ({last.duration_seconds}s)</td></tr>
            {["chunks_repaired", "copies_or_shards_rebuilt", "bytes_read", "bytes_written", "chunks_lost"].map((k) => (
              <tr key={k}>
                <td>{k.replaceAll("_", " ")}</td>
                {["replication", "ec"].map((s) => (
                  <td key={s}>{k.startsWith("bytes") ? fmtBytes(last.by_scheme[s][k]) : last.by_scheme[s][k]}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      ) : <p className="muted small">No repair has been needed yet.</p>}
    </div>
  );
}

export default function Dashboard({ me }) {
  const toast = useToast();
  const [status, error] = usePoll(clusterStatus, 3000);
  const [stats] = usePoll(storageStats, 10000);
  const [busy, setBusy] = useState(null);

  const run = async (action) => {
    setBusy(action);
    try {
      const r = await adminAction(action);
      toast(`${action}: ${JSON.stringify(r).slice(0, 140)}`, "success");
    } catch (e) { toast(`${action} failed: ${e.message}`, "error"); }
    setBusy(null);
  };

  if (error && !status) return <div className="card error">Cannot load cluster status: {error.message}</div>;
  if (!status) return <div className="muted">Loading cluster status…</div>;

  const nodes = status.nodes || [];
  const healthy = nodes.filter((n) => n.status === "healthy" && n.in_ring).length;
  const ringNodes = nodes.filter((n) => n.in_ring).length;

  return (
    <div className="page">
      <div className="page-head">
        <h1>Cluster</h1>
        <span className="muted small">auto-refresh every 3 s</span>
        {me.role === "admin" && (
          <div className="actions">
            {["scrub", "repair", "rebalance"].map((a) => (
              <button key={a} disabled={!!busy} onClick={() => run(a)}>
                {busy === a ? "Running…" : `Run ${a}`}
              </button>
            ))}
          </div>
        )}
      </div>

      <div className="tiles">
        <Tile label="Nodes healthy" value={`${healthy} / ${ringNodes}`}
          tone={healthy < ringNodes ? "warn" : "ok"}
          sub={status.failed_nodes.length ? `failed: ${status.failed_nodes.join(", ")}` : "all heartbeats on time"} />
        <Tile label="Objects" value={status.objects_total} sub={`${status.chunks_total} unique chunks`} />
        <Tile label="Degraded objects" value={status.objects_degraded}
          tone={status.objects_degraded ? "warn" : "ok"} sub={`${status.chunks_degraded} chunks below full redundancy`} />
        <Tile label="Unavailable objects" value={status.objects_unavailable}
          tone={status.objects_unavailable ? "bad" : "ok"} sub={`${status.chunks_unavailable} chunks unreadable`} />
        <Tile label="Stored (logical)" value={fmtBytes(stats?.logical_bytes)}
          sub={stats ? `${fmtBytes(stats.raw_bytes_on_disk)} on disk · dedup ×${stats.dedup_ratio ?? "—"}` : ""} />
        <Tile label="Free space" value={fmtBytes(status.total_free_bytes)} sub="on healthy nodes" />
      </div>

      <section className="card">
        <h2>Storage nodes</h2>
        <div className="node-grid">{nodes.map((n) => <NodeCard key={n.node_id} node={n} />)}</div>
      </section>

      <div className="two-col">
        <section className="card">
          <h2>Chunk distribution</h2>
          <p className="muted small">Blobs (replicas and EC shards) per node, from the chunk catalog.</p>
          <DistributionChart counts={status.blobs_per_node || {}} bytes={status.bytes_per_node || {}}
            failed={status.failed_nodes} />
        </section>
        <section className="card">
          <h2>Repair</h2>
          <RepairPanel repair={status.repair} />
        </section>
      </div>

      {status.degraded_objects_sample?.length > 0 && (
        <section className="card">
          <h2>Degraded objects (sample)</h2>
          <table className="table">
            <thead><tr><th>Bucket</th><th>Key</th><th>Version</th></tr></thead>
            <tbody>{status.degraded_objects_sample.map((o) => (
              <tr key={o.file_id}><td>{o.bucket}</td><td>{o.key}</td><td>{o.version}</td></tr>))}</tbody>
          </table>
        </section>
      )}
    </div>
  );
}
