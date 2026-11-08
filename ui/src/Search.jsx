import { useEffect, useState } from "react";
import { download, listBuckets, searchIndexStatus, searchSql, searchText } from "./api.js";
import { fmtBytes, fmtDate } from "./util.js";
import { useToast } from "./App.jsx";
import { Tags } from "./Browser.jsx";
import ObjectDetails from "./ObjectDetails.jsx";

const MiB = 1024 * 1024;

export default function Search({ me }) {
  const toast = useToast();
  const [buckets, setBuckets] = useState([]);
  const [engine, setEngine] = useState("elasticsearch");
  const [esStatus, setEsStatus] = useState(null);
  const [f, setF] = useState({ q: "", bucket: "", prefix: "", minMb: "", maxMb: "", after: "", before: "",
    tags: "", sort: "relevance", order: "desc" });
  const [res, setRes] = useState(null);
  const [busy, setBusy] = useState(false);
  const [detail, setDetail] = useState(null);

  useEffect(() => {
    listBuckets().then((r) => setBuckets(r.buckets)).catch(() => {});
    searchIndexStatus().then((s) => {
      setEsStatus(s);
      if (!s.enabled || !s.ready) setEngine("sql");
    }).catch(() => setEngine("sql"));
  }, []);

  const set = (k) => (e) => setF({ ...f, [k]: e.target.value });

  const run = async (e, override = {}) => {
    e?.preventDefault();
    const p = { ...f, ...override };
    const common = {
      bucket: p.bucket, prefix: p.prefix,
      min_size: p.minMb ? Math.round(Number(p.minMb) * MiB) : "",
      max_size: p.maxMb ? Math.round(Number(p.maxMb) * MiB) : "",
      created_after: p.after ? new Date(p.after).toISOString() : "",
      created_before: p.before ? new Date(p.before + "T23:59:59").toISOString() : "",
      tags: p.tags, limit: 100,
    };
    setBusy(true);
    try {
      if (engine === "elasticsearch") {
        setRes(await searchText({ ...common, q: p.q, sort: p.sort, order: p.order }));
      } else {
        const sort = p.sort === "relevance" ? "key" : p.sort;
        const r = await searchSql({ ...common, prefix: p.prefix || p.q, sort, order: p.sort === "relevance" ? "asc" : p.order });
        setRes({ ...r, engine: "postgresql" });
      }
    } catch (err) {
      toast(err.message, "error");
      if (engine === "elasticsearch" && err.status === 503) setEngine("sql");
    }
    setBusy(false);
  };

  const facet = (name, value) => {
    if (name === "buckets") { setF({ ...f, bucket: value }); run(null, { bucket: value }); }
  };

  return (
    <div className="page">
      <div className="page-head">
        <h1>Search</h1>
        <div className="engine">
          <button className={engine === "elasticsearch" ? "active" : ""}
            disabled={!esStatus?.enabled} onClick={() => setEngine("elasticsearch")}
            title={esStatus?.enabled ? "" : "Elasticsearch not configured"}>Elasticsearch (full-text)</button>
          <button className={engine === "sql" ? "active" : ""} onClick={() => setEngine("sql")}>PostgreSQL (exact)</button>
        </div>
        {esStatus && <span className="muted small">
          index: {esStatus.enabled ? (esStatus.ready ? `ready · ${esStatus.pending_changes} pending` : "starting…") : "disabled"}
        </span>}
      </div>

      <form className="card search-form" onSubmit={run}>
        <input className="q" placeholder={engine === "elasticsearch"
          ? "Search keys and tags (fuzzy): e.g. beach sunset, error log, project alpha"
          : "Key prefix: e.g. logs/2026/"} value={f.q} onChange={set("q")} />
        <div className="filters">
          <label>Bucket<select value={f.bucket} onChange={set("bucket")}>
            <option value="">all</option>
            {buckets.map((b) => <option key={b.name}>{b.name}</option>)}
          </select></label>
          {engine === "elasticsearch" && <label>Prefix<input value={f.prefix} onChange={set("prefix")} placeholder="logs/" /></label>}
          <label>Min MiB<input type="number" step="any" value={f.minMb} onChange={set("minMb")} /></label>
          <label>Max MiB<input type="number" step="any" value={f.maxMb} onChange={set("maxMb")} /></label>
          <label>From<input type="date" value={f.after} onChange={set("after")} /></label>
          <label>To<input type="date" value={f.before} onChange={set("before")} /></label>
          <label>Tags<input value={f.tags} onChange={set("tags")} placeholder="k=v,k2=v2" /></label>
          <label>Sort<select value={f.sort} onChange={set("sort")}>
            <option value="relevance">relevance / key</option>
            <option value="size">size</option>
            <option value="created_at">date</option>
          </select></label>
          <label>Order<select value={f.order} onChange={set("order")}>
            <option value="desc">desc</option><option value="asc">asc</option>
          </select></label>
          <button className="primary" type="submit" disabled={busy}>{busy ? "Searching…" : "Search"}</button>
        </div>
      </form>

      {res && (
        <div className={res.facets ? "results-grid" : ""}>
          {res.facets && (
            <aside className="card facets">
              <h3>Buckets</h3>
              {res.facets.buckets.map((b) => (
                <button key={b.value} className="facet" onClick={() => facet("buckets", b.value)}>
                  {b.value}<span>{b.count}</span></button>))}
              <h3>Size</h3>
              {res.facets.sizes.map((b) => <div key={b.value} className="facet static">{b.value}<span>{b.count}</span></div>)}
              {res.facets.content_types.length > 0 && <h3>Content type</h3>}
              {res.facets.content_types.map((b) => <div key={b.value} className="facet static">{b.value}<span>{b.count}</span></div>)}
            </aside>
          )}
          <section className="card">
            <div className="muted small">{res.total} result(s) · {res.engine}{res.took_ms !== undefined ? ` · ${res.took_ms} ms` : ""}</div>
            <table className="table">
              <thead><tr><th>Bucket</th><th>Key</th><th>Size</th><th>Tags</th><th>Created</th><th /></tr></thead>
              <tbody>{res.results.map((o) => (
                <tr key={`${o.bucket}/${o.key}/${o.version}`}>
                  <td>{o.bucket}</td>
                  <td>{o.highlight?.key
                    ? <span dangerouslySetInnerHTML={{ __html: o.highlight.key[0] }} />
                    : o.key}</td>
                  <td>{fmtBytes(o.size)}</td>
                  <td><Tags tags={o.tags} /></td>
                  <td className="small">{fmtDate(o.created_at)}</td>
                  <td className="row-actions">
                    <button onClick={() => download(o.bucket, o.key).catch((e) => toast(e.message, "error"))}>⬇</button>
                    <button onClick={() => setDetail(o)}>ⓘ</button>
                  </td>
                </tr>))}</tbody>
            </table>
          </section>
        </div>
      )}
      {detail && <ObjectDetails bucket={detail.bucket} objectKey={detail.key} me={me}
        onClose={() => setDetail(null)} onChanged={() => run()} />}
    </div>
  );
}
