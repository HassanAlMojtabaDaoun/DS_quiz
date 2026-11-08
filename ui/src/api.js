// Thin client for the gateway API. Every call sends the API key; errors
// are thrown as ApiError(status, detail).

const LS = {
  get(k, d) { try { return localStorage.getItem(k) ?? d; } catch { return d; } },
  set(k, v) { try { localStorage.setItem(k, v); } catch { /* private mode */ } },
};

export const settings = {
  get base() { return LS.get("ds.base", "/api").replace(/\/+$/, ""); },
  set base(v) { LS.set("ds.base", v); },
  get apiKey() { return LS.get("ds.apiKey", ""); },
  set apiKey(v) { LS.set("ds.apiKey", v); },
};

export class ApiError extends Error {
  constructor(status, detail) {
    super(typeof detail === "string" ? detail : JSON.stringify(detail));
    this.status = status;
  }
}

const enc = (key) => key.split("/").map(encodeURIComponent).join("/");

export async function api(method, path, { json, body, headers = {}, query, raw } = {}) {
  let url = settings.base + path;
  if (query) {
    const q = new URLSearchParams();
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null && v !== "") q.set(k, v);
    }
    const s = q.toString();
    if (s) url += "?" + s;
  }
  const h = { "X-API-Key": settings.apiKey, ...headers };
  if (json !== undefined) {
    h["Content-Type"] = "application/json";
    body = JSON.stringify(json);
  }
  const r = await fetch(url, { method, headers: h, body });
  if (raw) return r;
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!r.ok) throw new ApiError(r.status, data?.detail ?? data ?? r.statusText);
  return data;
}

export const listBuckets = () => api("GET", "/buckets");
export const createBucket = (b) => api("POST", "/buckets", { json: b });
export const deleteBucket = (name, force) =>
  api("DELETE", `/buckets/${encodeURIComponent(name)}`, { query: { force: force ? "true" : "" } });
export const bucketStats = (name) => api("GET", `/buckets/${encodeURIComponent(name)}/stats`);
export const listObjects = (bucket, query) =>
  api("GET", `/buckets/${encodeURIComponent(bucket)}/objects`, { query });
export const objectMetadata = (bucket, key, version) =>
  api("GET", `/buckets/${encodeURIComponent(bucket)}/metadata/${enc(key)}`, { query: { version } });
export const deleteObject = (bucket, key, version) =>
  api("DELETE", `/buckets/${encodeURIComponent(bucket)}/objects/${enc(key)}`, { query: { version } });
export const putTags = (bucket, key, tags) =>
  api("PUT", `/buckets/${encodeURIComponent(bucket)}/tags/${enc(key)}`, { json: { tags } });
export const clusterStatus = () => api("GET", "/cluster/status");
export const repairStatus = () => api("GET", "/cluster/repair");
export const storageStats = () => api("GET", "/stats");
export const whoami = () => api("GET", "/me");
export const searchSql = (query) => api("GET", "/search", { query });
export const searchText = (query) => api("GET", "/search/text", { query });
export const searchIndexStatus = () => api("GET", "/search/index");
export const adminAction = (action) => api("POST", `/admin/${action}`);
export const listKeys = () => api("GET", "/admin/api-keys");
export const createKey = (k) => api("POST", "/admin/api-keys", { json: k });
export const revokeKey = (id) => api("DELETE", `/admin/api-keys/${id}`);

// Download through a presigned URL: the browser streams the file itself,
// so even very large objects never have to fit in memory.
export async function download(bucket, key, version) {
  const { url } = await api("POST", "/presign", {
    json: { bucket, key, version, expires_in: 300, download: true },
  });
  const a = document.createElement("a");
  a.href = settings.base + url;
  a.download = key.split("/").pop();
  document.body.appendChild(a);
  a.click();
  a.remove();
}

// Single PUT with upload progress (XMLHttpRequest exposes progress events).
function putWithProgress(url, data, headers, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", url);
    for (const [k, v] of Object.entries(headers)) xhr.setRequestHeader(k, v);
    xhr.upload.onprogress = (e) => e.lengthComputable && onProgress(e.loaded);
    xhr.onload = () => {
      let body = null;
      try { body = JSON.parse(xhr.responseText); } catch { body = xhr.responseText; }
      if (xhr.status >= 200 && xhr.status < 300) resolve(body);
      else reject(new ApiError(xhr.status, body?.detail ?? xhr.statusText));
    };
    xhr.onerror = () => reject(new ApiError(0, "Network error"));
    xhr.send(data);
  });
}

export const MULTIPART_THRESHOLD = 64 * 1024 * 1024;
export const PART_SIZE = 16 * 1024 * 1024;

// Upload one file: small files in one streaming PUT, large files as a
// multipart upload (independent 16 MiB parts, each retried up to 3 times).
export async function uploadFile(file, bucket, key, { tags, chunking }, onProgress) {
  const headers = { "X-API-Key": settings.apiKey };
  if (file.type) headers["Content-Type"] = file.type;
  if (tags) headers["X-Tags"] = tags;
  if (chunking) headers["X-Chunking"] = chunking;

  if (file.size < MULTIPART_THRESHOLD) {
    const url = `${settings.base}/buckets/${encodeURIComponent(bucket)}/objects/${enc(key)}`;
    return putWithProgress(url, file, headers, (n) => onProgress(n, file.size));
  }

  const up = await api("POST", `/buckets/${encodeURIComponent(bucket)}/uploads`, {
    json: { key, tags: tags || null, chunking: chunking || "fixed", content_type: file.type || null },
  });
  const parts = Math.ceil(file.size / PART_SIZE);
  let done = 0;
  try {
    for (let n = 1; n <= parts; n++) {
      const blob = file.slice((n - 1) * PART_SIZE, Math.min(n * PART_SIZE, file.size));
      for (let attempt = 1; ; attempt++) {
        try {
          await putWithProgress(`${settings.base}/uploads/${up.upload_id}/parts/${n}`, blob,
            { "X-API-Key": settings.apiKey }, (x) => onProgress(done + x, file.size));
          break;
        } catch (e) {
          if (attempt >= 3) throw e;
        }
      }
      done += blob.size;
      onProgress(done, file.size);
    }
    return await api("POST", `/uploads/${up.upload_id}/complete`, { json: {} });
  } catch (e) {
    api("DELETE", `/uploads/${up.upload_id}`).catch(() => {});
    throw e;
  }
}
