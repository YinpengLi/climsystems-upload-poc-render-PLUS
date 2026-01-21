export const API_BASE = import.meta.env.VITE_API_URL;

async function jsonFetch(path, opts = {}) {
  const res = await fetch(API_BASE + path, opts);
  const ct = res.headers.get('content-type') || '';
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`HTTP ${res.status}: ${txt.slice(0, 300)}`);
  }
  if (ct.includes('application/json')) return res.json();
  return res;
}

export async function listDatasets() { return jsonFetch('/datasets'); }
export async function getDataset(id) { return jsonFetch(`/datasets/${id}`); }

export async function uploadInit(filename, sizeBytes) {
  const fd = new FormData();
  fd.append('filename', filename);
  fd.append('size_bytes', String(sizeBytes || 0));
  return jsonFetch('/upload/init', { method: 'POST', body: fd });
}
export async function uploadChunk(upload_id, dataset_id, part_number, blob, filename) {
  const fd = new FormData();
  fd.append('upload_id', upload_id);
  fd.append('dataset_id', dataset_id);
  fd.append('part_number', String(part_number));
  fd.append('chunk', blob, filename);
  return jsonFetch('/upload/chunk', { method: 'POST', body: fd });
}
export async function uploadFinalize(upload_id, dataset_id, filename) {
  const fd = new FormData();
  fd.append('upload_id', upload_id);
  fd.append('dataset_id', dataset_id);
  fd.append('filename', filename);
  return jsonFetch('/upload/finalize', { method: 'POST', body: fd });
}
export async function startIngest(dataset_id, mapping) {
  return jsonFetch(`/datasets/${dataset_id}/ingest`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(mapping),
  });
}

export async function filterOptions(dataset_id) {
  return jsonFetch(`/datasets/${dataset_id}/filter-options`);
}
export async function listAssets(dataset_id, q) {
  const qs = q ? `?q=${encodeURIComponent(q)}` : '';
  return jsonFetch(`/datasets/${dataset_id}/assets${qs}`);
}
export async function topAssets(dataset_id, params) {
  const url = new URL(API_BASE + `/datasets/${dataset_id}/portfolio/top-assets`);
  Object.entries(params || {}).forEach(([k,v])=>{
    if (v==null) return;
    if (Array.isArray(v)) v.forEach(x=>url.searchParams.append(k, x));
    else url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString());
  if(!res.ok) throw new Error(await res.text());
  return res.json();
}
export async function facts(dataset_id, params) {
  const url = new URL(API_BASE + `/datasets/${dataset_id}/facts`);
  Object.entries(params || {}).forEach(([k,v])=>{
    if (v==null) return;
    if (Array.isArray(v)) v.forEach(x=>url.searchParams.append(k, x));
    else url.searchParams.set(k, v);
  });
  const res = await fetch(url.toString());
  if(!res.ok) throw new Error(await res.text());
  return res.json();
}

export async function reportPreview(payload) {
  const res = await fetch(API_BASE + '/reports/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if(!res.ok) throw new Error(await res.text());
  return await res.blob();
}


export async function renameDataset(id, name) {
  return jsonFetch(`/datasets/${id}/rename`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ name }) })
}
export function originalDownloadUrl(id) {
  return API_BASE + `/datasets/${id}/original`;
}
export function exportCsvUrl(dataset_id, params) {
  const url = new URL(API_BASE + `/datasets/${dataset_id}/export-csv`);
  Object.entries(params || {}).forEach(([k,v])=>{
    if (v==null) return;
    if (Array.isArray(v)) v.forEach(x=>url.searchParams.append(k, x));
    else url.searchParams.set(k, v);
  });
  return url.toString();
}
export async function aiAsk(dataset_id, question) {
  return jsonFetch('/ai/ask', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ dataset_id, question }) });
}
export async function reportPreviewPortfolio(payload) {
  const res = await fetch(API_BASE + '/reports/preview-portfolio', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  if(!res.ok) throw new Error(await res.text());
  return await res.blob();
}