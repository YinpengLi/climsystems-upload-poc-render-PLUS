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

export async function uploadInit(filename, sizeBytes) {
  const fd = new FormData();
  fd.append('filename', filename);
  fd.append('size_bytes', String(sizeBytes || 0));
  return jsonFetch('/upload/init', { method:'POST', body: fd });
}

export async function uploadChunk(upload_id, dataset_id, part_number, blob) {
  const fd = new FormData();
  fd.append('upload_id', upload_id);
  fd.append('dataset_id', dataset_id);
  fd.append('part_number', String(part_number));
  fd.append('chunk', blob, 'chunk.bin');
  return jsonFetch('/upload/chunk', { method:'POST', body: fd });
}

export async function uploadFinalize(upload_id, dataset_id, filename) {
  const fd = new FormData();
  fd.append('upload_id', upload_id);
  fd.append('dataset_id', dataset_id);
  fd.append('filename', filename);
  return jsonFetch('/upload/finalize', { method:'POST', body: fd });
}

export async function startIngest(dataset_id, mapping) {
  return jsonFetch(`/datasets/${dataset_id}/ingest`, {
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify(mapping || {})
  });
}

export async function ingestStep(dataset_id, chunk_rows=5000) {
  return jsonFetch(`/datasets/${dataset_id}/ingest-step?chunk_rows=${chunk_rows}`, { method:'POST' });
}

export async function datasetStatus(dataset_id) { return jsonFetch(`/datasets/${dataset_id}/status`); }
export async function datasetDetect(dataset_id) { return jsonFetch(`/datasets/${dataset_id}/detect`); }
export async function cancelIngest(dataset_id) { return jsonFetch(`/datasets/${dataset_id}/cancel`, { method:'POST' }); }
export async function renameDataset(dataset_id, name) { return jsonFetch(`/datasets/${dataset_id}/rename?name=${encodeURIComponent(name)}`, { method:'POST' }); }
export async function hardDeleteDataset(dataset_id) { return jsonFetch(`/datasets/${dataset_id}/hard-delete`, { method:'DELETE' }); }
export function originalDownloadUrl(dataset_id) { return API_BASE + `/datasets/${dataset_id}/original`; }
