import React, { useState } from 'react'
import { uploadInit, uploadChunk, uploadFinalize, startIngest, renameDataset, originalDownloadUrl } from '../api.js'

const CHUNK_MB = 8

export default function DataManagement({ ctx }) {
  const { datasets, refreshDatasets, setActiveId } = ctx
  const [busy, setBusy] = useState(false)
  const [detected, setDetected] = useState(null)
  const [datasetId, setDatasetId] = useState(null)
  const [mapping, setMapping] = useState(null)
  const [err, setErr] = useState(null)

  async function handleFile(file) {
    setErr(null); setBusy(true)
    try {
      const init = await uploadInit(file.name, file.size)
      const { upload_id, dataset_id } = init
      setDatasetId(dataset_id)
      const chunkSize = CHUNK_MB * 1024 * 1024
      let part = 0
      for (let start=0; start<file.size; start+=chunkSize) {
        const blob = file.slice(start, Math.min(file.size, start+chunkSize))
        await uploadChunk(upload_id, dataset_id, part, blob, file.name)
        part += 1
      }
      const fin = await uploadFinalize(upload_id, dataset_id, file.name)
      setDetected(fin.detected)
      setMapping(fin.detected.guess)
      await refreshDatasets()
      setActiveId(dataset_id)
    } catch(e) {
      setErr(String(e.message || e))
    } finally {
      setBusy(false)
    }
  }

  async function ingest() {
    setErr(null); setBusy(true)
    try {
      await startIngest(datasetId, mapping)
      await refreshDatasets()
    } catch(e) {
      setErr(String(e.message || e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div style={{ padding: 16, display:'grid', gridTemplateColumns:'1fr 340px', gap: 14 }}>
      <div>
        <div style={{ fontWeight: 800, fontSize: 18, marginBottom: 8 }}>Data Management</div>
        <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
          <div style={{ fontWeight: 700, marginBottom: 6 }}>Upload dataset</div>
          <input type="file" accept=".csv,.xlsx" disabled={busy} onChange={e=>e.target.files?.[0] && handleFile(e.target.files[0])}/>
          <div style={{ fontSize: 12, color:'#666', marginTop: 6 }}>
            Chunked upload enabled. For the biggest files, CSV is the most stable on Render Standard.
          </div>
          {err ? <div style={{ marginTop: 10, color:'crimson', fontSize: 12, whiteSpace:'pre-wrap' }}>{err}</div> : null}
        </div>

        <div style={{ marginTop: 14, border:'1px solid #eee', borderRadius: 12, overflow:'hidden' }}>
          <div style={{ padding: 10, borderBottom:'1px solid #eee', fontWeight: 700 }}>Datasets</div>
          <div style={{ padding: 10 }}>
            {datasets.map(d => (
              <div key={d.id} style={{ padding: 10, border:'1px solid #f0f0f0', borderRadius: 10, marginBottom: 8 }}>
                <div style={{ display:'flex', justifyContent:'space-between' }}>
                  <div><b>{d.name}</b></div>
                  <div style={{ fontSize: 12, color:'#666' }}>{d.status}</div>
                </div>
                <div style={{ fontSize: 12, color:'#666' }}>Rows: {d.summary?.row_count ?? '-'} | Assets: {d.summary?.asset_count ?? '-'}</div>
                <div style={{ marginTop: 8, display:'flex', gap: 8, flexWrap:'wrap' }}>
                  <a href={originalDownloadUrl(d.id)} target="_blank" rel="noreferrer" style={{ fontSize: 12 }}>Download original</a>
                  <button onClick={async()=>{ const name = prompt('Rename dataset', d.name); if(!name) return; await renameDataset(d.id, name); await refreshDatasets(); }} style={{ fontSize: 12 }}>Rename</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
        <div style={{ fontWeight: 800 }}>Mapping wizard</div>
        <div style={{ fontSize: 12, color:'#666', marginBottom: 10 }}>
          Confirm columns for lat/lon and key fields. Then ingest.
        </div>

        {detected ? (
          <>
            <div style={{ fontSize: 12, color:'#666', marginBottom: 6 }}>Detected columns:</div>
            <div style={{ maxHeight: 120, overflow:'auto', fontSize: 12, border:'1px solid #eee', borderRadius: 8, padding: 8 }}>
              {(detected.columns||[]).slice(0, 50).join(', ')}{(detected.columns||[]).length>50?' …':''}
            </div>

            <div style={{ marginTop: 10, display:'grid', gap: 8 }}>
              {["lat_col","lon_col","asset_id_col","label_col","theme_col","indicator_col","value_col","year_col","scenario_col","units_col"].map(k => (
                <label key={k} style={{ fontSize: 12 }}>
                  {k.replaceAll('_',' ')}:
                  <input
                    value={mapping?.[k] || ''}
                    onChange={e=>setMapping({ ...(mapping||{}), [k]: e.target.value })}
                    placeholder="column name"
                    style={{ width:'100%', padding: 8, border:'1px solid #eee', borderRadius: 8, marginTop: 4 }}
                  />
                </label>
              ))}
            </div>

            <button disabled={busy} onClick={ingest} style={{ marginTop: 10, width:'100%', padding: 10, borderRadius: 10, border:'1px solid #111', background:'#111', color:'#fff' }}>
              Start ingestion
            </button>
            <div style={{ fontSize: 12, color:'#666', marginTop: 8 }}>
              After ingestion completes, go to Overview to see map and portfolio analytics.
            </div>
          </>
        ) : (
          <div style={{ fontSize: 12, color:'#666' }}>
            Upload a file to enable mapping and ingestion.
          </div>
        )}
      </div>
    </div>
  )
}
