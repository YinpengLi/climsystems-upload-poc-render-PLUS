import React, { useEffect, useMemo, useState } from 'react'
import {
  uploadInit, uploadChunk, uploadFinalize, startIngest,
  renameDataset, originalDownloadUrl,
  datasetDetect, datasetStatus, cancelIngest, retryIngest, hardDeleteDataset
} from '../api.js'

const CHUNK_MB = 8

export default function DataManagement({ ctx }) {
  const { datasets, refreshDatasets, setActiveId } = ctx
  const [busy, setBusy] = useState(false)
  const [selectedId, setSelectedId] = useState(null)
  const [detected, setDetected] = useState(null)
  const [mapping, setMapping] = useState(null)
  const [job, setJob] = useState(null)
  const [ds, setDs] = useState(null)
  const [err, setErr] = useState(null)

  const selected = useMemo(() => datasets.find(d => d.id === selectedId) || null, [datasets, selectedId])

  async function handleFile(file) {
    setErr(null); setBusy(true)
    try {
      const init = await uploadInit(file.name, file.size)
      const upload_id = init.upload_id
      const dataset_id = init.dataset_id

      const chunkSize = CHUNK_MB * 1024 * 1024
      let offset = 0, part = 0
      while (offset < file.size) {
        const blob = file.slice(offset, offset + chunkSize)
        await uploadChunk(upload_id, part, blob)
        offset += chunkSize
        part += 1
      }
      const fin = await uploadFinalize(upload_id, dataset_id, file.name)
      await refreshDatasets()
      setSelectedId(fin.dataset_id)
      setDetected(fin.detected)
      setMapping(fin.detected?.guess || {})
    } catch(e) {
      setErr(String(e.message || e))
    } finally {
      setBusy(false)
    }
  }

  async function loadDetected(id) {
    setErr(null)
    try {
      const det = await datasetDetect(id)
      setDetected(det)
      setMapping(det?.guess || {})
    } catch(e) {
      setErr(String(e.message || e))
    }
  }

  async function loadStatus(id) {
    try {
      const s = await datasetStatus(id)
      setDs(s.dataset)
      setJob(s.job)
    } catch(e) {
      // ignore transient
    }
  }

  // When a dataset is selected, load detection and status
  useEffect(() => {
    if (!selectedId) return
    loadDetected(selectedId)
    loadStatus(selectedId)
  }, [selectedId])

  // Poll status while processing
  useEffect(() => {
    if (!selectedId) return
    const interval = setInterval(async () => {
      const current = datasets.find(d => d.id === selectedId)
      if (!current) return
      if (current.status === "PROCESSING") await refreshDatasets()
      await loadStatus(selectedId)
    }, 2000)
    return () => clearInterval(interval)
  }, [selectedId, datasets])

  async function doStartIngest() {
    if (!selectedId) return
    setErr(null); setBusy(true)
    try {
      await startIngest(selectedId, mapping || {})
      await refreshDatasets()
      await loadStatus(selectedId)
    } catch(e) {
      setErr(String(e.message || e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div style={{ padding: 16, display:'grid', gridTemplateColumns:'1fr 380px', gap: 14 }}>
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
              <div
                key={d.id}
                style={{
                  padding: 10,
                  border: d.id===selectedId ? '2px solid #111' : '1px solid #f0f0f0',
                  borderRadius: 10,
                  marginBottom: 8,
                  cursor:'pointer'
                }}
                onClick={()=>setSelectedId(d.id)}
              >
                <div style={{ display:'flex', justifyContent:'space-between', gap: 10, flexWrap:'wrap' }}>
                  <div><b>{d.name}</b></div>
                  <div style={{ fontSize: 12, color:'#666' }}>{d.status}</div>
                </div>
                <div style={{ fontSize: 12, color:'#666' }}>Rows: {d.summary?.row_count ?? '-'} | Assets: {d.summary?.asset_count ?? '-'}</div>

                <div style={{ marginTop: 8, display:'flex', gap: 8, flexWrap:'wrap', alignItems:'center' }}>
                  <button onClick={(e)=>{ e.stopPropagation(); setActiveId(d.id); }} style={{ fontSize: 12 }}>Use in dashboard</button>
                  <a onClick={(e)=>e.stopPropagation()} href={originalDownloadUrl(d.id)} target="_blank" rel="noreferrer" style={{ fontSize: 12 }}>Download original</a>
                  <button onClick={async(e)=>{ e.stopPropagation(); const name = prompt('Rename dataset', d.name); if(!name) return; await renameDataset(d.id, name); await refreshDatasets(); }} style={{ fontSize: 12 }}>Rename</button>
                  <button onClick={async(e)=>{ e.stopPropagation(); await retryIngest(d.id); await refreshDatasets(); }} style={{ fontSize: 12 }}>Retry ingest</button>
                  <button onClick={async(e)=>{ e.stopPropagation(); await cancelIngest(d.id); }} style={{ fontSize: 12 }}>Cancel</button>
                  <button onClick={async(e)=>{ e.stopPropagation(); if(!confirm('Hard delete this dataset? This removes files and DB rows.')) return; await hardDeleteDataset(d.id); await refreshDatasets(); if(selectedId===d.id){ setSelectedId(null); setDetected(null); setMapping(null); setJob(null);} }} style={{ fontSize: 12, color:'crimson' }}>Delete</button>
                </div>
              </div>
            ))}
            {!datasets.length ? <div style={{ fontSize: 12, color:'#666' }}>No datasets yet.</div> : null}
          </div>
        </div>
      </div>

      <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
        <div style={{ fontWeight: 800 }}>Mapping & Ingestion</div>
        <div style={{ fontSize: 12, color:'#666', marginBottom: 10 }}>
          Select a dataset on the left, confirm columns, then start ingestion. Status updates live.
        </div>

        {!selectedId ? (
          <div style={{ fontSize: 12, color:'#666' }}>Select a dataset to continue.</div>
        ) : (
          <>
            <div style={{ fontSize: 12, color:'#111', marginBottom: 6 }}>
              <b>Selected:</b> {selected?.name} <span style={{ color:'#666' }}>({selected?.status})</span>
            </div>

            {selected?.status === "PROCESSING" ? (
              <div style={{ marginBottom: 10, padding: 10, borderRadius: 10, background:'#fff7e6', border:'1px solid #ffe4b5', fontSize: 12 }}>
                Ingestion is running. Filters will become active when status becomes READY.
                <div style={{ marginTop: 6, color:'#666' }}>
                  {job ? (
                    <>Stage: <b>{job.stage || '-'}</b> | Processed rows: <b>{job.processed_rows ?? 0}</b></>
                  ) : <>Waiting for job status…</>}
                </div>
              </div>
            ) : null}

            {detected ? (
              <>
                <div style={{ fontSize: 12, color:'#666', marginBottom: 6 }}>Detected columns (first 50):</div>
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
                        placeholder={detected.guess?.[k] || ''}
                        style={{ marginLeft: 8, padding: 6, width: '100%' }}
                      />
                    </label>
                  ))}
                </div>

                <button
                  onClick={doStartIngest}
                  disabled={busy || selected?.status === "PROCESSING"}
                  style={{ marginTop: 12, padding: 10, borderRadius: 10, border:'1px solid #111', background:'#111', color:'#fff', width:'100%' }}
                >
                  {busy ? "Working…" : (selected?.status === "READY" ? "Re-ingest with this mapping" : "Start ingestion")}
                </button>

                <div style={{ fontSize: 12, color:'#666', marginTop: 8 }}>
                  Tip: if a dataset gets stuck PROCESSING, click <b>Cancel</b> then <b>Retry ingest</b>. If it was a bad upload, use <b>Delete</b>.
                </div>
              </>
            ) : (
              <div style={{ fontSize: 12, color:'#666' }}>Loading detected columns…</div>
            )}
          </>
        )}
      </div>
    </div>
  )
}
