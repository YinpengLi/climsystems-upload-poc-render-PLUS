import React, { useEffect, useMemo, useState } from 'react'
import {
  uploadInit, uploadChunk, uploadFinalize,
  datasetDetect, datasetStatus, startIngest, ingestStep,
  cancelIngest, renameDataset, hardDeleteDataset,
  originalDownloadUrl
} from '../api.js'

const CHUNK_MB = 8

export default function DataManagement({ ctx }) {
  const { datasets, refreshDatasets, setActiveId } = ctx
  const [busy, setBusy] = useState(false)
  const [selectedId, setSelectedId] = useState(null)
  const [detected, setDetected] = useState(null)
  const [mapping, setMapping] = useState({})
  const [job, setJob] = useState(null)
  const [err, setErr] = useState(null)

  const selected = useMemo(() => datasets.find(d => d.id === selectedId) || null, [datasets, selectedId])

  useEffect(() => {
    if (!selectedId && datasets.length) setSelectedId(datasets[0].id)
  }, [datasets])

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
        await uploadChunk(upload_id, dataset_id, part, blob)
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

  async function loadSide(id) {
    try {
      const det = await datasetDetect(id)
      setDetected(det)
      setMapping(det?.guess || {})
    } catch {}
    try {
      const s = await datasetStatus(id)
      setJob(s.job)
    } catch {}
  }

  useEffect(() => {
    if (!selectedId) return
    loadSide(selectedId)
  }, [selectedId])

  // Poll & auto-step ingestion while PROCESSING
  useEffect(() => {
    if (!selectedId) return
    const t = setInterval(async () => {
      const current = datasets.find(d => d.id === selectedId)
      if (!current) return
      if (current.status === "PROCESSING") {
        try { await ingestStep(selectedId, 5000) } catch {}
        await refreshDatasets()
        try {
          const s = await datasetStatus(selectedId)
          setJob(s.job)
        } catch {}
      }
    }, 1500)
    return () => clearInterval(t)
  }, [selectedId, datasets])

  async function doStartIngest() {
    if (!selectedId) return
    setErr(null); setBusy(true)
    try {
      await startIngest(selectedId, mapping || {})
      await ingestStep(selectedId, 5000)
      await refreshDatasets()
    } catch(e) {
      setErr(String(e.message || e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div style={{ display:'grid', gridTemplateColumns:'1fr 420px', gap: 14 }}>
      <div>
        <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
          <div style={{ fontWeight: 800, marginBottom: 6 }}>Upload dataset</div>
          <input type="file" accept=".csv,.xlsx" disabled={busy} onChange={e=>e.target.files?.[0] && handleFile(e.target.files[0])}/>
          <div style={{ fontSize: 12, color:'#666', marginTop: 6 }}>
            Best practice for Render: upload CSV for large datasets. XLSX is OK only for small files.
          </div>
          {err ? <div style={{ marginTop: 10, color:'crimson', fontSize: 12, whiteSpace:'pre-wrap' }}>{err}</div> : null}
        </div>

        <div style={{ marginTop: 14, border:'1px solid #eee', borderRadius: 12, overflow:'hidden' }}>
          <div style={{ padding: 10, borderBottom:'1px solid #eee', fontWeight: 800 }}>Datasets</div>
          <div style={{ padding: 10 }}>
            {datasets.map(d => (
              <div key={d.id} onClick={()=>setSelectedId(d.id)}
                style={{ padding: 10, borderRadius: 10, border: d.id===selectedId?'2px solid #111':'1px solid #f0f0f0', marginBottom: 10, cursor:'pointer' }}>
                <div style={{ display:'flex', justifyContent:'space-between', gap: 10 }}>
                  <div><b>{d.name}</b></div>
                  <div style={{ fontSize: 12, color:'#666' }}>{d.status}</div>
                </div>
                <div style={{ fontSize: 12, color:'#666' }}>
                  Rows: {d.summary?.row_count ?? '-'} | Assets: {d.summary?.asset_count ?? '-'}
                </div>
                <div style={{ marginTop: 8, display:'flex', gap: 8, flexWrap:'wrap' }}>
                  <button onClick={(e)=>{e.stopPropagation(); setActiveId(d.id)}}>Use in map</button>
                  <a onClick={(e)=>e.stopPropagation()} href={originalDownloadUrl(d.id)} target="_blank" rel="noreferrer">Download</a>
                  <button onClick={async(e)=>{e.stopPropagation(); const n=prompt('Rename dataset', d.name); if(!n) return; await renameDataset(d.id, n); await refreshDatasets();}}>Rename</button>
                  <button onClick={async(e)=>{e.stopPropagation(); await cancelIngest(d.id); await refreshDatasets();}}>Cancel</button>
                  <button onClick={async(e)=>{e.stopPropagation(); if(!confirm('Hard delete dataset?')) return; await hardDeleteDataset(d.id); await refreshDatasets();}}>Delete</button>
                </div>
              </div>
            ))}
            {!datasets.length ? <div style={{ fontSize: 12, color:'#666' }}>No datasets yet.</div> : null}
          </div>
        </div>
      </div>

      <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
        <div style={{ fontWeight: 900, marginBottom: 6 }}>Mapping & Ingestion</div>
        {!selected ? <div style={{ fontSize:12, color:'#666' }}>Select a dataset.</div> : (
          <>
            <div style={{ fontSize: 12, color:'#666' }}>Job status:</div>
            <div style={{ padding: 10, border:'1px solid #eee', borderRadius: 10, marginBottom: 10 }}>
              <div style={{ fontSize: 12 }}><b>{job?.status || selected.status}</b> — {job?.stage || '-'}</div>
              <div style={{ fontSize: 12, color:'#666' }}>Processed rows: {job?.processed_rows ?? 0}</div>
            </div>

            <div style={{ fontSize: 12, color:'#666', marginBottom: 6 }}>Detected columns:</div>
            <div style={{ maxHeight: 120, overflow:'auto', fontSize: 12, border:'1px solid #eee', borderRadius: 8, padding: 8, marginBottom: 10 }}>
              {(detected?.columns || []).slice(0, 100).join(', ')}{(detected?.columns||[]).length>100?' …':''}
            </div>

            <div style={{ display:'grid', gap: 8 }}>
              {["lat_col","lon_col","asset_id_col","label_col","year_col","scenario_col","theme_col","indicator_col","value_col","units_col"].map(k => (
                <label key={k} style={{ fontSize: 12 }}>
                  {k.replaceAll('_',' ')}:
                  <input value={mapping?.[k] || ''} onChange={e=>setMapping({...(mapping||{}),[k]:e.target.value})}
                    style={{ width:'100%', padding: 6, marginTop: 4 }} />
                </label>
              ))}
            </div>

            <button onClick={doStartIngest} disabled={busy || selected.status==="PROCESSING"}
              style={{ marginTop: 12, width:'100%', padding: 10, borderRadius: 10, border:'1px solid #111', background:'#111', color:'#fff' }}>
              {busy ? "Working…" : (selected.status==="READY" ? "Re-ingest (Render-safe)" : "Start ingest (Render-safe)")}
            </button>

            <div style={{ fontSize: 12, color:'#666', marginTop: 8 }}>
              Render best practice: ingestion runs in small steps. Leave this page open while it finishes.
            </div>
          </>
        )}
      </div>
    </div>
  )
}
