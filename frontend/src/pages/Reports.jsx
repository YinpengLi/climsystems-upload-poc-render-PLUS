import React, { useEffect, useState } from 'react'
import Filters from '../components/Filters.jsx'
import { listAssets, reportPreview, reportPreviewPortfolio } from '../api.js'

export default function Reports({ ctx }) {
  const { activeId, filters, options, setFilters, selectedAssetId } = ctx
  const [assets, setAssets] = useState([])
  const [assetId, setAssetId] = useState('')
  const [pdfUrl, setPdfUrl] = useState(null)
  const [err, setErr] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!activeId) return
    listAssets(activeId).then(a=>{
      setAssets(a)
      if (selectedAssetId) setAssetId(selectedAssetId)
      else if (!assetId && a.length) setAssetId(a[0].asset_id)
    }).catch(e=>setErr(String(e.message||e)))
  }, [activeId])

  async function generate() {
    if (!activeId || !assetId) return
    setErr(null); setLoading(true)
    try {
      const blob = await reportPreview({ dataset_id: activeId, type: "asset", asset_id: assetId, filters })
      const url = URL.createObjectURL(blob)
      setPdfUrl(url)
    } catch(e) {
      setErr(String(e.message||e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ padding: 16, display:'grid', gridTemplateColumns:'1fr 340px', gap: 14 }}>
      <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
        <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center' }}>
          <div>
            <div style={{ fontWeight: 800, fontSize: 18 }}>Reports</div>
            <div style={{ fontSize: 12, color:'#666' }}>Generate a real PDF from uploaded data (filter snapshot included).</div>
          </div>
          <select value={assetId} onChange={e=>setAssetId(e.target.value)} style={{ padding: 8 }}>
            {assets.slice(0, 2000).map(a => <option key={a.asset_id} value={a.asset_id}>{a.asset_id}</option>)}
          </select>
        </div>

        <button onClick={generate} disabled={loading} style={{ marginTop: 10, padding: 10, borderRadius: 10, border:'1px solid #111', background:'#111', color:'#fff' }}>
          {loading ? "Generating…" : "Generate Asset Report Preview"}
        </button>
        <button onClick={async()=>{
          if(!activeId) return;
          setErr(null); setLoading(true);
          try{ const blob = await reportPreviewPortfolio({ dataset_id: activeId, filters, top_n: 20 }); const url = URL.createObjectURL(blob); setPdfUrl(url);}catch(e){ setErr(String(e.message||e)) } finally { setLoading(false) }
        }} disabled={loading} style={{ marginTop: 10, marginLeft: 10, padding: 10, borderRadius: 10, border:'1px solid #111', background:'#fff', color:'#111' }}>
          {loading ? "Generating…" : "Generate Portfolio Report"}
        </button>
        {err ? <div style={{ marginTop: 10, color:'crimson', fontSize: 12, whiteSpace:'pre-wrap' }}>{err}</div> : null}

        <div style={{ marginTop: 12, border:'1px solid #eee', borderRadius: 12, overflow:'hidden', height: 700 }}>
          {pdfUrl ? (
            <iframe title="Report preview" src={pdfUrl} style={{ width:'100%', height:'100%', border:0 }} />
          ) : (
            <div style={{ padding: 16, fontSize: 12, color:'#666' }}>Generate a preview to see the PDF here.</div>
          )}
        </div>
      </div>

      <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
        <div style={{ fontWeight: 800, marginBottom: 8 }}>Global Filters</div>
        <Filters options={options} filters={filters} setFilters={setFilters} />
        <div style={{ fontSize: 12, color:'#666' }}>You can tune year/scenario/theme/indicator then regenerate.</div>
      </div>
    </div>
  )
}
