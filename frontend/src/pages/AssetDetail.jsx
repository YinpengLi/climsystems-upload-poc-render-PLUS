import React, { useEffect, useState } from 'react'
import Filters from '../components/Filters.jsx'
import SpiderChart from '../components/SpiderChart.jsx'
import TimeSeries from '../components/TimeSeries.jsx'
import { listAssets, facts, exportCsvUrl } from '../api.js'

export default function AssetDetail({ ctx }) {
  const { activeId, filters, setFilters, options, selectedAssetId } = ctx
  const [assetId, setAssetId] = useState('')
  const [assets, setAssets] = useState([])
  const [rows, setRows] = useState([])
  const [err, setErr] = useState(null)

  useEffect(() => {
    if (!activeId) return
    listAssets(activeId).then(a=>{
      setAssets(a)
      if (selectedAssetId) setAssetId(selectedAssetId)
      else if (!assetId && a.length) setAssetId(a[0].asset_id)
    }).catch(e=>setErr(String(e.message||e)))
  }, [activeId])

  useEffect(() => {
    if (!activeId || !assetId) return
    setErr(null)
    facts(activeId, { assets:[assetId], years:filters.years, scenarios:filters.scenarios, themes:filters.themes, indicators:filters.indicators, limit:5000, offset:0 })
      .then(r=>setRows(r.rows||[]))
      .catch(e=>setErr(String(e.message||e)))
  }, [activeId, assetId, filters])

  const current = assets.find(a=>a.asset_id===assetId)

  return (
    <div style={{ padding: 16, display:'grid', gridTemplateColumns:'1fr 340px', gap: 14 }}>
      <div style={{ display:'grid', gap: 12 }}>
        <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
          <div style={{ display:'flex', justifyContent:'space-between', alignItems:'center', gap: 10, flexWrap:'wrap' }}>
            <div>
              <div style={{ fontWeight: 800, fontSize: 18 }}>Asset Detail</div>
              <div style={{ fontSize: 12, color:'#666' }}>Select an asset (map click can be added next) — Lat/Lon always shown.</div>
            </div>
            <div style={{ display:'flex', gap: 10, alignItems:'center' }}>
            <a href={exportCsvUrl(activeId, { assets:[assetId], years:filters.years, scenarios:filters.scenarios, themes:filters.themes, indicators:filters.indicators })} target="_blank" rel="noreferrer" style={{ fontSize: 12 }}>
              Download asset CSV
            </a>
            <select value={assetId} onChange={e=>setAssetId(e.target.value)} style={{ padding: 8 }}>
              {assets.slice(0, 2000).map(a => <option key={a.asset_id} value={a.asset_id}>{a.asset_id}</option>)}
            </select>
          </div>
          </div>
          {current ? (
            <div style={{ marginTop: 8, fontSize: 12 }}>
              <b>{current.asset_id}</b> — {current.label || ''}<br/>
              Lat/Lon: {current.latitude}, {current.longitude}
            </div>
          ) : null}
          {err ? <div style={{ marginTop: 10, color:'crimson', fontSize: 12 }}>{err}</div> : null}
        </div>

        <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap: 12 }}>
          <SpiderChart rows={rows} />
          <TimeSeries rows={rows} />
        </div>

        <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
          <div style={{ fontWeight: 800, marginBottom: 8 }}>Raw rows (first 200)</div>
          <div style={{ maxHeight: 260, overflow:'auto' }}>
            <table style={{ width:'100%', fontSize: 12, borderCollapse:'collapse' }}>
              <thead><tr style={{ textAlign:'left' }}>
                {["year","scenario","theme","indicator","value"].map(h => <th key={h} style={{ borderBottom:'1px solid #eee', padding: 6 }}>{h}</th>)}
              </tr></thead>
              <tbody>
                {rows.slice(0,200).map((r, idx)=>(
                  <tr key={idx}>
                    <td style={{ borderBottom:'1px solid #f3f3f3', padding: 6 }}>{r.year ?? ''}</td>
                    <td style={{ borderBottom:'1px solid #f3f3f3', padding: 6 }}>{r.scenario ?? ''}</td>
                    <td style={{ borderBottom:'1px solid #f3f3f3', padding: 6 }}>{r.theme ?? ''}</td>
                    <td style={{ borderBottom:'1px solid #f3f3f3', padding: 6 }}>{r.indicator ?? ''}</td>
                    <td style={{ borderBottom:'1px solid #f3f3f3', padding: 6 }}>{r.value ?? ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
        <div style={{ fontWeight: 800, marginBottom: 8 }}>Global Filters</div>
        <Filters options={options} filters={filters} setFilters={setFilters} disabledReason={(!options || (!options.years?.length && !options.scenarios?.length && !options.themes?.length && !options.indicators?.length)) ? 'Dataset is still processing (or has no categorical fields). Filters will activate when READY.' : null} />
      </div>
    </div>
  )
}
