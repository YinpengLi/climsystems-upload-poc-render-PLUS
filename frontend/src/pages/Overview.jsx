import React, { useEffect, useState } from 'react'
import MapView from '../components/MapView.jsx'
import Assistant from '../components/Assistant.jsx'
import Filters from '../components/Filters.jsx'
import { listAssets, topAssets, exportCsvUrl } from '../api.js'

export default function Overview({ ctx }) {
  const { activeId, filters, setFilters, options, setSelectedAssetId } = ctx
  const [assets, setAssets] = useState([])
  const [top, setTop] = useState([])
  const [err, setErr] = useState(null)

  useEffect(() => {
    if (!activeId) return
    setErr(null)
    listAssets(activeId).then(setAssets).catch(e=>setErr(String(e.message||e)))
  }, [activeId])

  useEffect(() => {
    if (!activeId) return
    const params = { years: filters.years, scenarios: filters.scenarios, themes: filters.themes, indicators: filters.indicators, top_n: 20 }
    topAssets(activeId, params).then(setTop).catch(e=>setErr(String(e.message||e)))
  }, [activeId, filters])

  return (
    <div style={{ padding: 16, display:'grid', gridTemplateColumns:'1fr 340px', gap: 14 }}>
      <div style={{ display:'grid', gap: 12 }}>
        <MapView assets={assets} height={520} onSelectAsset={(id)=>setSelectedAssetId(id)} />
        <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
          <div style={{ fontWeight: 800, marginBottom: 8 }}>Portfolio Top Assets (MAX(value))</div>
          {err ? <div style={{ color:'crimson', fontSize: 12 }}>{err}</div> : null}
          <div style={{ maxHeight: 280, overflow:'auto' }}>
            <table style={{ width:'100%', fontSize: 12, borderCollapse:'collapse' }}>
              <thead><tr style={{ textAlign:'left' }}>
                <th style={{ borderBottom:'1px solid #eee', padding: 6 }}>Asset</th>
                <th style={{ borderBottom:'1px solid #eee', padding: 6 }}>MAX(value)</th>
              </tr></thead>
              <tbody>
                {top.map(r => (
                  <tr key={r.asset_id}>
                    <td style={{ borderBottom:'1px solid #f3f3f3', padding: 6 }}>{r.asset_id}</td>
                    <td style={{ borderBottom:'1px solid #f3f3f3', padding: 6 }}>{r.score ?? ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div style={{ fontSize: 12, color:'#666', marginTop: 6 }}>
            “Theme=Change” is treated as a normal theme and remains visible and exportable.
          </div>
        </div>
      </div>

      <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12, display:'grid', gap: 12 }}>
        <div style={{ fontWeight: 800, marginBottom: 8 }}>Global Filters</div>
        <a href={exportCsvUrl(activeId, { years:filters.years, scenarios:filters.scenarios, themes:filters.themes, indicators:filters.indicators })} target="_blank" rel="noreferrer" style={{ fontSize: 12, display:'inline-block', marginBottom: 10 }}>
          Download filtered CSV
        </a>

        <Filters options={options} filters={filters} setFilters={setFilters} disabledReason={(!options || (!options.years?.length && !options.scenarios?.length && !options.themes?.length && !options.indicators?.length)) ? 'Dataset is still processing (or has no categorical fields). Filters will activate when READY.' : null} />
        <Assistant datasetId={activeId} />
      </div>
    </div>
  )
}
