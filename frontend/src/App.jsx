import React, { useEffect, useState } from 'react'
import { listDatasets } from './api.js'
import DataManagement from './pages/DataManagement.jsx'
import MapView from './pages/MapView.jsx'

export default function App() {
  const [datasets, setDatasets] = useState([])
  const [activeId, setActiveId] = useState(null)
  const [tab, setTab] = useState('data')

  async function refresh() {
    const ds = await listDatasets()
    setDatasets(ds)
    if (!activeId && ds.length) setActiveId(ds[0].id)
  }

  useEffect(()=>{ refresh() }, [])

  const ctx = { datasets, refreshDatasets: refresh, activeId, setActiveId }

  return (
    <div style={{ fontFamily:'system-ui, -apple-system, Segoe UI, Roboto, Arial', padding: 14 }}>
      <div style={{ display:'flex', gap: 10, alignItems:'center', marginBottom: 12 }}>
        <div style={{ fontWeight: 900 }}>ClimSystems Upload POC</div>
        <button onClick={()=>setTab('data')} style={{ padding:'6px 10px', borderRadius: 10, border:'1px solid #ddd', background: tab==='data'?'#111':'#fff', color: tab==='data'?'#fff':'#111' }}>Data Management</button>
        <button onClick={()=>setTab('map')} style={{ padding:'6px 10px', borderRadius: 10, border:'1px solid #ddd', background: tab==='map'?'#111':'#fff', color: tab==='map'?'#fff':'#111' }}>Map View</button>
        <button onClick={refresh} style={{ marginLeft:'auto', padding:'6px 10px', borderRadius: 10, border:'1px solid #ddd' }}>Refresh</button>
      </div>

      {tab === 'data' ? <DataManagement ctx={ctx} /> : <MapView ctx={ctx} />}
    </div>
  )
}
