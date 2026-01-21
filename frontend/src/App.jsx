import React, { useEffect, useMemo, useState } from 'react'
import { NavLink, Route, Routes } from 'react-router-dom'
import DataManagement from './pages/DataManagement.jsx'
import Overview from './pages/Overview.jsx'
import AssetDetail from './pages/AssetDetail.jsx'
import Reports from './pages/Reports.jsx'
import { listDatasets, getDataset, filterOptions } from './api.js'

export default function App() {
  const [datasets, setDatasets] = useState([])
  const [activeId, setActiveId] = useState(null)
  const [activeDataset, setActiveDataset] = useState(null)
  const [filters, setFilters] = useState({ years: [], scenarios: [], themes: [], indicators: [], assets: [] })
  const [selectedAssetId, setSelectedAssetId] = useState(null)
  const [options, setOptions] = useState({ years: [], scenarios: [], themes: [], indicators: [] })

  async function refreshDatasets() {
    const ds = await listDatasets()
    setDatasets(ds)
    if (!activeId && ds.length) setActiveId(ds[0].id)
  }

  useEffect(() => { refreshDatasets().catch(console.error) }, [])
  useEffect(() => {
    if (!activeId) return
    getDataset(activeId).then(setActiveDataset).catch(console.error)
    filterOptions(activeId).then(setOptions).catch(()=>setOptions({years:[],scenarios:[],themes:[],indicators:[]}))
  }, [activeId])

  const ctx = useMemo(() => ({
    datasets, refreshDatasets,
    activeId, setActiveId,
    activeDataset,
    filters, setFilters,
    options,
    selectedAssetId, setSelectedAssetId
  }), [datasets, activeId, activeDataset, filters, options, selectedAssetId])

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '220px 1fr', minHeight: '100vh', fontFamily: 'system-ui, Arial' }}>
      <aside style={{ borderRight: '1px solid #eee', padding: 14 }}>
        <div style={{ fontWeight: 800, marginBottom: 10 }}>ClimSystems POC</div>

        <div style={{ fontSize: 12, color: '#666', marginBottom: 8 }}>Dataset</div>
        <select value={activeId || ''} onChange={e=>setActiveId(e.target.value)} style={{ width:'100%', padding: 8 }}>
          {datasets.map(d => <option key={d.id} value={d.id}>{d.name} ({d.status})</option>)}
        </select>

        <nav style={{ marginTop: 16, display: 'grid', gap: 6 }}>
          <NavLink to="/" end style={({isActive})=>({ padding: 8, borderRadius: 8, textDecoration:'none', color:'#111', background: isActive ? '#f0f0f0':'transparent'})}>Data Management</NavLink>
          <NavLink to="/overview" style={({isActive})=>({ padding: 8, borderRadius: 8, textDecoration:'none', color:'#111', background: isActive ? '#f0f0f0':'transparent'})}>Overview</NavLink>
          <NavLink to="/asset" style={({isActive})=>({ padding: 8, borderRadius: 8, textDecoration:'none', color:'#111', background: isActive ? '#f0f0f0':'transparent'})}>Asset Detail</NavLink>
          <NavLink to="/reports" style={({isActive})=>({ padding: 8, borderRadius: 8, textDecoration:'none', color:'#111', background: isActive ? '#f0f0f0':'transparent'})}>Reports</NavLink>
        </nav>
      </aside>

      <main style={{ display:'grid', gridTemplateRows:'56px 1fr' }}>
        <header style={{ borderBottom:'1px solid #eee', padding:'12px 16px', display:'flex', alignItems:'center', justifyContent:'space-between' }}>
          <div style={{ fontWeight: 700 }}>
            {activeDataset ? `${activeDataset.name}` : 'No dataset selected'}
            {activeDataset?.status ? <span style={{ marginLeft: 10, fontSize: 12, color:'#666' }}>{activeDataset.status}</span> : null}
          </div>
          <div style={{ fontSize: 12, color:'#666' }}>
            Theme semantics: <b>Score</b>=hazard score, <b>Data</b>=raw, <b>Change</b>=delta (all visible)
          </div>
        </header>

        <Routes>
          <Route path="/" element={<DataManagement ctx={ctx} />} />
          <Route path="/overview" element={<Overview ctx={ctx} />} />
          <Route path="/asset" element={<AssetDetail ctx={ctx} />} />
          <Route path="/reports" element={<Reports ctx={ctx} />} />
        </Routes>
      </main>
    </div>
  )
}
