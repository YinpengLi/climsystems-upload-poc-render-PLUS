import React, { useMemo, useState } from 'react'
import { MapContainer, TileLayer, CircleMarker, Popup, GeoJSON, useMap } from 'react-leaflet'
import { useEffect } from 'react'

const BASEMAPS = {
  light: { name: 'Light', url: 'https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', attrib: '©OpenStreetMap ©Carto' },
  street: { name: 'Street', url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', attrib: '©OpenStreetMap' },
  satellite: { name: 'Satellite', url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', attrib: '©Esri' },
}

function FixMapResize() {
  const map = useMap()
  useEffect(() => {
    const t = setTimeout(() => { try { map.invalidateSize() } catch(e) {} }, 200)
    return () => clearTimeout(t)
  }, [map])
  return null
}

function FlyTo({ center, zoom }) {
  const map = useMap()
  React.useEffect(() => {
    if (!center) return
    map.flyTo(center, zoom ?? map.getZoom(), { duration: 0.8 })
  }, [center, zoom])
  return null
}

export default function MapView({ assets, height=520, onSelectAsset }) {
  const [basemap, setBasemap] = useState('light')
  const [latlon, setLatlon] = useState('')
  const [flyCenter, setFlyCenter] = useState(null)
  const [geojson, setGeojson] = useState(null)

  const center = useMemo(() => {
    if (!assets?.length) return [-36.85, 174.76]
    return [assets[0].latitude, assets[0].longitude]
  }, [assets])

  function nearestAsset(lat, lon) {
    let best = null, bestD = Infinity
    for (const a of (assets||[])) {
      const d = Math.abs(a.latitude - lat) + Math.abs(a.longitude - lon)
      if (d < bestD) { bestD = d; best = a }
    }
    return best
  }

  function goLatLon() {
    const parts = latlon.split(',').map(s=>Number(s.trim()))
    if (parts.length !== 2 || Number.isNaN(parts[0]) || Number.isNaN(parts[1])) return
    const c = [parts[0], parts[1]]
    setFlyCenter(c)
    const near = nearestAsset(parts[0], parts[1])
    if (near && onSelectAsset) onSelectAsset(near.asset_id)
  }

  async function loadGeoJSON(file) {
    const txt = await file.text()
    const gj = JSON.parse(txt)
    setGeojson(gj)
  }

  return (
    <div style={{ border:'1px solid #eee', borderRadius: 12, overflow:'hidden' }}>
      <div style={{ padding: 8, borderBottom:'1px solid #eee', display:'flex', justifyContent:'space-between', alignItems:'center', gap: 10, flexWrap:'wrap' }}>
        <div style={{ fontWeight: 700 }}>Map</div>
        <div style={{ display:'flex', alignItems:'center', gap: 10, flexWrap:'wrap' }}>
          <div style={{ fontSize: 12 }}>
            Basemap:{' '}
            <select value={basemap} onChange={e=>setBasemap(e.target.value)}>
              {Object.entries(BASEMAPS).map(([k,v])=> <option key={k} value={k}>{v.name}</option>)}
            </select>
          </div>
          <div style={{ fontSize: 12 }}>
            Lat,Lon:{' '}
            <input value={latlon} onChange={e=>setLatlon(e.target.value)} placeholder="-36.85, 174.76" style={{ padding: 6, width: 150 }} />
            <button onClick={goLatLon} style={{ marginLeft: 6, padding:'6px 10px' }}>Go</button>
          </div>
          <div style={{ fontSize: 12 }}>
            GeoJSON overlay:{' '}
            <input type="file" accept=".geojson,application/geo+json,application/json" onChange={e=>e.target.files?.[0] && loadGeoJSON(e.target.files[0])} />
          </div>
        </div>
      </div>
      <MapContainer center={center} zoom={5} style={{ height: height, minHeight: 520, width:'100%' }}>
        <FixMapResize />
        <FlyTo center={flyCenter} zoom={10} />
        <TileLayer url={BASEMAPS[basemap].url} attribution={BASEMAPS[basemap].attrib} />
        {geojson ? <GeoJSON data={geojson} /> : null}
        {(assets||[]).slice(0, 20000).map(a => (
          <CircleMarker key={a.asset_id} center={[a.latitude, a.longitude]} radius={5} pathOptions={{}}>
            <Popup>
              <div style={{ fontSize: 12 }}>
                <div><b>{a.asset_id}</b></div>
                <div>{a.label || ''}</div>
                <div>Lat/Lon: {a.latitude}, {a.longitude}</div>
                {onSelectAsset ? (
                  <button onClick={()=>onSelectAsset(a.asset_id)} style={{ marginTop: 8, padding:'6px 10px' }}>
                    Open asset
                  </button>
                ) : null}
              </div>
            </Popup>
          </CircleMarker>
        ))}
      </MapContainer>
    </div>
  )
}
