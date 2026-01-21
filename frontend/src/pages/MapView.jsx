import React from 'react'

export default function MapView({ ctx }) {
  const { activeId, datasets } = ctx
  const active = datasets.find(d => d.id === activeId)

  return (
    <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 14 }}>
      <div style={{ fontWeight: 900, marginBottom: 6 }}>Map View (POC)</div>
      <div style={{ fontSize: 12, color:'#666' }}>
        This package focuses on Render-safe upload + ingestion reliability.
        Next step is enabling full map + filters once ingestion is stable.
      </div>
      <div style={{ marginTop: 10, fontSize: 12 }}>
        Active dataset: <b>{active?.name || 'None'}</b> — status: <b>{active?.status || '-'}</b>
      </div>
    </div>
  )
}
