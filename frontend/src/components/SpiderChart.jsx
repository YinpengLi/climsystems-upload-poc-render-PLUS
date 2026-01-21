import React, { useMemo } from 'react'
import { RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, Tooltip, ResponsiveContainer } from 'recharts'

export default function SpiderChart({ rows }) {
  const data = useMemo(() => {
    const by = new Map()
    for (const r of (rows||[])) {
      const ind = r.indicator || 'Unknown'
      const v = (r.value==null)? null : Number(r.value)
      if (v==null) continue
      const prev = by.get(ind)
      if (prev==null || v>prev) by.set(ind, v)
    }
    return Array.from(by.entries()).slice(0, 12).map(([indicator, value]) => ({ indicator, value }))
  }, [rows])

  return (
    <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 10 }}>
      <div style={{ fontWeight: 700, marginBottom: 6 }}>Spider (Radar) — MAX(value) by indicator</div>
      <div style={{ height: 320 }}>
        <ResponsiveContainer width="100%" height="100%">
          <RadarChart data={data}>
            <PolarGrid />
            <PolarAngleAxis dataKey="indicator" tick={{ fontSize: 10 }} />
            <PolarRadiusAxis tick={{ fontSize: 10 }} />
            <Radar dataKey="value" />
            <Tooltip />
          </RadarChart>
        </ResponsiveContainer>
      </div>
      <div style={{ fontSize: 12, color:'#666' }}>Capped to 12 indicators for readability.</div>
    </div>
  )
}
