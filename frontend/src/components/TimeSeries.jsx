import React, { useMemo } from 'react'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'

export default function TimeSeries({ rows }) {
  const data = useMemo(() => {
    const byYear = new Map()
    for (const r of (rows||[])) {
      if (r.year==null || r.value==null) continue
      const y = Number(r.year)
      const v = Number(r.value)
      const prev = byYear.get(y)
      byYear.set(y, prev==null ? v : Math.max(prev, v))
    }
    return Array.from(byYear.entries()).sort((a,b)=>a[0]-b[0]).map(([year, value])=>({ year, value }))
  }, [rows])

  return (
    <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 10 }}>
      <div style={{ fontWeight: 700, marginBottom: 6 }}>Time series — MAX(value) per year</div>
      <div style={{ height: 260 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year" />
            <YAxis />
            <Tooltip />
            <Line dataKey="value" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
