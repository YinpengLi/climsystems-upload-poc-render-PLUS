import React, { useState } from 'react'
import { aiAsk } from '../api.js'

export default function Assistant({ datasetId }) {
  const [q, setQ] = useState("Explain Theme=Change and how to interpret it.")
  const [a, setA] = useState(null)
  const [err, setErr] = useState(null)
  const [loading, setLoading] = useState(false)

  async function ask() {
    setErr(null); setLoading(true)
    try {
      const res = await aiAsk(datasetId, q)
      setA(res.answer)
    } catch(e) {
      setErr(String(e.message||e))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ border:'1px solid #eee', borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 800, marginBottom: 6 }}>AI Assistant (offline demo)</div>
      <div style={{ fontSize: 12, color:'#666', marginBottom: 8 }}>
        Explains terminology + adaptation context. Deterministic (no external calls) for a stable client demo.
      </div>
      <textarea value={q} onChange={e=>setQ(e.target.value)} rows={3} style={{ width:'100%', padding: 8, border:'1px solid #eee', borderRadius: 10 }} />
      <button onClick={ask} disabled={loading} style={{ marginTop: 8, padding: 10, borderRadius: 10, border:'1px solid #111', background:'#111', color:'#fff' }}>
        {loading ? "Thinking…" : "Ask"}
      </button>
      {err ? <div style={{ marginTop: 8, color:'crimson', fontSize: 12, whiteSpace:'pre-wrap' }}>{err}</div> : null}
      {a ? <div style={{ marginTop: 10, fontSize: 13, lineHeight: 1.4, whiteSpace:'pre-wrap' }}>{a}</div> : null}
    </div>
  )
}
