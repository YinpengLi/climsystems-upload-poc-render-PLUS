import React from 'react'

function MultiSelect({ label, options, selected, onChange }) {
  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ fontSize: 12, color:'#666', marginBottom: 4 }}>{label}</div>
      <div style={{ maxHeight: 120, overflow:'auto', border:'1px solid #eee', borderRadius: 8, padding: 8 }}>
        <label style={{ display:'block', fontSize: 12 }}>
          <input type="checkbox" checked={selected.length===0} onChange={()=>onChange([])} /> Select all
        </label>
        {options.map(o => (
          <label key={String(o)} style={{ display:'block', fontSize: 12 }}>
            <input
              type="checkbox"
              checked={selected.includes(o)}
              onChange={e=>{
                if (e.target.checked) onChange([...selected, o])
                else onChange(selected.filter(x=>x!==o))
              }}
            /> {String(o)}
          </label>
        ))}
      </div>
    </div>
  )
}

export default function Filters({ options, filters, setFilters }) {
  return (
    <div>
      <MultiSelect label="Years" options={options.years||[]} selected={filters.years||[]} onChange={years=>setFilters({...filters, years})} />
      <MultiSelect label="Scenarios" options={options.scenarios||[]} selected={filters.scenarios||[]} onChange={scenarios=>setFilters({...filters, scenarios})} />
      <MultiSelect label="Themes" options={options.themes||[]} selected={filters.themes||[]} onChange={themes=>setFilters({...filters, themes})} />
      <MultiSelect label="Indicators" options={options.indicators||[]} selected={filters.indicators||[]} onChange={indicators=>setFilters({...filters, indicators})} />
    </div>
  )
}
