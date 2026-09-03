from pathlib import Path
import re

p=Path('stats_server.py')
s=p.read_text(encoding='utf-8')

s=re.sub(r'<div class="section card"><h2 id=catalogTitle>Критерии стратегии</h2><div id=catalog class=criteria></div></div>\n?', '', s)
s=s.replace('renderChecks();renderCatalog();renderRows()', 'renderChecks();renderRows()')

marker='function levels(r){const c=r.candidate||{},s=(r.stop||{}).snapshot||{};return {entry:c.entry??s.entry,sl:c.sl??s.sl,tp1:c.tp1??c.tp??s.tp1??s.tp,tp2:c.tp2??s.tp2,rr:r.rr_value}}'
if marker not in s:
    raise SystemExit('levels marker not found')
helper=r'''function levels(r){const c=r.candidate||{},s=(r.stop||{}).snapshot||{};return {entry:c.entry??s.entry,sl:c.sl??s.sl,tp1:c.tp1??c.tp??s.tp1??s.tp,tp2:c.tp2??s.tp2,tp3:c.tp3??s.tp3,rr:r.rr_value}}
function detailText(r){
  const l=levels(r),g=r.groq_review||{},stop=r.stop||{},snap=stop.snapshot||{},checks=Array.isArray(r.checks)?r.checks:[];
  const dir=String(r.candidate?.direction??snap.direction??'').toUpperCase()||'—';
  const sections={CONTEXT:[],STRUCTURE:[],RISK:[],EXTERNAL:[],OTHER:[]};
  const classify=t=>{t=String(t||'').toLowerCase();if(/4h|1h|htf|trend|premium|discount|session|location|regime|btc/.test(t))return 'CONTEXT';if(/bos|choch|ob|fvg|displacement|sweep|liquid|spring|sos|utad|sow|structure|volume/.test(t))return 'STRUCTURE';if(/rr|risk|entry|sl|tp|drift|integrity/.test(t))return 'RISK';if(/funding|open interest|\boi\b|whale|flow|derivative|external/.test(t))return 'EXTERNAL';return 'OTHER'};
  for(const ch of checks){const label=ch.label||ch.condition||ch.code||'check';const st=String(ch.state||'').toUpperCase();const icon=st==='PASS'?'✅':st==='FAIL'?'❌':'⚠️';sections[classify(label)].push(`${icon} ${label}`)}
  const lines=[`${r.symbol||'—'} · ${r.strategy||'—'} · ${dir}`,(r.finished_at||r.occurred_at||'').replace('T',' ').slice(0,19)+' UTC',''];
  for(const k of ['CONTEXT','STRUCTURE','RISK','EXTERNAL','OTHER']){if(sections[k].length){lines.push(`[${k}]`,...sections[k],'')}}
  lines.push('[LEVELS]',`Entry: ${num(l.entry)}`,`SL:    ${num(l.sl)}`,`TP1:   ${num(l.tp1)}`,`TP2:   ${num(l.tp2)}`,`TP3:   ${num(l.tp3)}`,`RR:    ${num(l.rr)}`,'');
  if(g.decision){lines.push('[GROQ]',`Decision: ${g.decision}`,`Confidence: ${g.confidence!==undefined?Math.round(Number(g.confidence)*100)+'%':'—'}`);if(Array.isArray(g.reasons)&&g.reasons.length)lines.push('Reasons:',...g.reasons.map(x=>'❌ '+x));if(Array.isArray(g.risks)&&g.risks.length)lines.push('Risks:',...g.risks.map(x=>'⚠️ '+x));lines.push('')}else lines.push('[GROQ]','Not reached','');
  lines.push('[STOP]',stop.label||stop.code||'—');
  return lines.join('\n')
}'''
s=s.replace(marker,helper)
old='<div class=detailbox>${esc(JSON.stringify(det,null,2))}</div>'
new='<div class=detailbox>${esc(detailText(r))}</div><details><summary class=muted style="cursor:pointer;margin-top:8px">Raw data</summary><div class=detailbox>${esc(JSON.stringify(det,null,2))}</div></details>'
if old not in s:
    raise SystemExit('detail box marker not found')
s=s.replace(old,new)
p.write_text(s,encoding='utf-8')
print('patched stats_server.py')
