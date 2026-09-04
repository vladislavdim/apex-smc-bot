import json, statistics, time
from collections import Counter
from datetime import datetime, timezone
import requests

BASE='https://api.gateio.ws/api/v4'
S=requests.Session(); S.headers.update({'User-Agent':'APEX-fast-research/3.0'})
CRYPTO=['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT','DOGEUSDT','TONUSDT','ADAUSDT','AVAXUSDT','LINKUSDT','SUIUSDT','TRXUSDT','DOTUSDT','LTCUSDT','BCHUSDT','NEARUSDT','APTUSDT','ARBUSDT','OPUSDT','INJUSDT','ATOMUSDT','FILUSDT','WLDUSDT','AAVEUSDT','UNIUSDT','ETCUSDT','ICPUSDT','HBARUSDT','ENAUSDT','ONDOUSDT','TAOUSDT','RENDERUSDT','SEIUSDT','TIAUSDT','JUPUSDT','FETUSDT','PEPEUSDT','SHIBUSDT','BONKUSDT','TRUMPUSDT']

def get(path,p=None):
 r=S.get(BASE+path,params=p or {},timeout=20); r.raise_for_status(); return r.json()
def norm(x): return {'t':int(float(x['t'])),'open':float(x['o']),'high':float(x['h']),'low':float(x['l']),'close':float(x['c']),'volume':float(x.get('v') or 0)}
def fetch(sym,tf,limit):
 rows=get('/futures/usdt/candlesticks',{'contract':sym[:-4]+'_USDT','interval':tf,'limit':limit}); a=[norm(x) for x in rows];a.sort(key=lambda x:x['t']);return a
def slc(a,t): return [x for x in a if x['t']<t]
def atr(a,n=14): return statistics.mean([x['high']-x['low'] for x in a[-n:]]) if len(a)>=n else 0

def piv(a,lb=2):
 h=[];l=[]
 for i in range(lb,len(a)-lb):
  w=a[i-lb:i+lb+1]
  if a[i]['high']>=max(x['high'] for x in w):h.append((i,a[i]['high']))
  if a[i]['low']<=min(x['low'] for x in w):l.append((i,a[i]['low']))
 return h,l

def trend(a):
 h,l=piv(a[-80:],2)
 if len(h)<2 or len(l)<2:return None
 if h[-1][1]>h[-2][1] and l[-1][1]>l[-2][1]:return 'BULLISH'
 if h[-1][1]<h[-2][1] and l[-1][1]<l[-2][1]:return 'BEARISH'
 return None

def fresh_bos_event(a,age=4):
 if len(a)<30:return None
 for off in range(1,min(age,len(a)-10)+1):
  idx=len(a)-off; hist=a[:idx]; c=a[idx]
  h,l=piv(hist[-60:],2)
  if not h or not l:continue
  rh=h[-1][1]; rl=l[-1][1]
  if c['close']>rh and c['open']<=rh:return ('BULLISH',c['t'])
  if c['close']<rl and c['open']>=rl:return ('BEARISH',c['t'])
 return None

def fvg(a,d):
 for i in range(len(a)-1,max(1,len(a)-30),-1):
  x,z=a[i-2],a[i]
  if d=='BULLISH' and x['high']<z['low']:return (x['high'],z['low'])
  if d=='BEARISH' and x['low']>z['high']:return (z['high'],x['low'])
 return None

def ob(a,d):
 if len(a)<22:return None
 ar=statistics.mean([x['high']-x['low'] for x in a[-20:]])
 for i in range(len(a)-2,max(1,len(a)-30),-1):
  c,n=a[i],a[i+1];r=n['high']-n['low'];b=abs(n['close']-n['open'])
  if ar<=0 or r<=0 or b/r<.55 or r<ar*1.05:continue
  if d=='BULLISH' and c['close']<c['open'] and n['close']>n['open']:return(c['low'],c['high'])
  if d=='BEARISH' and c['close']>c['open'] and n['close']<n['open']:return(c['low'],c['high'])
 return None

def recent_retest(a,d,age=8):
 z=ob(a,d) or fvg(a,d)
 if not z:return None
 tol=atr(a)*.20
 for c in a[-age:]:
  if c['low']<=z[1]+tol and c['high']>=z[0]-tol:return z
 return None

def recent_trigger(a,d,age=4):
 if len(a)<25:return False
 for cidx in range(max(1,len(a)-age),len(a)):
  c=a[cidx];p=a[cidx-1];r=c['high']-c['low'];b=abs(c['close']-c['open']);pb=abs(p['close']-p['open'])
  if r<=0 or b/r<.65:continue
  av=statistics.mean(x['volume'] for x in a[max(0,cidx-20):cidx]) if cidx>0 else 0
  if av<=0 or c['volume']<av*1.6:continue
  if d=='BULLISH': ok=c['close']>c['open'] and p['close']<p['open'] and c['open']<=p['close'] and c['close']>=p['open'] and b>pb*1.1
  else: ok=c['close']<c['open'] and p['close']>p['open'] and c['open']>=p['close'] and c['close']<=p['open'] and b>pb*1.1
  if ok:return True
 return False

def levels(a,d,z):
 e=a[-1]['close'];A=atr(a);h,l=piv(a[-80:],2)
 if not A:return None
 if d=='BULLISH':
  lows=[v for _,v in l if v<e]; sl=min(z[0],max(lows) if lows else z[0])-A*.1; targets=sorted(v for _,v in h if v>e)
 else:
  highs=[v for _,v in h if v>e]; sl=max(z[1],min(highs) if highs else z[1])+A*.1; targets=sorted((v for _,v in l if v<e),reverse=True)
 risk=abs(e-sl)
 if risk<=0:return None
 for tp in targets:
  rr=abs(tp-e)/risk
  if rr>=2:return(e,sl,tp,rr)
 return None

def outcome(level,fut):
 e,sl,tp,rr=level; bull=tp>e
 for c in fut:
  s=c['low']<=sl if bull else c['high']>=sl; p=c['high']>=tp if bull else c['low']<=tp
  if s and p:return 'AMBIG'
  if s:return 'SL'
  if p:return 'TP'
 return 'OPEN'

def pd1h(a,d):
 h=max(x['high'] for x in a[-30:]);l=min(x['low'] for x in a[-30:]);m=(h+l)/2;p=a[-1]['close']
 return p<=m if d=='BULLISH' else p>=m

def context_ok(name,s,D,d4,d1,b4,b1,a1):
 if name=='A_CONSERVATIVE':
  if d1!=D:return False,'need_1h_align'
  if d4 is not None and d4!=D:return False,'4h_opposes'
  if s!='BTCUSDT' and b1 is not None and b1!=D:return False,'btc1_opposes'
  return True,'ok'
 if name=='B_BALANCED':
  if not (d1==D or d4==D):return False,'need_1h_or_4h_support'
  if d1 is not None and d4 is not None and d1==d4 and d1!=D:return False,'pair_htf_both_oppose'
  if s!='BTCUSDT' and b1 is not None and b4 is not None and b1==b4 and b1!=D:return False,'btc_htf_both_oppose'
  return True,'ok'
 if name=='C_AGGRESSIVE':
  if d1 is not None and d4 is not None and d1==d4 and d1!=D:return False,'pair_htf_both_oppose'
  if s!='BTCUSDT' and b1 is not None and b4 is not None and b1==b4 and b1!=D and d1==d4==b1:return False,'macro_pair_all_oppose'
  if not (d1==D or d4==D or pd1h(a1,D)):return False,'weak_context'
  return True,'ok'
 raise ValueError(name)

def summarize(rows):
 outc=Counter(r['outcome'] for r in rows); closed=outc['TP']+outc['SL'];
 wins=[r for r in rows if r['outcome']=='TP']; losses=outc['SL']
 expectancy=(sum(r['rr'] for r in wins)-losses)/closed if closed else None
 worst_closed=closed+outc['AMBIG']; worst_exp=(sum(r['rr'] for r in wins)-losses-outc['AMBIG'])/worst_closed if worst_closed else None
 return {'candidates':len(rows),'outcomes':dict(outc),'closed':closed,'winrate':round(outc['TP']/closed,4) if closed else None,'avg_rr':round(statistics.mean(r['rr'] for r in rows),3) if rows else None,'median_rr':round(statistics.median(r['rr'] for r in rows),3) if rows else None,'expectancy_R':round(expectancy,3) if expectancy is not None else None,'worstcase_expectancy_R':round(worst_exp,3) if worst_exp is not None else None}

def main():
 data={};bad=[]
 for s in CRYPTO:
  try:
   data[s]={'4h':fetch(s,'4h',180),'1h':fetch(s,'1h',500),'15m':fetch(s,'15m',1000)}
   if min(map(len,data[s].values()))<80:raise ValueError('short history')
  except Exception as e:bad.append([s,str(e)]);data.pop(s,None)
  time.sleep(.03)
 btc=data['BTCUSDT']; end=max(x['t'] for x in btc['15m']); start=end-9*86400; ts=range((start//1800+1)*1800,end-12*3600,1800)
 variants=['A_CONSERVATIVE','B_BALANCED','C_AGGRESSIVE']; counts={v:Counter() for v in variants}; rows={v:[] for v in variants}; seen={v:set() for v in variants}
 for t in ts:
  b4s=slc(btc['4h'],t); b1s=slc(btc['1h'],t); bt4=trend(b4s); bt1=trend(b1s)
  for s,d in data.items():
   a4=slc(d['4h'],t);a1=slc(d['1h'],t);a15=slc(d['15m'],t)
   if len(a4)<30 or len(a1)<40 or len(a15)<80:continue
   ev=fresh_bos_event(a15,4)
   for v in variants: counts[v]['eval']+=1
   if not ev:
    for v in variants: counts[v]['no_fresh_15m_structure']+=1
    continue
   D,event_t=ev; d4=trend(a4); d1=trend(a1)
   z=recent_retest(a15,D,8)
   trig=recent_trigger(a15,D,4)
   L=levels(a15,D,z) if z and trig else None
   for v in variants:
    key=(s,D,event_t)
    if key in seen[v]: counts[v]['duplicate_same_structure_event']+=1; continue
    ok,reason=context_ok(v,s,D,d4,d1,bt4,bt1,a1)
    if not ok: counts[v][reason]+=1; continue
    if not z: counts[v]['no_15m_ob_fvg_retest']+=1; continue
    if not trig: counts[v]['no_15m_trigger']+=1; continue
    if not L: counts[v]['no_structural_rr_ge_2']+=1; continue
    seen[v].add(key); counts[v]['candidate']+=1
    fut=[x for x in d['15m'] if t<=x['t']<t+12*3600]; o=outcome(L,fut)
    rows[v].append({'t':datetime.fromtimestamp(t,timezone.utc).isoformat(),'symbol':s,'dir':D,'d4':d4,'d1':d1,'btc4':bt4,'btc1':bt1,'rr':round(L[3],4),'outcome':o})
 result={'window':[datetime.fromtimestamp(start,timezone.utc).isoformat(),datetime.fromtimestamp(end,timezone.utc).isoformat()],'symbols':len(data),'bad':bad,'variants':{}}
 for v in variants:
  result['variants'][v]={'summary':summarize(rows[v]),'stops':dict(counts[v]),'examples':rows[v][:15]}
 print('FAST_VARIANTS_JSON='+json.dumps(result,ensure_ascii=False,sort_keys=True))
if __name__=='__main__':main()
