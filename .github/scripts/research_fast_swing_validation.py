import json, math, statistics, time
from collections import Counter, defaultdict
from datetime import datetime, timezone
import requests

BASE='https://api.gateio.ws/api/v4'
S=requests.Session(); S.headers.update({'User-Agent':'APEX-research/1.0'})

def get_json(path, params=None):
    r=S.get(BASE+path, params=params or {}, timeout=20); r.raise_for_status(); return r.json()

def top_symbols(n=30):
    rows=get_json('/futures/usdt/tickers')
    scored=[]
    for x in rows:
        c=str(x.get('contract',''))
        if not c.endswith('_USDT'): continue
        try:
            last=float(x.get('last') or 0); vol=float(x.get('volume_24h') or 0)
            q=last*vol
        except: continue
        if q>0: scored.append((q,c))
    scored.sort(reverse=True)
    syms=[c.replace('_','') for _,c in scored[:n]]
    if 'BTCUSDT' not in syms: syms.insert(0,'BTCUSDT')
    return syms

def norm(row):
    if isinstance(row,dict):
        return {'t':int(float(row.get('t',0))),'open':float(row.get('o',0)),'high':float(row.get('h',0)),'low':float(row.get('l',0)),'close':float(row.get('c',0)),'volume':float(row.get('v',0) or 0)}
    # fallback list Gate spot-style [t, volume, close, high, low, open,...]
    return {'t':int(float(row[0])),'open':float(row[5]),'high':float(row[3]),'low':float(row[4]),'close':float(row[2]),'volume':float(row[1])}

def candles(symbol, interval, limit):
    contract=symbol[:-4]+'_USDT'
    rows=get_json('/futures/usdt/candlesticks',{'contract':contract,'interval':interval,'limit':limit})
    out=[norm(r) for r in rows]
    out=[x for x in out if x['t']>0 and x['high']>=x['low'] and x['close']>0]
    out.sort(key=lambda x:x['t']); return out

def atr(cs,n=14):
    if len(cs)<n: return 0
    return sum(x['high']-x['low'] for x in cs[-n:])/n

def pivots(cs,lb=2):
    hs=[]; ls=[]
    for i in range(lb,len(cs)-lb):
        w=cs[i-lb:i+lb+1]; c=cs[i]
        if c['high']>=max(x['high'] for x in w): hs.append((i,c['high']))
        if c['low']<=min(x['low'] for x in w): ls.append((i,c['low']))
    return hs,ls

def structure_dir(cs):
    hs,ls=pivots(cs[-80:],2)
    if len(hs)<2 or len(ls)<2: return None
    h1,h2=hs[-2][1],hs[-1][1]; l1,l2=ls[-2][1],ls[-1][1]
    if h2>h1 and l2>l1: return 'BULLISH'
    if h2<h1 and l2<l1: return 'BEARISH'
    return None

def bos_dir(cs, max_age=2):
    if len(cs)<15:return None
    hist=cs[:-1]
    hs,ls=pivots(hist[-50:],2)
    if not hs or not ls:return None
    last=cs[-1]
    rh=hs[-1][1]; rl=ls[-1][1]
    if last['close']>rh and last['open']<=rh:return 'BULLISH'
    if last['close']<rl and last['open']>=rl:return 'BEARISH'
    # allow one-bar-old break
    if max_age>=2 and len(cs)>=2:
        prev=cs[-2]
        if prev['close']>rh:return 'BULLISH'
        if prev['close']<rl:return 'BEARISH'
    return None

def fvg(cs,d):
    for i in range(len(cs)-1,1,-1):
        a,b,c=cs[i-2],cs[i-1],cs[i]
        if d=='BULLISH' and a['high']<c['low']:
            return (a['high'],c['low'])
        if d=='BEARISH' and a['low']>c['high']:
            return (c['high'],a['low'])
    return None

def ob(cs,d):
    # last opposite candle before a strong directional displacement
    avgr=statistics.mean([x['high']-x['low'] for x in cs[-20:]]) if len(cs)>=20 else 0
    for i in range(len(cs)-2,max(1,len(cs)-25),-1):
        c=cs[i]; nxt=cs[i+1]; rng=nxt['high']-nxt['low']; body=abs(nxt['close']-nxt['open'])
        strong=avgr>0 and rng>0 and body/rng>=0.55 and rng>=avgr*1.1
        if not strong: continue
        if d=='BULLISH' and c['close']<c['open'] and nxt['close']>nxt['open']:
            return (c['low'],c['high'])
        if d=='BEARISH' and c['close']>c['open'] and nxt['close']<nxt['open']:
            return (c['low'],c['high'])
    return None

def in_zone(price,z,tol): return bool(z and z[0]-tol<=price<=z[1]+tol)

def pd_ok(cs,d,neutral=0.10):
    hi=max(x['high'] for x in cs[-20:]); lo=min(x['low'] for x in cs[-20:]); mid=(hi+lo)/2; size=hi-lo; p=cs[-1]['close']
    return (d=='BULLISH' and p<mid-size*neutral) or (d=='BEARISH' and p>mid+size*neutral)

def engulf_disp_vol(cs,d,vol_mult=1.6):
    if len(cs)<22:return False
    cur,prev=cs[-1],cs[-2]; rng=cur['high']-cur['low']; body=abs(cur['close']-cur['open']); pbody=abs(prev['close']-prev['open'])
    if rng<=0 or body/rng<0.65:return False
    if d=='BULLISH': engulf=cur['close']>cur['open'] and prev['close']<prev['open'] and cur['open']<=prev['close'] and cur['close']>=prev['open'] and body>pbody*1.1
    else: engulf=cur['close']<cur['open'] and prev['close']>prev['open'] and cur['open']>=prev['close'] and cur['close']<=prev['open'] and body>pbody*1.1
    avgv=statistics.mean(x['volume'] for x in cs[-21:-1])
    return engulf and avgv>0 and cur['volume']>=avgv*vol_mult

def ltf_zone_retest(cs,d):
    a=atr(cs,14); p=cs[-1]['close']; tol=a*0.25
    for z in (ob(cs,d),fvg(cs,d)):
        if in_zone(p,z,tol): return z
    return None

def structural_levels(cs,d,z):
    entry=cs[-1]['close']; a=atr(cs,14); hs,ls=pivots(cs[-60:],2)
    if d=='BULLISH':
        lows=[v for _,v in ls if v<entry]; anchor=max(lows) if lows else (z[0] if z else entry-a)
        sl=min(anchor,z[0] if z else anchor)-a*0.10
        highs=[v for _,v in hs if v>entry]; targets=sorted(highs)
    else:
        highs=[v for _,v in hs if v>entry]; anchor=min(highs) if highs else (z[1] if z else entry+a)
        sl=max(anchor,z[1] if z else anchor)+a*0.10
        lows=[v for _,v in ls if v<entry]; targets=sorted(lows,reverse=True)
    risk=abs(entry-sl)
    if risk<=0:return None
    # choose nearest structural target with RR>=2, NO upper cap in research variant
    for tp in targets:
        rr=abs(tp-entry)/risk
        if rr>=2: return (entry,sl,tp,rr)
    return None

def outcome(levels, future):
    if not levels:return 'NONE'
    e,sl,tp,rr=levels; d='BULLISH' if tp>e else 'BEARISH'
    for c in future:
        if d=='BULLISH':
            sh=c['low']<=sl; th=c['high']>=tp
        else:
            sh=c['high']>=sl; th=c['low']<=tp
        if sh and th:return 'AMBIG'
        if sh:return 'SL'
        if th:return 'TP'
    return 'OPEN'

def slice_before(cs,t,minlen=1):
    a=[x for x in cs if x['t']<t]
    return a if len(a)>=minlen else []

def run():
    syms=top_symbols(30)
    data={}; failures=[]
    for idx,sym in enumerate(syms):
        try:
            data[sym]={'4h':candles(sym,'4h',180),'1h':candles(sym,'1h',500),'15m':candles(sym,'15m',1000)}
            if min(map(len,data[sym].values()))<50: raise ValueError('short history')
        except Exception as e: failures.append((sym,str(e)))
        time.sleep(0.04)
    syms=[s for s in syms if s in data]
    btc=data.get('BTCUSDT')
    if not btc: raise SystemExit('BTC data unavailable')
    end=max(x['t'] for x in btc['15m']); start=end-7*86400
    # every closed hour over last 7 days
    times=list(range((start//3600+1)*3600,end-4*3600,3600))
    counts=Counter(); outcomes=defaultdict(Counter); rr_values=[]; examples=[]
    for t in times:
      b1=slice_before(btc['1h'],t,30); btc_d=structure_dir(b1) if b1 else None
      for sym in syms:
        d=data[sym]; c4=slice_before(d['4h'],t,30); c1=slice_before(d['1h'],t,40); c15=slice_before(d['15m'],t,80)
        if not c4 or not c1 or not c15: continue
        counts['evaluations']+=1
        d4=structure_dir(c4); d1=structure_dir(c1); ltf=bos_dir(c15)
        # CURRENT-like gate chain reconstructed from production criteria.
        cur_dir=d4 or d1
        if not cur_dir: counts['current_no_context']+=1
        elif d4 and d1 and d4!=d1: counts['current_htf_conflict']+=1
        else:
            counts['current_context_pass']+=1
            if sym!='BTCUSDT' and btc_d and cur_dir!=btc_d: counts['current_btc_block']+=1
            else:
                z4=ob(c4,cur_dir) or fvg(c4,cur_dir); tol4=atr(c4)*0.5
                if not in_zone(c4[-1]['close'],z4,tol4): counts['current_4h_zone_block']+=1
                elif not pd_ok(c4,cur_dir,0.10): counts['current_pd_block']+=1
                elif not engulf_disp_vol(c15,cur_dir,1.6): counts['current_ltf_trigger_block']+=1
                elif ltf!=cur_dir: counts['current_ltf_structure_block']+=1
                else:
                    z15=ltf_zone_retest(c15,cur_dir); levels=structural_levels(c15,cur_dir,z15)
                    if not levels: counts['current_rr_block']+=1
                    else:
                        counts['current_candidates']+=1; rr_values.append(levels[3])
                        fut=[x for x in d['15m'] if t<=x['t']<t+24*3600]
                        o=outcome(levels,fut); outcomes['current'][o]+=1
        # PROPOSED FAST: LTF primary; HTF context, hard block only when BOTH 4h and 1h oppose.
        if not ltf:
            counts['proposed_no_ltf_structure']+=1; continue
        if d4 and d1 and d4==d1 and d4!=ltf:
            counts['proposed_both_htf_oppose']+=1; continue
        # BTC as context: hard-block only when BTC + both HTFs align opposite to LTF.
        if sym!='BTCUSDT' and btc_d and btc_d!=ltf and d4==d1==btc_d:
            counts['proposed_strong_macro_conflict']+=1; continue
        z15=ltf_zone_retest(c15,ltf)
        if not z15:
            counts['proposed_no_15m_zone_retest']+=1; continue
        if not engulf_disp_vol(c15,ltf,1.6):
            counts['proposed_no_15m_trigger']+=1; continue
        levels=structural_levels(c15,ltf,z15)
        if not levels:
            counts['proposed_no_rr2_structure']+=1; continue
        counts['proposed_candidates']+=1
        fut=[x for x in d['15m'] if t<=x['t']<t+24*3600]
        o=outcome(levels,fut); outcomes['proposed'][o]+=1
        if len(examples)<12: examples.append({'t':datetime.fromtimestamp(t,timezone.utc).isoformat(),'symbol':sym,'dir':ltf,'d4':d4,'d1':d1,'btc':btc_d,'entry':levels[0],'sl':levels[1],'tp':levels[2],'rr':round(levels[3],2),'outcome':o})
    report={'window_utc':[datetime.fromtimestamp(start,timezone.utc).isoformat(),datetime.fromtimestamp(end,timezone.utc).isoformat()],'symbols':len(syms),'symbol_list':syms,'fetch_failures':failures,'counts':dict(counts),'outcomes':{k:dict(v) for k,v in outcomes.items()},'current_rr_values':{'n':len(rr_values),'min':min(rr_values) if rr_values else None,'median':statistics.median(rr_values) if rr_values else None,'max':max(rr_values) if rr_values else None},'examples':examples,'method_note':'Read-only 7d replay on Gate perpetual candles. Current-like chain reconstructs production FAST gates; proposed variant makes 15m BOS/CHoCH+OB/FVG retest+engulfing/displacement/volume primary, uses 4h/1h/BTC as context, and uses structural RR>=2 with no upper cap.'}
    print('APEX_RESEARCH_JSON='+json.dumps(report,ensure_ascii=False,sort_keys=True))

if __name__=='__main__': run()
