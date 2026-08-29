"""Closed loop from immutable entry evidence to objective outcomes."""
from __future__ import annotations
import hashlib,json,os,sqlite3
from typing import Any
DB_PATH=os.environ.get("APEX_DB_PATH",os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),"brain.db"))

def _connect(db_path=DB_PATH):
    conn=sqlite3.connect(db_path,timeout=20,check_same_thread=False);conn.row_factory=sqlite3.Row;conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS closed_loop_observations(signal_id INTEGER PRIMARY KEY,symbol TEXT,strategy TEXT,direction TEXT,timeframe TEXT,regime TEXT,condition_key TEXT,entry_evidence_json TEXT NOT NULL,outcome TEXT,max_favorable_pct REAL,max_adverse_pct REAL,captured_at TEXT DEFAULT CURRENT_TIMESTAMP,closed_at TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS closed_loop_hypotheses(condition_key TEXT PRIMARY KEY,condition_json TEXT NOT NULL,samples INTEGER DEFAULT 0,wins INTEGER DEFAULT 0,losses INTEGER DEFAULT 0,win_rate REAL,avg_mfe REAL,avg_mae REAL,confidence REAL DEFAULT 0,state TEXT DEFAULT 'collecting',updated_at TEXT DEFAULT CURRENT_TIMESTAMP)")
    return conn
def _strategy(c): return str(c.get("scan_type") or c.get("signal_type") or c.get("grade") or "UNKNOWN").upper()
def _condition(c):
    review=c.get("_external_quality_review") if isinstance(c.get("_external_quality_review"),dict) else {}; ext=review.get("context") if isinstance(review.get("context"),dict) else {}; news=review.get("news_context") if isinstance(review.get("news_context"),dict) else {}; zones=review.get("historical_zones") if isinstance(review.get("historical_zones"),dict) else {}; nearest=(zones.get("zones") or [{}])[0] if isinstance(zones.get("zones"),list) and zones.get("zones") else {}
    return {"strategy":_strategy(c),"direction":str(c.get("direction","")).upper(),"timeframe":str(c.get("timeframe","")),"regime":str(c.get("regime","UNKNOWN")),"external_bias":str(ext.get("external_bias","unknown")),"external_confidence_bin":round(float(ext.get("external_confidence",0) or 0),1),"news_risk":str(news.get("risk_level","UNKNOWN")),"nearest_zone_type":str(nearest.get("zone_type","none"))}
def _key(condition): return hashlib.sha256(json.dumps(condition,sort_keys=True,separators=(",",":")).encode()).hexdigest()[:24]

def capture_signal_evidence(signal_id:int,candidate:dict[str,Any],db_path=DB_PATH):
    if not signal_id:return
    condition=_condition(candidate);key=_key(condition);review=candidate.get("_external_quality_review") if isinstance(candidate.get("_external_quality_review"),dict) else {}
    evidence={"technical_evidence":candidate.get("technical_evidence") or {},"quality_review":review,"immutable_levels":{k:candidate.get(k) for k in ("entry","sl","tp1","tp2","tp3","rr")},"condition":condition}
    conn=_connect(db_path);conn.execute("INSERT OR IGNORE INTO closed_loop_observations(signal_id,symbol,strategy,direction,timeframe,regime,condition_key,entry_evidence_json) VALUES(?,?,?,?,?,?,?,?)",(signal_id,candidate.get("symbol"),_strategy(candidate),candidate.get("direction"),candidate.get("timeframe"),candidate.get("regime","UNKNOWN"),key,json.dumps(evidence,ensure_ascii=False,default=str)));conn.execute("INSERT OR IGNORE INTO closed_loop_hypotheses(condition_key,condition_json) VALUES(?,?)",(key,json.dumps(condition,sort_keys=True,separators=(",",":"))));conn.commit();conn.close()

def close_learning_loop(signal_id:int,outcome:str,db_path=DB_PATH):
    if not signal_id:return
    conn=_connect(db_path)
    try: memory=conn.execute("SELECT max_favorable_pct,max_adverse_pct FROM market_memory_snapshots WHERE signal_id=?",(signal_id,)).fetchone()
    except sqlite3.OperationalError: memory=None
    mfe,mae=(memory[0],memory[1]) if memory else (None,None)
    conn.execute("UPDATE closed_loop_observations SET outcome=?,max_favorable_pct=?,max_adverse_pct=?,closed_at=CURRENT_TIMESTAMP WHERE signal_id=? AND outcome IS NULL",(outcome,mfe,mae,signal_id));row=conn.execute("SELECT condition_key FROM closed_loop_observations WHERE signal_id=?",(signal_id,)).fetchone()
    if not row:conn.commit();conn.close();return
    key=row[0];rows=conn.execute("SELECT outcome,max_favorable_pct,max_adverse_pct FROM closed_loop_observations WHERE condition_key=? AND outcome IS NOT NULL",(key,)).fetchall();wins=sum(str(r[0]).lower().startswith("tp") for r in rows);losses=sum(str(r[0]).lower() in {"sl","stop","stop_loss","trailing_sl"} for r in rows);samples=wins+losses;rate=wins/samples if samples else None;mfes=[float(r[1]) for r in rows if r[1] is not None];maes=[float(r[2]) for r in rows if r[2] is not None]
    try:minimum=max(8,int(os.environ.get("CLOSED_LOOP_MIN_SAMPLES","12")))
    except ValueError:minimum=12
    state="collecting"
    if samples>=minimum:state="confirmed" if rate is not None and rate>=.75 else "avoid" if rate is not None and rate<=.35 else "inconclusive"
    confidence=min(.95,samples/max(minimum*2,1)) if state in {"confirmed","avoid"} else 0
    conn.execute("UPDATE closed_loop_hypotheses SET samples=?,wins=?,losses=?,win_rate=?,avg_mfe=?,avg_mae=?,confidence=?,state=?,updated_at=CURRENT_TIMESTAMP WHERE condition_key=?",(samples,wins,losses,rate,sum(mfes)/len(mfes) if mfes else None,sum(maes)/len(maes) if maes else None,confidence,state,key));conn.commit();conn.close()

def build_learning_context(candidate:dict[str,Any],db_path=DB_PATH):
    condition=_condition(candidate);key=_key(condition)
    try:
        conn=_connect(db_path);row=conn.execute("SELECT * FROM closed_loop_hypotheses WHERE condition_key=?",(key,)).fetchone();total=conn.execute("SELECT COUNT(*) FROM closed_loop_observations WHERE outcome IS NOT NULL").fetchone()[0];conn.close()
        try:minimum=max(20,int(os.environ.get("NEW_STRATEGY_MIN_CLOSED_TRADES","30")))
        except ValueError:minimum=30
        return {"available":bool(row and row["samples"]),"comparable_condition":dict(row) if row else {"condition":condition,"samples":0,"state":"collecting"},"closed_results_total":total,"new_strategy_research_ready":total>=minimum,"new_strategy_minimum":minimum,"rule":"do not propose or activate a new strategy before the minimum objective sample"}
    except Exception:return {"available":False,"closed_results_total":0,"new_strategy_research_ready":False}
def format_learning_context(context):return "CLOSED-LOOP OUTCOME EVIDENCE:\n"+json.dumps(context,ensure_ascii=False,default=str,separators=(",",":"))
