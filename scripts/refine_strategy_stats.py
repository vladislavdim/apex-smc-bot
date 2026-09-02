from __future__ import annotations
import ast
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str):
    text=path.read_text(encoding='utf-8')
    if old not in text:
        raise RuntimeError(f'{path}: patch anchor missing: {old[:80]!r}')
    text=text.replace(old,new,1)
    ast.parse(text,filename=str(path))
    path.write_text(text,encoding='utf-8')


def patch_setup_audit():
    p=ROOT/'core/setup_audit.py'
    replace_once(p,
'''DB_PATH = os.environ.get("APEX_DB_PATH", os.environ.get("APEX_BRAIN_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "brain.db")))''',
'''_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.environ.get("APEX_STATS_LOCAL_DB", os.path.join(_ROOT, "setup_audit.db"))''')
    replace_once(p,
'''        conn.execute("DELETE FROM setup_audit_events WHERE occurred_at < datetime('now','-90 days')")''',
'''        conn.execute("DELETE FROM setup_audit_events WHERE occurred_at < datetime('now','-7 days')")''')
    replace_once(p,
'''        conn.execute("UPDATE setup_audit_events SET synced=1,last_sync_error=NULL WHERE event_key=?", (event_key,))''',
'''        # Postgres is the durable source for the dashboard.  The local SQLite
        # database is only an outbox, so synced payloads are removed immediately.
        conn.execute("DELETE FROM setup_audit_events WHERE event_key=?", (event_key,))''')


def patch_gitignore():
    p=ROOT/'.gitignore'; text=p.read_text(encoding='utf-8')
    extra='setup_audit.db\nsetup_audit.db-wal\nsetup_audit.db-shm\n'
    if 'setup_audit.db\n' not in text:
        text += ('' if text.endswith('\n') else '\n') + extra
        p.write_text(text,encoding='utf-8')


def patch_telegram():
    p=ROOT/'core/telegram_dashboard.py'; text=p.read_text(encoding='utf-8')
    block='''    stats_url = os.environ.get("APEX_STATS_URL", "").strip()\n    if stats_url:\n        safe_url = html.escape(stats_url, quote=True)\n        lines.extend(["", f'<a href="{safe_url}">📊 Полная статистика</a>'])\n'''
    if block not in text: raise RuntimeError('telegram dashboard stats link block missing')
    text=text.replace(block,'',1)
    # os is no longer needed in this view module.
    text=text.replace('import os\n','',1)
    ast.parse(text); p.write_text(text,encoding='utf-8')

    p=ROOT/'bot.py'; text=p.read_text(encoding='utf-8')
    old='''        await callback.message.edit_text(\n            text,\n            parse_mode="HTML",\n            reply_markup=InlineKeyboardMarkup(inline_keyboard=[\n                [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_scanners"),\n                 InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],\n            ]),\n        )\n\n    elif data == "menu_experience":'''
    new='''        radar_buttons = [\n            [InlineKeyboardButton(text="🔄 Обновить", callback_data="menu_scanners"),\n             InlineKeyboardButton(text="🔙 Меню", callback_data="menu_back")],\n        ]\n        stats_url = os.environ.get("APEX_STATS_URL", "").strip()\n        if stats_url:\n            radar_buttons.insert(0, [InlineKeyboardButton(text="📊 Полная статистика", url=stats_url)])\n        await callback.message.edit_text(\n            text,\n            parse_mode="HTML",\n            reply_markup=InlineKeyboardMarkup(inline_keyboard=radar_buttons),\n        )\n\n    elif data == "menu_experience":'''
    if old not in text: raise RuntimeError('bot radar keyboard anchor missing')
    text=text.replace(old,new,1); ast.parse(text); p.write_text(text,encoding='utf-8')


def patch_stats_server():
    p=ROOT/'stats_server.py'; text=p.read_text(encoding='utf-8')
    old='''def _fetch(days: int, strategy: str, symbol: str) -> list[dict[str, Any]]:\n    days = max(1, min(int(days), 30)); where = ["occurred_at >= NOW() - (%s * INTERVAL '1 day')"]; params: list[Any] = [days]\n    if strategy: where.append("strategy=%s"); params.append(strategy.upper())\n    if symbol: where.append("symbol=%s"); params.append(symbol.upper())'''
    new='''def _fetch(days: int, strategy: str, symbol: str, date_from: str = "", date_to: str = "") -> list[dict[str, Any]]:\n    days = max(1, min(int(days), 30)); where: list[str] = []; params: list[Any] = []\n    if date_from or date_to:\n        if date_from: where.append("occurred_at >= %s::date"); params.append(date_from)\n        if date_to: where.append("occurred_at < (%s::date + INTERVAL '1 day')"); params.append(date_to)\n    else:\n        where.append("occurred_at >= NOW() - (%s * INTERVAL '1 day')"); params.append(days)\n    if strategy: where.append("strategy=%s"); params.append(strategy.upper())\n    if symbol: where.append("symbol=%s"); params.append(symbol.upper())'''
    if old not in text: raise RuntimeError('stats _fetch anchor missing')
    text=text.replace(old,new,1)
    old='''def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",\n                    min_rr: float | None = None, max_rr: float | None = None, page: int = 1, page_size: int = 100) -> dict[str, Any]:\n    events = _fetch(days, strategy, symbol); attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]'''
    new='''def build_dashboard(days: int = 1, strategy: str = "", symbol: str = "", outcome: str = "", groq: str = "",\n                    min_rr: float | None = None, max_rr: float | None = None, page: int = 1, page_size: int = 100,\n                    date_from: str = "", date_to: str = "") -> dict[str, Any]:\n    events = _fetch(days, strategy, symbol, date_from, date_to); attempts=[]; reviews={}; decisions=defaultdict(list); scan_events=[]'''
    if old not in text: raise RuntimeError('build_dashboard anchor missing')
    text=text.replace(old,new,1)
    old='''<div class=controls><input id=symbol placeholder="Пара, напр. BTCUSDT"><select id=outcome>'''
    new='''<div class=controls><input id=symbol placeholder="Пара, напр. BTCUSDT"><input id=date_from type=date title="Дата от"><input id=date_to type=date title="Дата до"><select id=outcome>'''
    if old not in text: raise RuntimeError('HTML controls anchor missing')
    text=text.replace(old,new,1)
    old="""for(const id of ['symbol','outcome','groq']){const v=document.getElementById(id).value.trim();if(v)p.set(id,v)}"""
    new="""for(const id of ['symbol','date_from','date_to','outcome','groq']){const v=document.getElementById(id).value.trim();if(v)p.set(id,v)}"""
    if old not in text: raise RuntimeError('JS params anchor missing')
    text=text.replace(old,new,1)
    old='''data=build_dashboard(int(val("days","1")),val("strategy"),val("symbol"),val("outcome"),val("groq"),float(val("min_rr")) if val("min_rr") else None,float(val("max_rr")) if val("max_rr") else None,int(val("page","1")),int(val("page_size","100"))); self._json(data)'''
    new='''data=build_dashboard(int(val("days","1")),val("strategy"),val("symbol"),val("outcome"),val("groq"),float(val("min_rr")) if val("min_rr") else None,float(val("max_rr")) if val("max_rr") else None,int(val("page","1")),int(val("page_size","100")),val("date_from"),val("date_to")); self._json(data)'''
    if old not in text: raise RuntimeError('handler build_dashboard anchor missing')
    text=text.replace(old,new,1)
    ast.parse(text); p.write_text(text,encoding='utf-8')


def add_tests():
    p=ROOT/'tests/test_stats_server.py'
    p.write_text('''import unittest\nfrom unittest.mock import patch\nimport stats_server\n\n\nclass StatsServerAggregationTests(unittest.TestCase):\n    def test_attempt_groq_and_stop_are_joined_by_attempt_key(self):\n        events=[\n            {"event_key":"a1","kind":"attempt","strategy":"FAST","symbol":"BTCUSDT","occurred_at":"2026-09-02T12:00:00+00:00","payload":{"attempt_key":"a1","strategy":"FAST","symbol":"BTCUSDT","outcome":"FILTERED","stop":{"code":"NO_TRIGGER","label":"fresh 15m trigger","snapshot":{"entry":100,"sl":98,"rr":2.0}},"checks":[{"label":"4H zone","state":"PASS"}]}},\n            {"event_key":"a2","kind":"attempt","strategy":"SWING","symbol":"ETHUSDT","occurred_at":"2026-09-02T13:00:00+00:00","payload":{"attempt_key":"a2","strategy":"SWING","symbol":"ETHUSDT","outcome":"CANDIDATE","candidate":{"direction":"BEARISH","entry":200,"sl":210,"tp1":175,"rr":2.5}}},\n            {"event_key":"g2","kind":"groq_review","strategy":"SWING","symbol":"ETHUSDT","occurred_at":"2026-09-02T13:01:00+00:00","payload":{"attempt_key":"a2","decision":"REJECT","confidence":0.71,"reasons":["HTF conflict"]}},\n        ]\n        with patch.object(stats_server,'_fetch',return_value=events) as fetch:\n            data=stats_server.build_dashboard(days=7,date_from='2026-09-01',date_to='2026-09-02')\n        fetch.assert_called_once_with(7,'','', '2026-09-01','2026-09-02')\n        self.assertEqual(data['summary']['attempts'],2)\n        self.assertEqual(data['summary']['near_setups'],1)\n        self.assertEqual(data['summary']['groq_reject'],1)\n        self.assertEqual(data['failures'][0]['label'],'fresh 15m trigger')\n        self.assertEqual(data['groq']['reasons'][0]['label'],'HTF conflict')\n\n    def test_strategy_pair_groq_and_rr_filters_preserve_exact_rows(self):\n        events=[\n            {"event_key":"a","kind":"attempt","strategy":"FAST","symbol":"SOLUSDT","occurred_at":"2026-09-02T10:00:00+00:00","payload":{"attempt_key":"a","strategy":"FAST","symbol":"SOLUSDT","outcome":"CANDIDATE","candidate":{"rr":2.2}}},\n            {"event_key":"g","kind":"groq_review","strategy":"FAST","symbol":"SOLUSDT","occurred_at":"2026-09-02T10:01:00+00:00","payload":{"attempt_key":"a","decision":"WAIT","confidence":0.6,"reasons":["Need confirmation"]}},\n        ]\n        with patch.object(stats_server,'_fetch',return_value=events):\n            data=stats_server.build_dashboard(strategy='FAST',symbol='SOLUSDT',groq='WAIT',min_rr=2,max_rr=3)\n        self.assertEqual(data['pagination']['total'],1)\n        self.assertEqual(data['rows'][0]['rr_value'],2.2)\n        self.assertEqual(data['rows'][0]['groq_review']['decision'],'WAIT')\n\n\nif __name__=='__main__':\n    unittest.main()\n''',encoding='utf-8')


def main():
    patch_setup_audit(); patch_gitignore(); patch_telegram(); patch_stats_server(); add_tests()
    for rel in ['scripts/refine_strategy_stats.py','.github/workflows/refine_strategy_stats.yml']:
        try:(ROOT/rel).unlink()
        except FileNotFoundError:pass


if __name__=='__main__': main()
