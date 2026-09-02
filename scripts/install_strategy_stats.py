"""One-time source installer for passive APEX strategy statistics."""
from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKER = "APEX_STRATEGY_STATS_V1"


def _line_offsets(source: str) -> list[int]:
    offsets = [0]
    for match in re.finditer("\n", source):
        offsets.append(match.end())
    return offsets


def _abs(offsets: list[int], line: int, col: int) -> int:
    return offsets[line - 1] + col


def _nearest_comment(lines: list[str], lineno: int, condition: str = "") -> str:
    for index in range(lineno - 2, max(-1, lineno - 14), -1):
        if index < 0:
            break
        text = lines[index].strip()
        if not text:
            continue
        if text.startswith("#"):
            label = re.sub(r"^[#─═\s]+|[#─═\s]+$", "", text).strip()
            if label:
                return label[:260]
        if index < lineno - 5 and not text.startswith(("if ", "elif ", "try:", "except")):
            break
    clean = " ".join(condition.split())
    return clean[:260] or "detector returned None"


def _nearest_if(node: ast.AST, parents: dict[ast.AST, ast.AST]) -> ast.If | None:
    cur = parents.get(node)
    while cur is not None:
        if isinstance(cur, ast.If):
            return cur
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            break
        cur = parents.get(cur)
    return None


def _belongs(node: ast.AST, target: ast.FunctionDef, parents: dict[ast.AST, ast.AST]) -> bool:
    cur: ast.AST | None = node
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return cur is target
        cur = parents.get(cur)
    return False


def _contains_return_none(statements: list[ast.stmt]) -> bool:
    class Finder(ast.NodeVisitor):
        found = False
        def visit_FunctionDef(self, node): return
        def visit_AsyncFunctionDef(self, node): return
        def visit_Lambda(self, node): return
        def visit_Return(self, node):
            if node.value is None or (isinstance(node.value, ast.Constant) and node.value.value is None):
                self.found = True
    finder = Finder()
    for stmt in statements:
        finder.visit(stmt)
    return finder.found


def _insert_import(source: str) -> str:
    if "from core.setup_audit import audit_strategy as _audit_strategy" in source:
        return source
    tree = ast.parse(source); lines = source.splitlines(keepends=True); insert_line = 1
    if tree.body and isinstance(tree.body[0], ast.Expr) and isinstance(getattr(tree.body[0], "value", None), ast.Constant) and isinstance(tree.body[0].value.value, str):
        insert_line = (tree.body[0].end_lineno or tree.body[0].lineno) + 1
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module == "__future__":
            insert_line = max(insert_line, (node.end_lineno or node.lineno) + 1)
    lines.insert(insert_line - 1, f"# {MARKER}\nfrom core.setup_audit import audit_strategy as _audit_strategy, audit_test as _audit_test, audit_fail as _audit_fail\n")
    return "".join(lines)


def instrument_file(path: Path, targets: dict[str, tuple[str, str]]) -> None:
    source = path.read_text(encoding="utf-8")
    if f"# {MARKER}" not in source:
        source = _insert_import(source)
    tree = ast.parse(source); parents = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent): parents[child] = parent
    functions = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    lines = source.splitlines(); offsets = _line_offsets(source); edits = []
    for fn_name, (strategy, subtype) in targets.items():
        fn = functions.get(fn_name)
        if fn is None: raise RuntimeError(f"{path}: target function not found: {fn_name}")
        existing = [ast.get_source_segment(source, d) or "" for d in fn.decorator_list]
        if not any("_audit_strategy" in d for d in existing):
            dec = f'@_audit_strategy("{strategy}"{", subtype=" + repr(subtype) if subtype else ""})\n'
            pos = _abs(offsets, fn.lineno, fn.col_offset); edits.append((pos, pos, " " * fn.col_offset + dec))
        prefix = re.sub(r"[^A-Z0-9]+", "_", f"{strategy}_{fn_name}".upper()).strip("_")
        for node in ast.walk(fn):
            if not isinstance(node, ast.If) or not _belongs(node, fn, parents) or not _contains_return_none(node.body): continue
            segment = ast.get_source_segment(source, node.test)
            if not segment or "_audit_test(" in segment: continue
            label = _nearest_comment(lines, node.lineno, segment); code = f"{prefix}_G{node.lineno}"
            replacement = f'_audit_test({code!r}, ({segment}), {label!r}, {(" ".join(segment.split()))!r}, {node.lineno})'
            start = _abs(offsets, node.test.lineno, node.test.col_offset); end = _abs(offsets, node.test.end_lineno or node.test.lineno, node.test.end_col_offset or node.test.col_offset)
            edits.append((start, end, replacement))
        for node in ast.walk(fn):
            if not isinstance(node, ast.Return) or not _belongs(node, fn, parents): continue
            if not (node.value is None or (isinstance(node.value, ast.Constant) and node.value.value is None)): continue
            ifnode = _nearest_if(node, parents); condition = ast.get_source_segment(source, ifnode.test) if ifnode is not None else ""
            label = _nearest_comment(lines, node.lineno, condition or ""); code = f"{prefix}_R{node.lineno}"
            replacement = f'return _audit_fail({code!r}, {label!r}, locals(), {(" ".join((condition or "").split()))!r}, {node.lineno})'
            start = _abs(offsets, node.lineno, node.col_offset); end = _abs(offsets, node.end_lineno or node.lineno, node.end_col_offset or node.col_offset)
            edits.append((start, end, replacement))
    for start, end, replacement in sorted(edits, key=lambda x: (x[0], x[1]), reverse=True): source = source[:start] + replacement + source[end:]
    ast.parse(source); path.write_text(source, encoding="utf-8")


def patch_telegram_dashboard(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    if "import os\n" not in text: text = text.replace("import json\n", "import json\nimport os\n", 1)
    old = '''    reasons = data.get("reasons", [])
    if reasons:
        lines.extend(["", "<b>Главные причины за 24ч</b>"])
        for row in reasons[:8]:
            lines.append(
                f"• {html.escape(str(row.get('strategy') or '—'))}: "
                f"{html.escape(str(row.get('reason_code') or 'UNSPECIFIED'))} ×{int(row.get('count') or 0)}"
            )
'''
    if old in text: text = text.replace(old, "", 1)
    elif "Главные причины за 24ч" in text: raise RuntimeError("telegram dashboard reasons block changed")
    if "APEX_STATS_URL" not in text:
        needle = '    lines.extend(["", "<i>Панель показывает фактические проходы, а не расписание.</i>"])\n'
        if needle not in text: raise RuntimeError("telegram dashboard footer missing")
        insert = '    stats_url = os.environ.get("APEX_STATS_URL", "").strip()\n    if stats_url:\n        safe_url = html.escape(stats_url, quote=True)\n        lines.extend(["", f\'<a href="{safe_url}">📊 Полная статистика</a>\'])\n' + needle
        text = text.replace(needle, insert, 1)
    ast.parse(text); path.write_text(text, encoding="utf-8")


def _insert_after_docstring(source: str, function_name: str, insertion: str) -> str:
    tree = ast.parse(source); fn = next((n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == function_name), None)
    if fn is None: raise RuntimeError(f"function not found: {function_name}")
    first = fn.body[0]
    line = (first.end_lineno or first.lineno) + 1 if isinstance(first, ast.Expr) and isinstance(getattr(first, "value", None), ast.Constant) and isinstance(first.value.value, str) else first.lineno
    lines = source.splitlines(keepends=True); indent = " " * (fn.col_offset + 4); block = "".join(indent + piece + "\n" for piece in insertion.splitlines()); lines.insert(line - 1, block)
    out = "".join(lines); ast.parse(out); return out


def patch_strategy_decisions(path: Path) -> None:
    text = path.read_text(encoding="utf-8"); imp = "from core.setup_audit import emit_decision_event as _emit_setup_audit_decision\n"
    if imp not in text: text = text.replace("from typing import Any\n", "from typing import Any\n" + imp, 1)
    if "_emit_setup_audit_decision(candidate, outcome, stage, reason" not in text:
        text = _insert_after_docstring(text, "record_strategy_decision", "try:\n    _emit_setup_audit_decision(candidate, outcome, stage, reason, evidence)\nexcept Exception:\n    pass")
    path.write_text(text, encoding="utf-8")


def patch_signal_quality_gate(path: Path) -> None:
    text = path.read_text(encoding="utf-8"); imp = "from core.setup_audit import emit_groq_review_event as _emit_setup_audit_groq\n"
    anchor = "from core.setup_evidence import assess_candidate, persist_assessment\n"
    if imp not in text:
        if anchor not in text: raise RuntimeError("quality gate import anchor missing")
        text = text.replace(anchor, anchor + imp, 1)
    if "_emit_setup_audit_groq, candidate, review" not in text:
        needle = "    await asyncio.to_thread(_persist_review, candidate, context, news, memory, zones, learning, review)\n"
        if needle not in text: raise RuntimeError("quality gate persist anchor missing")
        text = text.replace(needle, needle + "    await asyncio.to_thread(_emit_setup_audit_groq, candidate, review)\n", 1)
    ast.parse(text); path.write_text(text, encoding="utf-8")


def patch_control_loop(path: Path) -> None:
    text = path.read_text(encoding="utf-8"); imp = "from core.setup_audit import emit_scan_event as _emit_setup_audit_scan\n"
    if imp not in text: text = text.replace("from typing import Any\n", "from typing import Any\n" + imp, 1)
    if "_emit_setup_audit_scan(run_id, strategy, symbol, stage, outcome" not in text:
        text = _insert_after_docstring(text, "record_scan_event", "try:\n    _emit_setup_audit_scan(run_id, strategy, symbol, stage, outcome, reason_code, detail)\nexcept Exception:\n    pass")
    text = text.replace("DELETE FROM scan_pair_events WHERE created_at < datetime('now','-14 days')", "DELETE FROM scan_pair_events WHERE created_at < datetime('now','-90 days')")
    ast.parse(text); path.write_text(text, encoding="utf-8")


def patch_requirements(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    if "psycopg2-binary" not in text: text += ("" if text.endswith("\n") else "\n") + "psycopg2-binary==2.9.10\n"
    path.write_text(text, encoding="utf-8")


def main() -> None:
    instrument_file(ROOT / "market.py", {"detect_fast_deal": ("FAST", ""), "detect_swing_setup": ("SWING", ""), "detect_zone_setup": ("ZONE", ""), "detect_wyckoff_spring": ("WYCKOFF", "SPRING"), "detect_wyckoff_distribution": ("WYCKOFF", "DISTRIBUTION"), "detect_wyckoff_reaccumulation": ("WYCKOFF", "REACCUMULATION")})
    instrument_file(ROOT / "bot.py", {"full_scan_raw": ("MTF", "")})
    patch_telegram_dashboard(ROOT / "core" / "telegram_dashboard.py")
    patch_strategy_decisions(ROOT / "core" / "strategy_decisions.py")
    patch_signal_quality_gate(ROOT / "core" / "signal_quality_gate.py")
    patch_control_loop(ROOT / "core" / "control_loop.py")
    patch_requirements(ROOT / "requirements.txt")
    for rel in ["market.py", "bot.py", "core/telegram_dashboard.py", "core/strategy_decisions.py", "core/signal_quality_gate.py", "core/control_loop.py", "core/setup_audit.py", "core/strategy_catalog.py", "stats_server.py"]:
        ast.parse((ROOT / rel).read_text(encoding="utf-8"), filename=rel)
    for rel in ["scripts/install_strategy_stats.py", ".github/workflows/install_strategy_stats.yml"]:
        try: (ROOT / rel).unlink()
        except FileNotFoundError: pass


if __name__ == "__main__": main()
