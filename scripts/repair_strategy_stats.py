from __future__ import annotations
import ast, re, subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]


def offsets(src):
    out=[0]
    for m in re.finditer('\n',src): out.append(m.end())
    return out

def pos(o,n): return o[n.lineno-1]+n.col_offset, o[(n.end_lineno or n.lineno)-1]+(n.end_col_offset or n.col_offset)
def is_name_call(node,name): return isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and node.func.id==name


def repair(path):
    src=path.read_text(encoding='utf-8'); tree=ast.parse(src); o=offsets(src); edits=[]
    for node in ast.walk(tree):
        if not isinstance(node,ast.If) or not is_name_call(node.test,'_audit_test'): continue
        direct_fail=any(isinstance(st,ast.Return) and is_name_call(st.value,'_audit_fail') for st in node.body)
        if direct_fail: continue
        if len(node.test.args)<2: raise RuntimeError(f'bad audit wrapper at {path}:{node.lineno}')
        original=ast.get_source_segment(src,node.test.args[1])
        if not original: raise RuntimeError(f'cannot recover condition at {path}:{node.lineno}')
        a,b=pos(o,node.test); edits.append((a,b,original))
    for a,b,r in sorted(edits,reverse=True): src=src[:a]+r+src[b:]
    ast.parse(src); path.write_text(src,encoding='utf-8'); return len(edits)


class StripAudit(ast.NodeTransformer):
    def visit_ImportFrom(self,node):
        if node.module=='core.setup_audit': return None
        return self.generic_visit(node)
    def visit_FunctionDef(self,node):
        self.generic_visit(node)
        node.decorator_list=[d for d in node.decorator_list if not (isinstance(d,ast.Call) and isinstance(d.func,ast.Name) and d.func.id=='_audit_strategy')]
        return node
    def visit_Call(self,node):
        node=self.generic_visit(node)
        if is_name_call(node,'_audit_test') and len(node.args)>=2: return node.args[1]
        if is_name_call(node,'_audit_fail'): return ast.Constant(value=None)
        return node


def semantic_dump(text):
    tree=ast.parse(text); tree=StripAudit().visit(tree); ast.fix_missing_locations(tree); return ast.dump(tree,include_attributes=False)


def assert_trading_semantics_same(rel):
    current=(ROOT/rel).read_text(encoding='utf-8')
    base=subprocess.check_output(['git','show',f'origin/main:{rel}'],cwd=ROOT,text=True)
    if semantic_dump(current)!=semantic_dump(base):
        raise RuntimeError(f'{rel}: semantic AST differs from main after stripping audit wrappers')


def main():
    print('unwrapped',repair(ROOT/'market.py'),'non-blocking branch predicates in market.py')
    print('unwrapped',repair(ROOT/'bot.py'),'non-blocking branch predicates in bot.py')
    assert_trading_semantics_same('market.py'); assert_trading_semantics_same('bot.py')
    for rel in ['scripts/repair_strategy_stats.py','.github/workflows/repair_strategy_stats.yml']:
        try:(ROOT/rel).unlink()
        except FileNotFoundError:pass
if __name__=='__main__': main()
