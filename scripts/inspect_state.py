"""Read-only integrity inspection for the canonical State DB."""
from __future__ import annotations
import argparse,sqlite3
from apex.db.integrity import check_integrity

def main():
    p=argparse.ArgumentParser(); p.add_argument("path"); a=p.parse_args()
    conn=sqlite3.connect(f"file:{a.path}?mode=ro",uri=True)
    try: check_integrity(conn); print("state_db: OK")
    finally: conn.close()
if __name__=="__main__": main()
