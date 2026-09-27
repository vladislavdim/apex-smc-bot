"""Explicit V3 migration entry point.
Schema creation is delegated to canonical State/Memory stores; no legacy trade
state is silently promoted by this script.
"""
from __future__ import annotations
import argparse
from apex.db.connection import state_connection,memory_connection
from apex.db.integrity import check_integrity

def main():
    p=argparse.ArgumentParser(); p.add_argument("--state",required=True); p.add_argument("--memory",required=True); a=p.parse_args()
    for factory,path in ((state_connection,a.state),(memory_connection,a.memory)):
        conn=factory(path); check_integrity(conn); conn.close()
    print("v3_stores: OK")
if __name__=="__main__": main()
