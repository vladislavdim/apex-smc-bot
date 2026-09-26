"""Explicit V3 migration entry point.
Migrations remain separated by State and Memory stores and are never run implicitly by UI code.
"""
from __future__ import annotations
import argparse
from apex.db.migrations import migrate_memory,migrate_state

def main():
    p=argparse.ArgumentParser();p.add_argument("store",choices=("state","memory"));p.add_argument("path");a=p.parse_args()
    (migrate_state if a.store=="state" else migrate_memory)(a.path)
if __name__=="__main__": main()
