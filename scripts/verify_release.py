"""Verify that a release SHA is explicit and non-empty before controlled release."""
from __future__ import annotations
import argparse,re

def valid_sha(value): return bool(re.fullmatch(r"[0-9a-fA-F]{40}",str(value or "")))
def main():
    p=argparse.ArgumentParser(); p.add_argument("sha"); a=p.parse_args()
    if not valid_sha(a.sha): raise SystemExit("invalid_release_sha")
    print(a.sha.lower())
if __name__=="__main__": main()
