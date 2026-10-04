#!/usr/bin/env python3
"""Measure HTTP Range parallelism for one hash-pinned software distribution.

This is a tool bootstrap helper, not a provider downloader. It never handles
market-data requests or quota. URL, byte ranges and complete SHA-256 are pinned;
probe timings select the current node's connection count, not a global default.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import sys
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from downloader.artifact_io import atomic_write_json


def boundaries(total: int, count: int) -> list[tuple[int,int]]:
    count=min(count,total)
    return [(total*i//count,total*(i+1)//count-1) for i in range(count)]


def read_range(url: str, start: int, end: int, total: int, etag: str) -> bytes:
    request=urllib.request.Request(url,headers={'Range':f'bytes={start}-{end}',
        'If-Range':etag,'User-Agent':'StockAgent-pinned-tool','Accept-Encoding':'identity'})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request,timeout=30) as response:
                if response.status!=206 or response.headers.get('Content-Range')!=f'bytes {start}-{end}/{total}':
                    raise ValueError('the official object did not return the exact requested byte range')
                if response.headers.get('ETag')!=etag:
                    raise ValueError('official object identity changed between range requests')
                body=response.read(end-start+2)
            if len(body)!=end-start+1:
                raise ValueError('official byte range was truncated or oversized')
            return body
        except (OSError,TimeoutError):
            if attempt==2:raise
            time.sleep(.2*(attempt+1))
    raise RuntimeError('unreachable range retry')


def download(url: str, expected: str, output: Path, *, allow_test_http=False) -> dict:
    if not re.fullmatch('[0-9a-f]{64}',expected):raise ValueError('a pinned SHA-256 is required')
    if not (url.startswith('https://github.com/') or allow_test_http and url.startswith('http://127.0.0.1:')):
        raise ValueError('tool bootstrap must use a public official GitHub HTTPS release')
    if output.exists() or output.is_symlink():raise ValueError('preserve the previous downloaded tool')
    started=time.perf_counter()
    with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',
        headers={'User-Agent':'StockAgent-pinned-tool'}),timeout=20) as response:
        total=int(response.headers['Content-Length']);etag=response.headers.get('ETag','')
        actual_url=response.geturl()  # ephemeral signed redirect is never printed or persisted
    if not 0<total<=256*1024**2 or not etag:raise ValueError('bounded tool size and immutable ETag are required')
    probe_size=min(total,2*1024**2);trials=[]
    for count in (1,4,8,16):
        trial_start=time.perf_counter()
        with ThreadPoolExecutor(max_workers=count) as pool:
            bodies=list(pool.map(lambda pair:read_range(actual_url,*pair,total,etag),boundaries(probe_size,count)))
        observed=sum(len(body) for body in bodies)
        seconds=time.perf_counter()-trial_start
        row={'connections':count,'bytes':observed,'complete_seconds':seconds,'bytes_per_second':observed/seconds}
        trials.append(row)
        print(json.dumps({'event':'official_tool_transfer_probe',**row}),flush=True)
    selected=min(trials,key=lambda row:row['complete_seconds'])['connections']
    with ThreadPoolExecutor(max_workers=selected) as pool:
        bodies=list(pool.map(lambda pair:read_range(actual_url,*pair,total,etag),boundaries(total,selected)))
    body=b''.join(bodies)
    if len(body)!=total or hashlib.sha256(body).hexdigest()!=expected:
        raise ValueError('assembled tool does not match the complete official release SHA-256')
    output.parent.mkdir(parents=True,exist_ok=True)
    with output.open('xb') as handle:
        handle.write(body)
    proof={'state':'accepted','url':url,'sha256':expected,'bytes':total,'etag':etag,
           'transfer_probes':trials,'selected_connections':selected,
           'complete_seconds':time.perf_counter()-started,
           'scope':'complete official binary download; probe connection count belongs only to this node/time'}
    atomic_write_json(output.with_suffix(output.suffix+'.download.json'),proof)
    return proof


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',required=True);parser.add_argument('--sha256',required=True)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args();print(json.dumps(download(args.url,args.sha256,args.output)))


if __name__=='__main__':main()
