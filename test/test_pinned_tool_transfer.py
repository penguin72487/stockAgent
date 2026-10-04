"""Parallel range assembly must preserve the complete pinned software bytes."""
import hashlib
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
import threading

import pytest

from scripts.download_pinned_tool import download,read_range


@pytest.fixture
def server():
    body=bytes(range(256))*16
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_HEAD(self):
            self.send_response(200);self.send_header('Content-Length',str(len(body)))
            self.send_header('ETag','"fixed-tool"');self.end_headers()
        def do_GET(self):
            start,end=map(int,self.headers['Range'].removeprefix('bytes=').split('-'))
            part=body[start:end+1]
            self.send_response(206);self.send_header('Content-Range',f'bytes {start}-{end}/{len(body)}')
            self.send_header('ETag','"fixed-tool"');self.send_header('Content-Length',str(len(part)))
            self.end_headers();self.wfile.write(part)
    http=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=http.serve_forever,daemon=True);thread.start()
    try:yield f'http://127.0.0.1:{http.server_port}/tool',body
    finally:http.shutdown();http.server_close();thread.join(timeout=2)


def test_parallel_tool_transfer_matches_the_whole_pinned_hash(server,tmp_path):
    url,body=server;path=tmp_path/'tool'
    proof=download(url,hashlib.sha256(body).hexdigest(),path,allow_test_http=True)
    assert path.read_bytes()==body and proof['bytes']==len(body)
    assert len(proof['transfer_probes'])==4
    assert path.with_suffix('.download.json').is_file()


def test_corrupt_hash_never_publishes_a_tool(server,tmp_path):
    url,_=server;path=tmp_path/'tool'
    with pytest.raises(ValueError,match='complete official'):
        download(url,'0'*64,path,allow_test_http=True)
    assert not path.exists()


def test_existing_software_path_keeps_its_owner(server,tmp_path):
    url,body=server;path=tmp_path/'tool';path.write_bytes(b'old-owner')
    with pytest.raises(ValueError,match='preserve'):
        download(url,hashlib.sha256(body).hexdigest(),path,allow_test_http=True)
    assert path.read_bytes()==b'old-owner'
