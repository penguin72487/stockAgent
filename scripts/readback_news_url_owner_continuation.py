"""Independently read back the private URL inbox continuation benchmark."""
import argparse
from pathlib import Path
import hashlib
import json
import sqlite3

from downloader.news_fulltext_queue import verify_bodies

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--input', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
repo = Path(__file__).resolve().parents[1]
destination = args.output.resolve()
if any(destination.is_relative_to(repo / name) for name in ('data_news', 'data_news_fulltext')):
    raise ValueError('readback_output_must_not_be_in_a_production_news_root')
input_bytes = args.input.read_bytes()
measured = json.loads(input_bytes)
flags = [x['continuation_enabled'] for x in measured['runs']]
orders = {'abba': [False, True, True, False], 'baab': [True, False, False, True]}
assert flags in orders.values() and (measured.get('order') is None or orders.get(measured['order']) == flags)
readbacks = []
for run in measured['runs']:
    root = Path(run['private_root'])
    assert root.is_relative_to('/var/tmp') and root.is_dir()
    with sqlite3.connect(f'file:{root / "queue.sqlite3"}?mode=ro', uri=True) as db:
        heads = db.execute("SELECT url,state,attempts FROM articles WHERE provider='staged' ORDER BY url").fetchall()
        items = db.execute('SELECT hex(article_id),provider,outcome FROM url_metadata_items ORDER BY 1,2,3').fetchall()
        # The foreground's immutable body is verified independently of the metadata inbox.
        foreground = db.execute("SELECT state,body_sha256 FROM articles WHERE url='https://publisher.test/story'").fetchone()
    assert len(heads) == len(items) == 772 and foreground and foreground[1]
    assert all(state == 'awaiting_authorization' and attempts == 0 for _, state, attempts in heads)
    assert all(provider == 'staged' and outcome == 'metadata_registered' for _, provider, outcome in items)
    completed = root / 'reviewed_repairs/url_metadata/completed'
    receipts = sorted(completed.glob('*.receipt.json'))
    assert len(receipts) == 2
    total_targets = 0
    for path in receipts:
        receipt = json.loads(path.read_text())
        request = completed / (receipt['request_id'] + '.request.json')
        assert hashlib.sha256(request.read_bytes()).hexdigest() == receipt['request_id']
        assert receipt['outcomes'] == {'metadata_registered': receipt['targets']}
        total_targets += receipt['targets']
    assert total_targets == 772
    verification = verify_bodies(root)
    assert verification['accepted']
    def digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    readbacks.append({'heads': len(heads), 'items': len(items), 'receipts': len(receipts),
                      'heads_sha256': digest(heads), 'items_sha256': digest(items),
                      'foreground_body_sha256': foreground[1], 'body_verification_accepted': True})
for key in ('heads_sha256', 'items_sha256', 'foreground_body_sha256'):
    assert len({row[key] for row in readbacks}) == 1
result = {'contract': 'news_url_continuation_private_abba_independent_readback_v1',
          'accepted': True, 'input_sha256': hashlib.sha256(input_bytes).hexdigest(),
          'runs': readbacks, 'publisher_requests': 0, 'production_queue_writes': 0}
with destination.open('x', encoding='utf-8') as stream:
    stream.write(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
