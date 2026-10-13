"""Publisher-declared public Anue index, distinct from bulk article bodies.

The category API has an observed thirty-page limit. The separately published
all-list advertises and serves later pages, but contains summaries, not bodies.
Use one list per actual overflowing source date; preserve the original gaps
and use the existing article queue for every newly discovered public ID.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import html
import json
import math
import re
import time
from urllib.parse import parse_qs, urlencode, urlsplit

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_policy import NewsError


CONTRACT_VERSION = 1
MANIFEST_SCHEDULE_CONTRACT_VERSION = 1
MANIFEST_PHASE_CONTRACT_VERSION = 1
ENDPOINT = 'https://api.cnyes.com/media/api/v1/newslist/all'
MAXIMUM_PAGE = 100_000  # Engineering bound, not an official pagination limit.
MANIFEST_PRIORITY_BAND = -10_000_000
FIELDS = frozenset({
    'abTesting', 'categoryId', 'columnists', 'coverSrc', 'hasCoverPhoto',
    'isCategoryHeadline', 'isIndex', 'newsId', 'payment', 'publishAt', 'summary', 'title',
})


def clock():
    return datetime.now(timezone.utc).isoformat()


def reviewed_index(entry, provider, authorization):
    """Keep this metadata ABI independent of every existing body/cache ABI."""
    config = entry.get('cnyes_public_index')
    if config is None:
        return '', ''
    if not isinstance(config, dict):
        raise ValueError('invalid reviewed Cnyes public index')
    navigation = config.get('publisher_navigation', {})
    pages = config.get('observed_pages', [])
    if (provider != 'cnyes' or config.get('contract_version') != CONTRACT_VERSION
            or config.get('endpoint') != ENDPOINT or config.get('observed_page_size') != 30
            or config.get('seed_scope') != 'actual_single_source_day_category_gaps'
            or not isinstance(navigation, dict)
            or not re.fullmatch(r'https://news[.]cnyes[.]com/_next/static/chunks/[A-Za-z0-9._-]+[.]js', navigation.get('url',''))
            or not re.fullmatch(r'[0-9a-f]{64}', navigation.get('response_sha256',''))
            or not isinstance(pages, list) or len(pages) != 3
            or [row.get('page') for row in pages if isinstance(row, dict)] != [1, 2, 31]
            or any(type(row.get('page')) is not int or not re.fullmatch(r'[0-9a-f]{64}', row.get('response_sha256','')) for row in pages)):
        raise ValueError('unreviewed Cnyes public index endpoint/navigation/pagination')
    payload = {'version':CONTRACT_VERSION, 'config':config, 'authorization':authorization,
               'purpose':'private_public_index_metadata_not_article_or_training_acceptance'}
    return ENDPOINT, hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def is_index_task(task):
    return task['kind'] == 'index' and task['url'].split('?',1)[0] == ENDPOINT


def request_url(source, day, page=1):
    if (getattr(source, 'cnyes_index_api', '') != ENDPOINT or type(day) is not int
            or type(page) is not int or not 1 <= page <= MAXIMUM_PAGE):
        raise ValueError('unregistered or unbounded public Cnyes index request')
    low = day*86400 - 28800
    return ENDPOINT+'?'+urlencode({'limit':30, 'page':page, 'startAt':low, 'endAt':low+86399})


def discovery_priority(day, page=1):
    """Observe every actual gap date before deep metadata-only pagination.

    This ordering applies only inside the existing index lane. The shared
    selector still serves current feeds and efficient body batches first, and
    its article fairness remains unchanged. A first page reveals source-reported
    work, not unique articles, accepted bodies, or a complete historical scope.
    """
    if type(day) is not int or type(page) is not int or not 1 <= page <= MAXIMUM_PAGE:
        raise ValueError('invalid bounded Cnyes index priority')
    return -day * 100 + (MANIFEST_PRIORITY_BAND if page == 1 else 0)


def validate_request(url, source):
    query = parse_qs(urlsplit(url).query, keep_blank_values=True)
    try:
        if (getattr(source, 'cnyes_index_api', '') != ENDPOINT or url.split('?',1)[0] != ENDPOINT
                or set(query) != {'limit','page','startAt','endAt'} or any(len(v) != 1 for v in query.values())
                or query['limit'] != ['30']):
            raise ValueError('unregistered parameters')
        page, low, high = (int(query[k][0]) for k in ('page','startAt','endAt'))
        day = (low+28800)//86400
        first = int(datetime(1999,1,1,tzinfo=timezone.utc).timestamp())-28800
        today = int(time.time())+86400
        if (low < first or high > today or low != day*86400-28800 or high != low+86399
                or not 1 <= page <= MAXIMUM_PAGE or url != request_url(source,day,page)):
            raise ValueError('unbounded or noncanonical date/page')
        return day, page, low, high
    except (ValueError, KeyError, TypeError) as error:
        raise NewsError('cnyes_index_request_unregistered') from error


def initialize(queue):
    queue.db.executescript('''
        CREATE TABLE IF NOT EXISTS cnyes_public_index_observations(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            index_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            total INTEGER NOT NULL,pages INTEGER NOT NULL,ids_sha256 TEXT NOT NULL,
            metadata_rows INTEGER NOT NULL,public_ids INTEGER NOT NULL,
            evidence_path TEXT NOT NULL,source_observed_at_utc TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS cnyes_public_index_heads(
            scope_url TEXT NOT NULL,page INTEGER NOT NULL,response_sha256 TEXT NOT NULL,
            index_fingerprint TEXT NOT NULL,authorization_fingerprint TEXT NOT NULL,
            PRIMARY KEY(scope_url,page,index_fingerprint,authorization_fingerprint)
        ) WITHOUT ROWID;
    ''')


def gap_dates(db, source):
    """The same real source-day set owns scheduling and its phase denominator."""
    days = set()
    from downloader.news_fulltext_cnyes_history import scope_key
    for row in db.execute("SELECT scope_url FROM bulk_window_gaps WHERE provider=?", (source.provider,)):
        key = scope_key(row[0])
        if key and key[0] in source.cnyes_apis and key[1] == key[2]:
            days.add(key[1])
    return days


def seed_gap_dates(queue, source):
    if not getattr(source,'cnyes_index_api','') or not source.allowed:
        return
    # Existing category work changes only its actual gap table. A bounded
    # once/minute read avoids scanning 1,518+ rows on every owner cycle.
    epoch = time.monotonic()
    if epoch < getattr(queue,'_cnyes_index_seed_after',0):
        return
    days = gap_dates(queue.db, source)
    # Existing first-page jobs migrate only their ordering. In particular, do
    # not reset successful deadlines, source errors, attempts, ETags, raw hashes
    # or clocks just because the scheduling policy changed. Subsequent seeds
    # have no changed rows; unrelated/foreign work at a colliding URL is retained.
    queue.db.executemany("INSERT INTO discovery(url,kind,provider,priority) VALUES (?,'index',?,?) "
                        "ON CONFLICT(url) DO UPDATE SET priority=excluded.priority "
                        "WHERE discovery.provider=excluded.provider AND discovery.kind='index' "
                        "AND discovery.priority!=excluded.priority",
                        ((request_url(source,day), source.provider, discovery_priority(day))
                         for day in sorted(days,reverse=True)))
    queue._cnyes_index_seed_after = epoch+60
    queue.metrics.count(source.provider,'public_index_seed_dates_examined',len(days))


def parse_index(response, source):
    day, page, low, high = validate_request(response.url,source)
    if response.status != 200:
        raise NewsError('cnyes_index_response_not_200',retry=True,delay=60)
    from downloader.news_fulltext_bulk import cnyes_pagination
    info, posts = cnyes_pagination(response,maximum_page=MAXIMUM_PAGE)
    if info['pages'] > MAXIMUM_PAGE:
        raise NewsError('cnyes_index_reported_pages_over_engineering_bound')
    links, seen = [], set()
    for post in posts:
        # This adapter never retains an unreviewed body field. A schema change
        # must be reviewed instead of accidentally archiving paid full text.
        if set(post)-FIELDS:
            raise NewsError('cnyes_index_unreviewed_metadata_fields',retry=True,delay=3600)
        if (type(post.get('publishAt')) is not int or not low <= post['publishAt'] <= high
                or type(post.get('categoryId')) is not int or post['categoryId'] <= 0
                or type(post.get('payment')) is not int or post['payment'] not in {0,1}
                or not isinstance(post.get('title'),str) or not post['title'].strip()):
            raise NewsError('cnyes_index_unverified_metadata',retry=True,delay=3600)
        identifier = post['newsId']
        if identifier in seen:
            continue
        seen.add(identifier)
        if post['payment'] == 0:
            # This is the existing publisher permalink contract using a real
            # source ID, never an invented ID or a guessed historical value.
            links.append(('https://news.cnyes.com/news/id/'+str(identifier),
                          html.unescape(post['title']),
                          datetime.fromtimestamp(post['publishAt'],timezone.utc).isoformat()))
    info.update(source_day=day,public_ids=len(links),metadata_rows=len(posts),
                article_bodies_accepted=0,source_total_is_not_unique_story_count=True)
    return info, posts, links


def accept_index(queue, task, response, links, source):
    day, page, _, _ = validate_request(task['url'],source)
    if response.url != task['url']:
        raise NewsError('cnyes_index_request_response_scope_mismatch',retry=True,delay=60)
    fingerprint = source.cnyes_index_data_fingerprint
    if response.status == 304:
        # A conditional response cannot supply lost list evidence or add URLs.
        from downloader.news_fulltext_bulk import cnyes_scope
        if task.get('parser_fingerprint') != fingerprint or not queue.db.execute(
                'SELECT 1 FROM cnyes_public_index_observations WHERE response_sha256=? AND index_fingerprint=? '
                'AND scope_url=? AND page=? AND authorization_fingerprint=?',
                (task.get('response_sha256'),fingerprint,cnyes_scope(task['url']),page,source.authorization_fingerprint)).fetchone():
            raise NewsError('cnyes_index_not_modified_without_evidence',retry=True,delay=60)
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=?,last_checked_at_utc=? WHERE url=?',
                             (time.time()+365*86400,clock(),task['url']))
        return 0
    info, posts, actual_links = parse_index(response,source)
    if links != actual_links:
        raise NewsError('cnyes_index_worker_metadata_mismatch',retry=True,delay=60)
    sha = hashlib.sha256(response.body).hexdigest()
    previous = queue.db.execute('SELECT page FROM cnyes_public_index_observations WHERE scope_url=? '
        'AND total=? AND ids_sha256=? AND page!=? AND index_fingerprint=? AND authorization_fingerprint=? LIMIT 1',
        (info['scope'],info['total'],info['ids_sha256'],info['page'],fingerprint,source.authorization_fingerprint)).fetchone()
    if posts and previous:
        raise NewsError('cnyes_index_pagination_no_progress',retry=True,delay=3600)
    context_sha = hashlib.sha256(response.url.encode()).hexdigest()
    relative = 'list_metadata/cnyes/'+fingerprint+'/'+context_sha+'-'+sha+'.parquet'
    target = queue.root/relative
    receipt = queue.root/(relative+'.receipt.json')
    if not target.exists():
        atomic_write_parquet(target,table_from_records([{
            'request_url':response.url,'response_sha256':sha,'raw_response_body':response.body,
            'source_observed_at_utc':clock(),'index_fingerprint':fingerprint,
            'authorization_fingerprint':source.authorization_fingerprint,
            'metadata_only':True,'accepted_body_count':0,'publish':False,
        }]),compression='zstd',compression_level=9,durable=True)
    # Recover a crash between atomic Parquet and receipt commits from exact
    # original bytes, request context, authorization and metadata-only flags.
    # Never rewrite the original file or its observation clock.
    from pyarrow import parquet as pq
    retained = pq.read_table(target).to_pylist()
    if len(retained) != 1 or any(retained[0].get(key) != value for key,value in {
            'request_url':response.url,'response_sha256':sha,'raw_response_body':response.body,
            'index_fingerprint':fingerprint,'authorization_fingerprint':source.authorization_fingerprint,
            'metadata_only':True,'accepted_body_count':0,'publish':False}.items()):
        raise NewsError('cnyes_index_original_evidence_unverified',retry=True,delay=60)
    receipt_data = {'parquet_path':relative,'sha256':sha256_file(target),'response_sha256':sha,
        'index_fingerprint':fingerprint,'metadata_rows':len(posts),'accepted_body_count':0,
        'metadata_only':True,'publish':False}
    if receipt.exists():
        original_receipt = json.loads(receipt.read_bytes())
        if any(original_receipt.get(key) != value for key,value in receipt_data.items()):
            raise NewsError('cnyes_index_original_receipt_unverified',retry=True,delay=60)
    else:
        atomic_write_json(receipt,receipt_data,durable=True)
    observed = clock()
    with queue.db:
        queue.add_articles([{'url':url,'title':title,'date':date} for url,title,date in actual_links])
        queue.db.execute('INSERT OR IGNORE INTO cnyes_public_index_observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (info['scope'],info['page'],sha,fingerprint,source.authorization_fingerprint,info['total'],info['pages'],
             info['ids_sha256'],len(posts),len(actual_links),relative,observed))
        queue.db.execute('INSERT OR REPLACE INTO cnyes_public_index_heads VALUES (?,?,?,?,?)',
            (info['scope'],info['page'],sha,fingerprint,source.authorization_fingerprint))
        if info['page'] < info['pages']:
            url = request_url(source,info['source_day'],info['page']+1)
            queue.db.execute("INSERT OR IGNORE INTO discovery(url,kind,provider,priority) VALUES (?,'index',?,?)",
                             (url,source.provider,discovery_priority(info['source_day'],info['page']+1)))
        queue.db.execute('UPDATE discovery SET due=?,etag=?,last_modified=?,response_sha256=?,parser_fingerprint=?,'
                         'last_checked_at_utc=?,attempts=0,error_code=NULL WHERE url=?',
            (time.time()+365*86400,response.headers.get('etag'),response.headers.get('last-modified'),sha,
             fingerprint,observed,task['url']))
        queue.db.execute('DELETE FROM bulk_endpoint_cooldowns WHERE provider=? AND endpoint=? AND data_fingerprint=?',
                         (source.provider,ENDPOINT,fingerprint))
    queue.metrics.count(source.provider,'public_index_pages_verified')
    queue.metrics.count(source.provider,'public_index_metadata_rows_verified',len(posts))
    queue.metrics.count(source.provider,'public_index_public_article_ids',len(actual_links))
    return 0  # Not an article-body acceptance, even when new URLs were added.


def manifest_phase(db, source, rows):
    """Only actual overflowing source dates, never the whole article history.

    Each denominator comes from its own accepted first page under the current
    metadata/authorization ABI. Missing first pages or changed source totals
    suppress totals instead of extrapolating observed days to unseen days.
    """
    from downloader.news_fulltext_bulk import cnyes_scope
    scopes = {cnyes_scope(request_url(source, day)) for day in gap_dates(db, source)}
    selected = [row for row in rows if row[0] in scopes]
    firsts = sum(row[8] is not None and row[9] is not None for row in selected)
    pages, metadata = sum(row[1] for row in selected), sum(row[6] for row in selected)
    changed = sum(row[2] != row[3] or row[4] != row[5] for row in selected)
    known = bool(scopes) and firsts == len(scopes) and not changed
    total_pages = sum(row[9] for row in selected) if known else None
    total_rows = sum(row[8] for row in selected) if known else None
    known = known and total_pages >= pages and total_rows >= metadata
    if not known:
        total_pages = total_rows = None
    return {'contract_version': MANIFEST_PHASE_CONTRACT_VERSION,
        'scope': 'actual_category_overflow_source_dates',
        'scope_fingerprint': hashlib.sha256(json.dumps(sorted(scopes)).encode()).hexdigest(),
        'planned_source_dates': len(scopes), 'first_pages_observed': firsts,
        'first_pages_remaining': len(scopes) - firsts, 'totals_known': known,
        'source_reported_total_pages': total_pages, 'source_reported_total_metadata_rows': total_rows,
        'verified_pages': pages, 'observed_metadata_rows': metadata,
        'remaining_pages': total_pages - pages if known else None,
        'progress_ratio': pages / total_pages if known and total_pages else None,
        'dates_with_changed_source_total': changed,
        'phase_complete': bool(known and pages == total_pages and metadata == total_rows),
        'metadata_only': True, 'body_completion_claimed': False,
        'all_history_complete': False}, scopes


def manifest_forecast(db, source, phase, scopes, *, owner_start_epoch, now_epoch, activity):
    """Recent durable page heads / whole sixty-second wall bins, including zero.

    This is a conditional metadata-phase scenario, not a statistical confidence
    interval, publisher quota, unique-story count, or final full-text deadline.
    Current-owner wall time prevents borrowing a previous owner's faster work.
    No queue writes, request, retained raw read, or page-view sampling occurs.
    """
    result = {'contract_version': MANIFEST_PHASE_CONTRACT_VERSION, 'state': 'warming',
        'sample_seconds': None, 'sample_count': None, 'verified_pages': None,
        'pages_per_second': None, 'fastest_seconds': None, 'middle_seconds': None,
        'slowest_seconds': None, 'middle_finish_at_utc': None,
        'observed_at_utc': datetime.fromtimestamp(now_epoch, timezone.utc).isoformat(),
        'scenario_not_confidence_interval': True, 'metadata_only': True,
        'all_history_complete': False}
    if not phase['totals_known']:
        return {**result, 'state': 'unknown_denominator'}
    if phase['phase_complete']:
        return {**result, 'state': 'phase_complete'}
    if phase['remaining_pages'] == 0:
        return {**result, 'state': 'unavailable'}
    if not source.allowed or activity not in {'running', 'idle', 'waiting', 'inventorying'}:
        return {**result, 'state': 'inactive'}
    if (type(owner_start_epoch) not in {int, float} or not math.isfinite(owner_start_epoch)
            or not math.isfinite(now_epoch) or owner_start_epoch > now_epoch):
        return result
    bin_count = min(5, int((now_epoch - owner_start_epoch) // 60))
    if bin_count < 3:
        return result
    start = now_epoch - bin_count * 60
    bins = [0] * bin_count
    low = datetime.fromtimestamp(start, timezone.utc).isoformat()
    high = datetime.fromtimestamp(now_epoch, timezone.utc).isoformat()
    for scope, observed in db.execute('SELECT o.scope_url,o.source_observed_at_utc '
            'FROM cnyes_public_index_heads h JOIN cnyes_public_index_observations o '
            'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
            'WHERE h.index_fingerprint=? AND h.authorization_fingerprint=? '
            'AND o.source_observed_at_utc>=? AND o.source_observed_at_utc<=?',
            (source.cnyes_index_data_fingerprint, source.authorization_fingerprint, low, high)):
        if scope not in scopes:
            continue
        try:
            stamp = datetime.fromisoformat(observed)
            if stamp.tzinfo is None:
                return {**result, 'state': 'unavailable'}
            seconds = stamp.timestamp() - start
            if not 0 <= seconds <= bin_count * 60:
                return {**result, 'state': 'unavailable'}
        except (ValueError, TypeError, OverflowError):
            return {**result, 'state': 'unavailable'}
        bins[min(bin_count - 1, int(seconds // 60))] += 1
    total = sum(bins)
    result.update(sample_seconds=bin_count * 60, sample_count=bin_count, verified_pages=total,
        window_page_counts=bins)
    if bins[-1] == 0:
        return {**result, 'state': 'no_recent_progress'}
    if total < 10:
        return result
    rate = total / result['sample_seconds']
    remaining = phase['remaining_pages']
    middle = remaining / rate
    result.update(state='recent_measured', pages_per_second=rate,
        fastest_seconds=remaining * 60 / max(bins), middle_seconds=middle,
        slowest_seconds=remaining * 60 / min(bins) if min(bins) else None,
        middle_finish_at_utc=datetime.fromtimestamp(now_epoch + middle, timezone.utc).isoformat())
    return result


def progress(db, source, *, owner_start_epoch=None, now_epoch=None, activity='inactive'):
    if not source or not getattr(source,'cnyes_index_api',''):
        return {'enabled':False,'article_bodies_accepted_by_index':0}
    rows = db.execute('SELECT o.scope_url,count(*),min(o.total),max(o.total),min(o.pages),max(o.pages),'
        'sum(o.metadata_rows),sum(o.public_ids),max(CASE WHEN o.page=1 THEN o.total END),'
        'max(CASE WHEN o.page=1 THEN o.pages END) FROM cnyes_public_index_heads h JOIN cnyes_public_index_observations o '
        'USING(scope_url,page,response_sha256,index_fingerprint,authorization_fingerprint) '
        'WHERE h.index_fingerprint=? AND h.authorization_fingerprint=? GROUP BY o.scope_url',
        (source.cnyes_index_data_fingerprint,source.authorization_fingerprint)).fetchall()
    phase, scopes = manifest_phase(db, source, rows)
    phase['forecast'] = manifest_forecast(db, source, phase, scopes,
        owner_start_epoch=owner_start_epoch, now_epoch=time.time() if now_epoch is None else now_epoch,
        activity=activity)
    return {'contract_version':CONTRACT_VERSION,'enabled':True,'manifest_phase':phase,
        'manifest_schedule_contract_version':MANIFEST_SCHEDULE_CONTRACT_VERSION,'source_dates_observed':len(rows),
        'verified_pages':sum(row[1] for row in rows),'metadata_rows':sum(row[6] for row in rows),
        'public_ID_row_observations':sum(row[7] for row in rows),
        'dates_all_reported_pages_observed':sum(row[2]==row[3] and row[4]==row[5] and row[1]==row[5] for row in rows),
        'dates_with_changed_source_total':sum(row[2]!=row[3] or row[4]!=row[5] for row in rows),
        'article_bodies_accepted_by_index':0,'all_history_complete':False,'finish_eta_utc':None,
        'source_total_is_not_unique_story_count':True,'category_gaps_preserved':True}
