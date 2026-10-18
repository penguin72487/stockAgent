"""Native ETtoday original reports; shared HTTP, scheduling and receipts.

Bind the literal response to its own canonical, headline, visible reporter,
pubdate and whole story. Publisher metadata is not a person's byline. A native
bad finance time attribute is retained, not repaired or used as a timestamp.
"""
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import re
from urllib.parse import urljoin, urlsplit

from bs4 import NavigableString, Tag

from downloader.news_fulltext_policy import NewsError, _objects, normalize_url
from downloader.news_fulltext_public_originals import _one, _visible
from downloader.news_fulltext_metrics import ETTODAY_ATTRIBUTION_FAILURE_STAGES


ETTODAY_BODY_SELECTOR = ':scope > .story[itemprop="articleBody"]'
ETTODAY_PATH_PATTERN = r'^(?:/news/[1-9][0-9]*|/news/[0-9]{8}/[1-9][0-9]*[.]htm)$'
ETTODAY_HOSTS = ('finance.ettoday.net', 'www.ettoday.net')
# The three old clock-cohort failures independently replayed on 2026-10-18.
# This is a bounded retry witness, not a general source-identity waiver.
ETTODAY_V5_PROVED_LITERAL_CONTROL_URLS = (
    'https://finance.ettoday.net/news/1924462',
    'https://finance.ettoday.net/news/2003225',
    'https://finance.ettoday.net/news/2761602',
)
# Observed native finance paragraph: caption, exact reporter, then prose,
# separated by direct BRs. Only this missing head may reopen on v8 adoption.
ETTODAY_V8_PROVED_BREAK_CREDIT_URLS = ('https://finance.ettoday.net/news/3246627',)
# Two independently diagnosed missing heads contain only empty, native Imgur
# declarations beside their complete publisher prose. Other failures are not
# evidence of this layout and must not be replayed by the v9 migration.
ETTODAY_V9_PROVED_STATIC_IMGUR_URLS = (
    'https://finance.ettoday.net/news/2031732',
    'https://finance.ettoday.net/news/2036089',
)
# Exact missing heads independently diagnosed against the same original
# reporter, canonical, clocks and static Threads preview UI. No other failure
# is evidence for this layout or permission to request the embedded post.
ETTODAY_V10_PROVED_THREADS_URLS = (
    'https://finance.ettoday.net/news/3238893',
    'https://finance.ettoday.net/news/3242657',
)
# Source-proved image caption and standalone reporter share one paragraph;
# the actual prose begins in the following native paragraph, not that line.
ETTODAY_V11_PROVED_CAPTION_CREDIT_URLS = ('https://finance.ettoday.net/news/3230422',)
# The source contains exactly two distinct, independently verified static
# Threads UI pairs. Do not reopen unrelated identity or attribution failures.
ETTODAY_V12_PROVED_TWO_THREADS_URLS = ('https://finance.ettoday.net/news/3227115',)
# Literal original DOM: <p><strong>記</strong>者…／…報導</p>. The old
# separator-inserting text read created a space that is not in the source.
ETTODAY_V13_PROVED_SPLIT_REPORTER_URLS = ('https://finance.ettoday.net/news/3200319',)
# One previously missing original contains an Instagram preview, not an Imgur
# declaration. Reopen only this independently observed, due/unleased head.
ETTODAY_V14_PROVED_INSTAGRAM_PREVIEW_URLS = ('https://finance.ettoday.net/news/3210081',)
# One missing native report has the exact publisher comment after author.url,
# not author.name. This is a retry witness, never a general identity waiver.
ETTODAY_V18_PROVED_AUTHOR_URL_COMMENT_URLS = ('https://www.ettoday.net/news/20260529/3174135.htm',)
# One missing report independently exposes a static, explicitly attributed
# Twitter quote. This witness is not a tweet-fetch or general retry grant.
ETTODAY_V19_PROVED_STATIC_TWITTER_URLS = ('https://finance.ettoday.net/news/2498959',)
# One old own-reporter report contains this static, publisher-linked video
# preview. It is not a grant to fetch Facebook or reopen other identity errors.
ETTODAY_V20_PROVED_STATIC_FACEBOOK_URLS = ('https://finance.ettoday.net/news/635788',)
# Native story has two explicitly named reporters; its own NewsArticle schema
# names the first, not the last. A subset never supplies a new newsroom name.
ETTODAY_V21_PROVED_REPORTER_SUBSET_URLS = ('https://www.ettoday.net/news/20210609/2002764.htm',)

# Native subsection and channel labels are different source properties.
ETTODAY_V22_PROVED_3C_CHANNEL_URLS = (
    'https://www.ettoday.net/news/20190718/1493196.htm',
    'https://www.ettoday.net/news/20200906/1802601.htm',
)
# Four missing literal heads were independently replayed on 2026-10-18.
# Two need independent subsection/channel reading; two already pass v22 but
# remain stranded under older failures. Never reopen the rest by error code.
ETTODAY_V23_PROVED_NATIVE_CHANNEL_URLS = (
    'https://www.ettoday.net/news/20141125/430546.htm',
    'https://www.ettoday.net/news/20190922/1540556.htm',
    'https://www.ettoday.net/news/20210113/1897844.htm',
    'https://www.ettoday.net/news/20220517/2253270.htm',
)
# Current house originals independently observed under the existing reviewed
# scope. These are finite retry witnesses, not generic attribution/identity
# waivers or a grant to follow the separate ESG redirect.
ETTODAY_V25_PROVED_HOUSE_NATIVE_FAILURES = (
    ('https://house.ettoday.net/news/3034738', 'article_identity_not_verified'),
    ('https://house.ettoday.net/news/1716642', 'article_attribution_not_verified'),
)
_FACEBOOK_SDK_LOADER = ('(function(d, s, id) {  var js, fjs = d.getElementsByTagName(s)[0];'
    '  if (d.getElementById(id)) return;  js = d.createElement(s); js.id = id;'
    '  js.src = "//connect.facebook.net/zh_TW/sdk.js#xfbml=1&version=v2.3";'
    "  fjs.parentNode.insertBefore(js, fjs);}(document, 'script', 'facebook-jssdk'));")
_THREADS_ICON_SHA256 = '2a70206e4108e03f75e58e69c65e0f0b0bae127c22d95aac53deb62d39064d1f'
_THREADS_WIDGET_STYLE = ('background:#FFF;border-width:1px;border-style:solid;border-color:#00000026;'
    'border-radius:16px;max-width:650px;margin:1px;min-width:270px;padding:0;width:99.375%;'
    'width:-webkit-calc(100%-2px);width:calc(100%-2px);')
_THREADS_COMPACT_WIDGET_STYLE = _THREADS_WIDGET_STYLE.replace('max-width:650px;', 'max-width:540px;')
_THREADS_LINK_STYLE = ('background:#FFFFFF;line-height:0;padding:00;text-align:center;'
    'text-decoration:none;width:100%;font-family:-apple-system,BlinkMacSystemFont,sans-serif;')
_CLOCK = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\+08:00')
_REVISION_PLACEHOLDER = '1970-01-01T08:00:00+08:00'
_REPORTER = re.compile(r'記者([\u3400-\u9fff]{2,8}(?:、[\u3400-\u9fff]{2,8}){0,3})／[\u3400-\u9fff]{1,12}報導')
_BARE_REPORTER = re.compile(r'([\u3400-\u9fff]{2,8})／[\u3400-\u9fff]{1,12}報導')
# Literal visible house signatures independently observed on 2026-10-18.
# These are unnamed publisher desks, not people or a general Center grammar.
_HOUSE_NEWSROOM_CREDITS = {'房產中心／綜合報導': '房產中心', '財經中心／台北報導': '財經中心'}
_CREDIT_PREFIX = re.compile(r'^(?:記者|編輯|編譯|實習記者|財經中心|生活中心|社會中心|'
    r'政治中心|國際中心|ETtoday|ETTODAY|中央社|路透社|本文來源|來源[：:]|轉載|文[／/])')
_AD_TEXT = '[廣告]請繼續往下閱讀...'


def _attribution_failure(stage):
    """A fixed branch witness, never source text or a second DOM parse."""
    if stage not in ETTODAY_ATTRIBUTION_FAILURE_STAGES:
        raise ValueError('invalid_ettoday_attribution_failure_stage')
    error = NewsError('article_attribution_not_verified')
    error.ettoday_attribution_failure_stage = stage
    return error


def ettoday_request_identity(url, *, requested=False, allow_finance_redirect_query=False, allow_house=False):
    """Bind the literal route; v16 admits only the observed finance parameter.

    This does not rewrite or fetch a queryless URL. The native canonical, OG
    and schema still have to independently bind the same host and path.
    """
    try:
        parts = urlsplit(normalize_url(url))
    except (NewsError, TypeError, ValueError):
        return None
    pattern = (r'/news/([1-9][0-9]*)' if parts.netloc == ETTODAY_HOSTS[0]
        or allow_house and parts.netloc == 'house.ettoday.net' else
        r'/news/[0-9]{8}/([1-9][0-9]*)[.]htm' if parts.netloc == ETTODAY_HOSTS[1] else None)
    verified_query = (allow_finance_redirect_query and parts.netloc == ETTODAY_HOSTS[0]
        and parts.query == 'redirect=1')
    if (parts.scheme not in ({'http', 'https'} if requested else {'https'})
            or parts.query and not verified_query or not pattern or not re.fullmatch(pattern, parts.path)):
        return None
    return parts.netloc, parts.path


def ettoday_source_redirect_verified(requested_url, response, *, allow_house=False):
    """v17 binds every actual Location before final native article validation.

    Matching IDs alone do not prove an alias. Only a connected source chain
    through the two reviewed literal routes may cross their host/date paths.
    No URL, body, byline or publication clock is generated or substituted.
    """
    try:
        current = normalize_url(requested_url)
        final = normalize_url(response.url)
        hops = response.redirects
        if not 1 <= len(hops) <= 5:
            return False
        initial = ettoday_request_identity(current, requested=True, allow_finance_redirect_query=True, allow_house=allow_house)
        if initial is None:
            return False
        article_id = re.search(r'/([1-9][0-9]*)(?:[.]htm)?$', initial[1])[1]
        visited = {current}
        for hop in hops:
            if (hop.from_url != current or type(hop.status) is not int
                    or hop.status not in {301, 302, 303, 307, 308}
                    or not isinstance(hop.location, str) or not hop.location.strip()
                    or hop.to_url != normalize_url(urljoin(current, hop.location))
                    or hop.to_url in visited
                    or urlsplit(current).scheme == 'https' and urlsplit(hop.to_url).scheme != 'https'
                    or type(hop.response_bytes) is not int or hop.response_bytes < 0
                    or not re.fullmatch(r'[0-9a-f]{64}', hop.response_sha256)):
                return False
            if datetime.fromisoformat(hop.observed_at_utc).utcoffset() is None:
                return False
            identity = ettoday_request_identity(hop.to_url, requested=True, allow_finance_redirect_query=True, allow_house=allow_house)
            if identity is None or re.search(r'/([1-9][0-9]*)(?:[.]htm)?$', identity[1])[1] != article_id:
                return False
            current = hop.to_url
            visited.add(current)
        return (current == final
                and ettoday_request_identity(final, allow_finance_redirect_query=True, allow_house=allow_house) is not None)
    except (NewsError, TypeError, ValueError, AttributeError, IndexError):
        return False


def _key(value):
    return re.sub(r'\s+', '', value) if isinstance(value, str) else ''


def _visible_text(node):
    _visible(node)
    for value in node.find_all(string=True):
        if str(value).strip() and value.parent.name not in {'script', 'style'}:
            _visible(value.parent)


def _clocks(soup, scope, host, *, adapter_contract_version=1):
    publication = _one(soup, 'head > meta[name="pubdate"][content]', 'invalid_bulk_publication_clock')
    # The actual www page repeats itemprop=dateCreated/datePublished on this
    # same pubdate meta. HTML engines select different aliases; the unique
    # native name/content and independently matching release minute control.
    if publication.get('itemprop') not in {'dateCreated', 'datePublished'}:
        raise NewsError('invalid_bulk_publication_clock')
    published = publication.get('content')
    modified = _one(soup, 'head > meta[name="lastmod"][itemprop="dateModified"][content]',
        'invalid_bulk_publication_clock').get('content')
    try:
        if not isinstance(published, str) or not isinstance(modified, str) or not (
                _CLOCK.fullmatch(published) and _CLOCK.fullmatch(modified)):
            raise ValueError('source precision or offset not verified')
        pub, mod = datetime.fromisoformat(published), datetime.fromisoformat(modified)
        revision_unknown = modified == _REVISION_PLACEHOLDER
        if revision_unknown and not (adapter_contract_version >= 4 and host == ETTODAY_HOSTS[0]):
            raise ValueError('observed source epoch placeholder, not revision evidence')
        if adapter_contract_version == 1 and mod < pub:
            raise ValueError('revision precedes source release')
        if adapter_contract_version >= 24 and host == 'house.ettoday.net':
            # House is not the finance or WWW DOM: its own article wrapper
            # and sibling breadcrumb minute independently bind publication.
            container = scope.parent.parent
            if scope.parent.name != 'article' or 'c1' not in container.get('class', []):
                raise ValueError('current house release scope not verified')
            label = _one(container, ':scope > .breadcrumb_box > time.date[itemprop="datePublished"]',
                'invalid_bulk_publication_clock')
            visible = datetime.strptime(label.get_text(' ', strip=True), '%Y-%m-%d %H:%M')
        elif host == ETTODAY_HOSTS[0]:
            if 'c1' not in scope.parent.get('class', []):
                raise ValueError('current release scope not verified')
            label = _one(scope.parent, ':scope > .breadcrumb_box > time.date[itemprop="datePublished"]',
                'invalid_bulk_publication_clock')
            visible = datetime.strptime(label.get_text(' ', strip=True), '%Y-%m-%d %H:%M')
        else:
            if scope.parent.name != 'article' or 'part_area_1' not in scope.parent.parent.get('class', []):
                raise ValueError('current release scope not verified')
            label = _one(scope.parent.parent, ':scope > .part_breadcrumb_2 > time.date[itemprop="datePublished"]',
                'invalid_bulk_publication_clock')
            visible = datetime.strptime(label.get_text(' ', strip=True), '%Y年%m月%d日 %H:%M')
        _visible_text(label)
        if visible != pub.replace(tzinfo=None, second=0):
            raise ValueError('native minute conflicts with pubdate')
        native_datetime = label.get('datetime')
        observed_nonclocks = {'28', '29', '30', '31'} if adapter_contract_version >= 3 else {'31'}
        if native_datetime != published and not (host == ETTODAY_HOSTS[0] and native_datetime in observed_nonclocks):
            raise ValueError('unreviewed native datetime anomaly')
        extra = soup.select('head > meta[property="article:published_time"][content]')
        if extra and (len(extra) != 1 or extra[0].get('content') != published):
            raise ValueError('competing release declarations')
    except (ValueError, TypeError, AttributeError):
        raise NewsError('invalid_bulk_publication_clock') from None
    notices = [{'kind': 'native_pubdate', 'text': published},
        {'kind': 'native_lastmod', 'text': modified},
        {'kind': 'native_visible_release_minute', 'text': label.get_text(' ', strip=True)}]
    if native_datetime in observed_nonclocks:
        # Native finance pages contain these literal non-clock attributes.
        # Never parse them as dates or infer a source bug's cause. The unique
        # offset pubdate, visible minute and optional schema still bind release.
        notices.append({'kind': 'source_invalid_datetime_attribute_preserved', 'text': native_datetime})
    if revision_unknown:
        # Only the observed finance placeholder is missing revision evidence.
        # The raw declaration still has to agree with any NewsArticle schema;
        # publication and all other identity proofs remain independently valid.
        notices.append({'kind': 'source_revision_placeholder_not_observation',
            'declared_modification': modified, 'usable_modification': None,
            'publication_substituted_for_modification': False})
    elif adapter_contract_version >= 2 and mod < pub:
        # These are distinct source properties, not a universally ordered
        # event sequence. Preserve both declarations after independent native
        # and schema checks; do not infer why they differ or backdate public
        # availability to an edit time. No historical PIT claim follows.
        notices.append({'kind': 'source_modification_precedes_publication',
            'declared_publication': published, 'declared_modification': modified,
            'availability_backdated_to_modification': False,
            'source_reason_inferred': False})
    return published, modified, notices


def _native_json(text, *, allow_literal_value_controls=False, allow_author_url_comment=False):
    """Observed source syntax defects, without changing a source value.

    Literal CR/LF inside a description and one exact source author comment are
    not valid JSON. A bounded lexer distinguishes strings from comments; never
    delete arbitrary comments/bytes, replace quotes or use a forgiving JS eval.
    v5 additionally escapes literal C0 in string *values* with JSON's exact
    Unicode escape. Decoded values, including backspace/STX, stay unchanged;
    keys, broken escapes, outside-string controls and other grammar still fail.
    v18 also admits that exact comment after a Person author's final URL; a
    second, bounded structural parse proves the non-value token's location.
    Original response bytes and syntax anomalies remain in the receipt chain.
    """
    try:
        return json.loads(text), []
    except json.JSONDecodeError:
        pass
    output, anomalies, quoted, escaped, field, start, index = [], [], False, False, '', 0, 0
    author_url_comment_offset = None
    while index < len(text):
        char = text[index]
        if quoted:
            if escaped:
                output.append(char)
                escaped = False
            elif char == '\\':
                output.append(char)
                escaped = True
            elif char == '"':
                output.append(char)
                quoted = False
                cursor = index + 1
                while cursor < len(text) and text[cursor].isspace():
                    cursor += 1
                if cursor < len(text) and text[cursor] == ':':
                    field = json.loads(text[start:index + 1])
            elif ord(char) < 32:
                if field == 'description' and char in '\r\n':
                    output.append('\\r' if char == '\r' else '\\n')
                    anomalies.append('source_unescaped_description_linebreak')
                elif allow_literal_value_controls:
                    # Strict parsing of original key tokens at their closing
                    # quote above prevents this from silently repairing keys.
                    output.append(f'\\u{ord(char):04x}')
                    anomalies.append('source_unescaped_json_value_control')
                else:
                    raise ValueError('unreviewed raw string control')
            else:
                output.append(char)
        elif char == '"':
            output.append(char)
            quoted, start = True, index
        elif char == '/' and text[index:index + 2] == '//':
            end = text.find('\n', index)
            if end < 0:
                raise ValueError('unreviewed JSON comment')
            if (text[index:end].strip() != '//如果有筆名，才會有url'
                    or 'source_non_json_author_comment' in anomalies
                    or author_url_comment_offset is not None):
                raise ValueError('unreviewed JSON comment')
            if field == 'name':
                anomalies.append('source_non_json_author_comment')
            elif (allow_author_url_comment and field == 'url'
                    and ''.join(output).rstrip().endswith('"')
                    and text[end:].lstrip().startswith('}')):
                # One observed comment follows the author's final URL value.
                # Bind its actual object position below; an unrelated URL or
                # text containing this token is not an author declaration.
                author_url_comment_offset = len(''.join(output))
                anomalies.append('source_non_json_author_url_comment')
            else:
                raise ValueError('unreviewed JSON comment')
            index = end
            continue
        else:
            output.append(char)
        index += 1
    if quoted or not anomalies:
        raise ValueError('source JSON not verified')
    normalized = ''.join(output)
    data = json.loads(normalized)
    if author_url_comment_offset is not None:
        # A separate structural witness locates the removed non-value token.
        # The marker never enters returned source data or changes any value.
        marker = '__stockagent_author_url_comment_position__'
        if marker in text or any(marker in obj for obj in _objects(data)):
            raise ValueError('ambiguous author comment position')
        position = author_url_comment_offset
        witness = json.loads(normalized[:position] + ',"' + marker + '":true' + normalized[position:])
        matched = []
        for article in _objects(witness):
            if article.get('@type') not in ('NewsArticle', 'Article'):
                continue
            authors = article.get('author')
            authors = authors if isinstance(authors, list) else [authors]
            matched.extend(author for author in authors if isinstance(author, dict) and marker in author
                and author.get('@type') == 'Person' and isinstance(author.get('url'), str)
                and list(author)[-2:] == ['url', marker])
        if len(matched) != 1:
            raise ValueError('unreviewed author URL comment position')
    return data, list(dict.fromkeys(anomalies))


def _schemas(soup, original, title, published, modified, names, *, adapter_contract_version=1,
             require_headline_witness=False, social_headline_key=None, native_bare_reporters=(),
             native_newsroom=False):
    declarations = []
    notices = []
    scripts = soup.select('script[type="application/ld+json"]')
    try:
        if len(scripts) > 16:
            raise ValueError('bounded article schema required')
        for script in scripts:
            if len(script.get_text()) > 262144:
                raise ValueError('bounded schema required')
            data, anomalies = _native_json(script.get_text(),
                allow_literal_value_controls=adapter_contract_version >= 5,
                allow_author_url_comment=adapter_contract_version >= 18)
            declarations.extend(obj for obj in _objects(data)
                if obj.get('@type') in ('NewsArticle', 'Article'))
            # HTML parsers normalize line endings differently. The raw
            # response hash lives in the shared capture receipt, not a hash
            # falsely labelled "original" after DOM normalization.
            notices.extend({'kind': kind, 'raw_evidence': 'shared_original_response_receipt'} for kind in anomalies)
        if len(declarations) > 1 or require_headline_witness and len(declarations) != 1:
            raise ValueError('ambiguous current article')
    except (ValueError, TypeError, RecursionError):
        raise NewsError('article_identity_not_verified') from None
    for article in declarations:
        headline_key = _key(article.get('headline'))
        headline_matches = headline_key == _key(title)
        social_matches = (adapter_contract_version >= 7 and require_headline_witness
            and bool(social_headline_key) and headline_key == social_headline_key)
        if (article.get('url') != original or article.get('mainEntityOfPage') != original
                or not (headline_matches or social_matches) or article.get('articleBody')):
            raise NewsError('article_identity_not_verified')
        free = article.get('isAccessibleForFree')
        if free is not None and free is not True:
            raise NewsError('paywall_or_login')
        if article.get('datePublished') != published or article.get('dateModified') != modified:
            raise NewsError('invalid_bulk_publication_clock')
        if native_bare_reporters:
            # The visible byline names the writer without the 記者 prefix.
            # Existing variants have a preceding reporter photo caption;
            # v28 also proves the literal name/place/報導 signature itself.
            # Bind either native name to this article's creator/name/id.
            # Never take an author name from Organization or an account alone.
            creator = article.get('creator')
            if (len(native_bare_reporters) != 1 or not isinstance(creator, list) or len(creator) != 2
                    or not isinstance(creator[1], str) or not re.fullmatch(r'[1-9][0-9]{0,9}', creator[1])
                    or creator[0] != creator[1] + '-' + native_bare_reporters[0]):
                raise _attribution_failure('bare_reporter_creator_mismatch')
        author, publisher = article.get('author'), article.get('publisher')
        organization = author == {'@type': 'Organization', 'name': 'ETtoday新聞雲', 'url': 'https://www.ettoday.net/'}
        if native_newsroom and not organization:
            # A schema editor/account or Person must not supply a desk's
            # missing individual writer or clear an outside contributor.
            raise NewsError('article_rights_or_scope_excluded')
        authors = author if isinstance(author, list) else [author]
        reporters = (len(authors) == len(names)
            and all(isinstance(a, dict) and a.get('@type') == 'Person' for a in authors)
            and [a.get('name') for a in authors] == names)
        last_of_two = (len(names) == 2 and len(authors) == 1 and isinstance(authors[0], dict)
            and authors[0].get('@type') == 'Person' and authors[0].get('name') == names[-1])
        # The schema's author property is not an assertion that the visible
        # signature has no other reporters. Require an exact, nonempty ordered
        # subset of its unique literal names; no fuzzy matching or invented
        # attribution. Keep the existing last-of-two notice/value ABI intact.
        subset_names = ([a.get('name') for a in authors]
            if all(isinstance(a, dict) and a.get('@type') == 'Person' for a in authors) else [])
        ordered_subset = (adapter_contract_version >= 21 and 1 <= len(subset_names) < len(names) <= 4
            and len(set(names)) == len(names) and all(isinstance(name, str) and name in names for name in subset_names)
            and len(set(subset_names)) == len(subset_names)
            and [names.index(name) for name in subset_names] == sorted(names.index(name) for name in subset_names))
        if (not (organization or reporters or last_of_two or ordered_subset) or not isinstance(publisher, dict) or publisher.get('@type') != 'Organization'
                or _key(publisher.get('name')) != 'ETtoday新聞雲'):
            raise NewsError('article_rights_or_scope_excluded')
        if organization:
            notices.append({'kind': 'schema_organization_author_not_reporter', 'text': 'ETtoday新聞雲'})
        elif last_of_two:
            notices.append({'kind': 'schema_declares_last_of_two_native_reporters', 'text': names[-1]})
        elif ordered_subset:
            notices.append({'kind': 'schema_declares_ordered_subset_of_native_reporters',
                'schema_declared_reporters': subset_names, 'native_reporter_indices': [names.index(name) for name in subset_names],
                'reporters_taken_from_whole_visible_signature': True,
                'schema_promoted_to_additional_reporter': False, 'source_values_rewritten': False})
        if require_headline_witness:
            notices.append({'kind': 'native_variant_current_schema_headline', 'text': article['headline'],
                'matches': 'native_headline' if headline_matches else 'social_title',
                'literal_article_url_and_clocks_verified': True})
    return notices


def _house_image_caption_credit(node, *, allow_reporter=False, allow_trailing_caption_break=False):
    """The observed image/caption/credit-only first P, never sliced prose.

    Keep the original response intact. Only remove this verified prelude from
    the working copy, retaining the literal caption and credit separately.
    Nested blocks, scripts, extra prose, credits and hidden text stay closed.
    """
    children = [child for child in node.contents
        if not isinstance(child, NavigableString) or str(child).strip()]
    if (len(children) < 4 or not isinstance(children[0], Tag) or children[0].name != 'img'
            or type(children[-1]) is not NavigableString):
        return None
    credit = str(children[-1]).strip()
    newsroom = _HOUSE_NEWSROOM_CREDITS.get(credit)
    reporter = _REPORTER.fullmatch(credit) if allow_reporter else None
    if not newsroom and not reporter:
        return None
    middle = children[1:-1]
    captions = [child for child in middle if isinstance(child, Tag) and child.name == 'strong']
    if (len(captions) != 1 or any(type(child) is not Tag or child.name not in {'br', 'strong'} for child in middle)
            or not any(child.name == 'br' for child in middle[:middle.index(captions[0])])):
        raise _attribution_failure('image_credit_structure')
    caption = captions[0]
    direct_break = any(child.name == 'br' for child in middle[middle.index(caption) + 1:])
    caption_leaves = [child for child in caption.contents
        if not isinstance(child, NavigableString) or str(child).strip()]
    # The observed903852 keeps its caption/report separator inside STRONG.
    # Only an empty, attribute-free trailing BR can supply that boundary,
    # and only for the explicitly named reporter variant. No prose slicing.
    native_trailing_break = (allow_trailing_caption_break and bool(reporter) and bool(caption_leaves)
        and type(caption_leaves[-1]) is Tag and caption_leaves[-1].name == 'br'
        and all(not child.attrs and not child.contents for child in caption_leaves
            if type(child) is Tag and child.name == 'br'))
    if not direct_break and not native_trailing_break:
        raise _attribution_failure('image_credit_structure')
    if any(type(child) not in (NavigableString, Tag) or isinstance(child, Tag) and child.name != 'br'
            for child in caption.contents):
        raise _attribution_failure('image_caption_structure')
    if len([child for child in caption.contents if type(child) is NavigableString and str(child).strip()]) != 1:
        raise _attribution_failure('image_caption_structure')
    text = caption.get_text(' ', strip=True)
    if not text.startswith('▲') or _REPORTER.search(text) or any(c in text for c in _HOUSE_NEWSROOM_CREDITS):
        raise _attribution_failure('image_caption_ambiguous_credit')
    following = node.find_next_sibling()
    following_text = following.get_text(' ', strip=True) if following is not None and following.name == 'p' else ''
    if not following_text or following_text.startswith('▲'):
        raise _attribution_failure('image_credit_missing_following_prose')
    _visible_text(node)
    _visible(children[0])
    _visible_text(following)
    return newsroom, credit, text, native_trailing_break and not direct_break


def _caption_break_credit(node, *, allow_caption_credit_only=False, syntax_notices=None):
    """Remove only the observed BR-delimited caption/byline prefix in place.

    Called on a visible caption paragraph immediately after its native image.
    A byline is one plain, standalone source line, never a prose substring.
    Keep the original paragraph, trailing inline nodes, separators and order;
    do not synthesize body text from a flattened paragraph or image metadata.
    """
    _visible_text(node)
    lines, current = [], []
    for child in node.contents:
        if isinstance(child, Tag) and child.name == 'br':
            lines.append(current)
            current = []
        else:
            # Only direct BR boundaries and the observed inline vocabulary.
            # Nested BR/block/media/script layouts need their own evidence.
            if isinstance(child, Tag) and (child.name not in {'strong', 'b', 'span', 'a', 'em', 'i', 'u'}
                    or any(tag.name not in {'strong', 'b', 'span', 'a', 'em', 'i', 'u'} for tag in child.find_all(True))):
                raise _attribution_failure('caption_credit_structure')
            if not isinstance(child, (Tag, NavigableString)) or type(child) not in (Tag, NavigableString):
                raise _attribution_failure('caption_credit_structure')
            current.append(child)
    lines.append(current)
    values = [' '.join(value for child in line
        if (value := child.get_text(' ', strip=True) if isinstance(child, Tag) else str(child).strip())) for line in lines]
    nonempty = [index for index, value in enumerate(values) if value]
    credit_only = allow_caption_credit_only and len(nonempty) == 2
    if (len(nonempty) < 3 and not credit_only) or not values[nonempty[0]].startswith('▲'):
        raise _attribution_failure('caption_credit_boundaries')
    credit_index = nonempty[1]
    credit = values[credit_index]
    match = _REPORTER.fullmatch(credit)
    leaves = [child for child in lines[credit_index] if not isinstance(child, NavigableString) or str(child).strip()]
    if (not match or len(leaves) != 1 or type(leaves[0]) is not NavigableString
            or any(_REPORTER.search(value) for index, value in enumerate(values) if index != credit_index)):
        raise _attribution_failure('caption_credit_reporter')
    if any(re.match(r'^(?:中央社|路透社|本文來源|來源[：:]|轉載)', values[index]) for index in nonempty[2:]):
        raise NewsError('article_rights_or_scope_excluded')
    names = match[1].split('、')
    if len(set(names)) != len(names):
        raise _attribution_failure('caption_credit_duplicate_reporters')
    if credit_only:
        # The attested native paragraph contains only the caption and reporter.
        # Require actual, visible, separate following prose; never manufacture
        # a third line to satisfy the old caption/byline/prose grammar.
        following = node.find_next_sibling()
        if following is None or following.name != 'p':
            raise _attribution_failure('caption_credit_missing_following_prose')
        text = following.get_text(' ', strip=True)
        if not text or text.startswith('▲') or _REPORTER.fullmatch(text):
            raise _attribution_failure('caption_credit_missing_following_prose')
        _visible_text(following)
        for paragraph in node.find_next_siblings('p'):
            if re.match(r'^(?:中央社|路透社|本文來源|來源[：:]|轉載)', paragraph.get_text(' ', strip=True)):
                raise NewsError('article_rights_or_scope_excluded')
        # Both removed fields are retained by the canonical sidecars. There is
        # no prose in this working-copy paragraph, and the source DOM is intact.
        node.clear()
        if syntax_notices is not None:
            syntax_notices.append({'kind': 'native_caption_and_reporter_only_paragraph',
                'contract_version': 11, 'nonempty_lines': 2,
                'following_prose_is_separate_native_paragraph': True,
                'reporter_line_is_plain_text': True, 'source_values_rewritten': False})
        return names, credit, values[nonempty[0]]
    # Extract the actual original nodes only through the byline's closing BR.
    # No subsequent prose/inline nodes or paragraph attributes are rewritten.
    boundaries = 0
    for child in list(node.contents):
        is_break = isinstance(child, Tag) and child.name == 'br'
        child.extract()
        if is_break:
            if boundaries == credit_index:
                break
            boundaries += 1
    return names, credit, values[nonempty[0]]


def _static_facebook_video_preview(body):
    """Separate only the observed native SDK/preview/empty-root declaration.

    This is a literal static grammar, not Javascript evaluation or arbitrary
    script/quote removal. The linked label remains in the shared original and
    a sidecar, never a newsroom sentence or a video transcript. Other markup,
    hosts, scripts, hidden content and unbound video identities stay closed.
    """
    widgets, roots = body.select('.fb-video'), body.select('#fb-root')
    scripts = body.find_all('script')
    if not widgets and not roots and not any(n.get_text() == _FACEBOOK_SDK_LOADER for n in scripts):
        return []

    def reject():
        raise NewsError('article_identity_not_verified')

    def elements(node):
        values = []
        for child in node.contents:
            if type(child) is NavigableString and not str(child).strip():
                continue
            if type(child) is not Tag:
                reject()
            values.append(child)
        return values

    if len(widgets) != 1 or len(roots) != 1 or len(scripts) != 1:
        reject()
    widget, root, script = widgets[0], roots[0], scripts[0]
    route = widget.get('data-href')
    match = re.fullmatch(r'/ETtodayPOLITICS/videos/vb[.]([1-9][0-9]*)/([1-9][0-9]*)/\?type=3',
        route) if isinstance(route, str) else None
    next_node = script.next_sibling
    while type(next_node) is NavigableString and not str(next_node).strip():
        next_node = next_node.next_sibling
    if (not match or widget.name != 'div' or widget.parent is not body
            or widget.attrs != {'class': ['fb-video'], 'data-allowfullscreen': '1', 'data-href': route}
            or script.parent is not body or script.attrs or len(script.contents) != 1
            or script.get_text() != _FACEBOOK_SDK_LOADER or next_node is not widget
            or root.name != 'div' or root.parent is not body or root.attrs != {'id': 'fb-root'}
            or body.find_all(True, recursive=False)[-1] is not root):
        reject()
    ignored_children = elements(widget)
    if len(ignored_children) != 1:
        reject()
    ignored = ignored_children[0]
    if ignored.name != 'div' or ignored.attrs != {'class': ['fb-xfbml-parse-ignore']}:
        reject()
    quotes = elements(ignored)
    if len(quotes) != 1 or quotes[0].name != 'blockquote':
        reject()
    quote = quotes[0]
    literal_url = 'https://www.facebook.com/ETtodayPOLITICS/videos/' + match[2] + '/'
    if quote.attrs != {'cite': literal_url}:
        reject()
    anchors = elements(quote)
    if len(anchors) != 1 or anchors[0].name != 'a' or anchors[0].attrs != {'href': literal_url}:
        reject()
    anchor = anchors[0]
    if (len(anchor.contents) != 3 or type(anchor.contents[0]) is not NavigableString
            or not 1 <= len(str(anchor.contents[0])) <= 256 or not str(anchor.contents[0]).strip()
            or type(anchor.contents[1]) is not Tag or anchor.contents[1].name != 'br'
            or anchor.contents[1].attrs or anchor.contents[1].contents
            or type(anchor.contents[2]) is not NavigableString or str(anchor.contents[2]) != '\n'):
        reject()
    root_children = elements(root)
    if (len(root_children) != 1 or root_children[0].name != 'strong' or root_children[0].attrs
            or len(root_children[0].contents) != 1 or type(root_children[0].contents[0]) is not NavigableString
            or str(root_children[0].contents[0]) != '\xa0'):
        reject()
    for node in (script, widget, ignored, quote, anchor, anchor.contents[1], root, root_children[0]):
        _visible(node)
    label = anchor.get_text().replace('\r\n', '\n').replace('\r', '\n')
    notice = {'kind': 'native_static_facebook_video_preview_declaration',
        'source_widget_attributes': dict(widget.attrs), 'source_quote_attributes': dict(quote.attrs),
        'source_link_attributes': dict(anchor.attrs), 'source_visible_video_label': label,
        'source_loader_dom_lf_sha256': hashlib.sha256(_FACEBOOK_SDK_LOADER.encode()).hexdigest(),
        'text_witness_basis': 'HTML_input_CRLF_CR_to_LF; exact_bytes_in_shared_original',
        'source_root_nonbreaking_space_preserved': True, 'raw_declarations_preserved': True,
        'excluded_from_newsroom_body': True, 'external_request_sent': False, 'script_executed': False,
        'media_content_accepted': False, 'video_label_is_reporter_attribution': False}
    for node in (script, widget, root):
        node.decompose()
    return [notice]


def _static_imgur_declarations(body):
    """Separate exact empty media declarations, never download their content.

    The observed source has one or two direct empty blockquote/script pairs.
    No script execution, arbitrary script stripping, fallback prose removal,
    hidden media, inferred image contents or third-party requests are allowed.
    Raw response bytes preserve both declarations unchanged in the receipt.
    """
    scripts = body.find_all('script')
    if not scripts:
        return []
    widgets = body.select('.imgur-embed-pub')
    if not 1 <= len(scripts) == len(widgets) <= 2:
        raise NewsError('article_identity_not_verified')
    pairs, identifiers = [], set()
    for widget in widgets:
        identifier = widget.get('data-id', '')
        script = widget.next_sibling
        while type(script) is NavigableString and not str(script).strip():
            script = script.next_sibling
        if (widget.name != 'blockquote' or widget.parent is not body or widget.contents
                or widget.attrs != {'class': ['imgur-embed-pub'], 'lang': 'en',
                    'data-id': identifier, 'data-context': 'false'}
                or not isinstance(identifier, str) or not re.fullmatch(r'[A-Za-z0-9]{7}', identifier)
                or identifier in identifiers or not isinstance(script, Tag)
                or script.name != 'script' or script.parent is not body or script.contents
                or script.attrs != {'async': '', 'src': '//s.imgur.com/min/embed.js', 'charset': 'utf-8'}
                or not any(script is candidate for candidate in scripts)):
            raise NewsError('article_identity_not_verified')
        _visible(widget)
        _visible(script)
        identifiers.add(identifier)
        pairs.append((widget, script))
    notices = []
    for widget, script in pairs:
        notices.append({'kind': 'native_empty_static_imgur_declaration',
            'source_blockquote_attributes': dict(widget.attrs), 'source_script_attributes': dict(script.attrs),
            'external_request_sent': False, 'script_executed': False, 'media_content_accepted': False,
            'raw_declarations_preserved': True})
        widget.decompose()
        script.decompose()
    return notices


def _static_threads_declaration(body, *, allow_multiple=False, allow_compact_width=False):
    """Validate every static UI pair before removing any working-copy node.

    V12 admits two distinct direct pairs, using the same single-pair grammar.
    Earlier ABIs still require one. No script execution or embedded-post fetch
    is involved, and no arbitrary script, quote or prose is stripped.
    """
    widgets = body.select('.text-post-media')
    if not widgets:
        return []
    scripts = body.find_all('script')
    if not 1 <= len(widgets) == len(scripts) <= (2 if allow_multiple else 1):
        raise NewsError('article_identity_not_verified')
    pairs, identifiers, notices = [], set(), []
    for widget in widgets:
        script = widget.next_sibling
        while type(script) is NavigableString and not str(script).strip():
            script = script.next_sibling
        if type(script) is not Tag or script.name != 'script':
            raise NewsError('article_identity_not_verified')
        notice = _static_threads_pair(body, widget, script, allow_compact_width=allow_compact_width)
        identifier = notice['source_blockquote_attributes']['id']
        if identifier in identifiers:
            raise NewsError('article_identity_not_verified')
        identifiers.add(identifier)
        notices.append(notice)
        pairs.append((widget, script))
    if {id(script) for _, script in pairs} != {id(script) for script in scripts}:
        raise NewsError('article_identity_not_verified')
    for widget, script in pairs:
        widget.decompose()
        script.decompose()
    return notices


def _static_threads_pair(body, widget, script, *, allow_compact_width=False):
    """Same exact six descendants, attributes, icon and UI label for each pair."""

    def reject():
        raise NewsError('article_identity_not_verified')

    def children(node):
        values = []
        for child in node.contents:
            if type(child) is NavigableString and not str(child).strip():
                continue
            if type(child) is not Tag:
                reject()
            values.append(child)
        return values

    def attrs(node, expected):
        actual = dict(node.attrs)
        if 'style' in expected and isinstance(actual.get('style'), str):
            actual['style'] = re.sub(r'\s+', '', actual['style'])
        if actual != expected:
            reject()
        _visible(node)

    permalink = widget.get('data-text-post-permalink', '')
    match = (re.fullmatch(r'https://www[.]threads[.]com/@[A-Za-z0-9_.]{1,64}/post/([A-Za-z0-9_-]{11})', permalink)
        if isinstance(permalink, str) else None)
    adjacent = widget.next_sibling
    while type(adjacent) is NavigableString and not str(adjacent).strip():
        adjacent = adjacent.next_sibling
    if (not match or widget.name != 'blockquote' or widget.parent is not body
            or script.parent is not body or adjacent is not script or script.contents):
        reject()
    # Native 3031687 independently publishes the same closed UI tree at 540px.
    # Admit only this exact presentation variant in v15, not arbitrary CSS.
    compact = allow_compact_width and isinstance(widget.get('style'), str) and (
        re.sub(r'\s+', '', widget['style']) == _THREADS_COMPACT_WIDGET_STYLE)
    attrs(widget, {'class': ['text-post-media'], 'id': 'ig-tp-' + match[1],
        'data-text-post-permalink': permalink, 'data-text-post-version': '0',
        'style': _THREADS_COMPACT_WIDGET_STYLE if compact else _THREADS_WIDGET_STYLE})
    attrs(script, {'async': '', 'src': 'https://www.threads.com/embed.js'})
    direct = children(widget)
    if len(direct) != 1 or direct[0].name != 'a':
        reject()
    anchor = direct[0]
    attrs(anchor, {'href': permalink, 'target': '_blank', 'style': _THREADS_LINK_STYLE})
    direct = children(anchor)
    if len(direct) != 1 or direct[0].name != 'div':
        reject()
    outer = direct[0]
    attrs(outer, {'style': 'padding:40px;display:flex;flex-direction:column;align-items:center;'})
    direct = children(outer)
    if len(direct) != 2 or any(node.name != 'div' for node in direct):
        reject()
    icon, label = direct
    attrs(icon, {'style': 'display:block;height:32px;width:32px;padding-bottom:20px;'})
    attrs(label, {'style': 'font-size:15px;line-height:21px;color:#000000;font-weight:600;'})
    if (len(label.contents) != 1 or type(label.contents[0]) is not NavigableString
            or str(label.contents[0]) != '在 Threads 查看'):
        reject()
    direct = children(icon)
    if len(direct) != 1 or direct[0].name != 'svg':
        reject()
    svg = direct[0]
    attrs(svg, {'aria-label': 'Threads', 'height': '32px', 'role': 'img', 'viewbox': '',
        'width': '32px', 'xmlns': 'http://www.w3.org/2000/svg'})
    direct = children(svg)
    if (len(direct) != 1 or direct[0].name != 'path' or set(direct[0].attrs) != {'d'}
            or direct[0].contents or not isinstance(direct[0].get('d'), str)
            or hashlib.sha256(direct[0]['d'].encode()).hexdigest() != _THREADS_ICON_SHA256):
        reject()
    notice = {'kind': 'native_static_threads_preview_ui_declaration',
        'source_blockquote_attributes': dict(widget.attrs), 'source_script_attributes': dict(script.attrs),
        'source_link_attributes': dict(anchor.attrs), 'source_icon_sha256': _THREADS_ICON_SHA256,
        'excluded_ui_label': str(label.contents[0]), 'external_request_sent': False,
        'script_executed': False, 'embedded_post_content_accepted': False,
        'raw_declarations_preserved': True}
    if compact:
        notice['source_presentation_variant'] = 'native_540px_preview_width'
    return notice


def _caption_editor_padding(node, *, allow_plain_break=False):
    """Observed terminal caption BR, not a byline/prose split.

    The v10 source uses type=_moz; v15 independently observes the otherwise
    identical plain BR plus trailing whitespace. Both keep the next reporter
    paragraph separate and preserve the original caption in its sidecar.
    """
    if node.attrs or len(node.contents) != 1 or type(node.contents[0]) is not Tag:
        return False
    strong = node.contents[0]
    if strong.name != 'strong' or strong.attrs or len(strong.contents) != 3:
        return False
    text, padding, space = strong.contents
    return (type(text) is NavigableString and str(text).strip().startswith('▲')
        and not _REPORTER.search(str(text)) and type(padding) is Tag and padding.name == 'br'
        and (padding.attrs == {'type': '_moz'} or (allow_plain_break and not padding.attrs)) and not padding.contents
        and type(space) is NavigableString and bool(str(space)) and not str(space).strip())


def _static_twitter_quote_declaration(body, script):
    """Separate the observed source-declared quote from newsroom journalism.

    No Twitter request, script execution, post-time inference or attribution
    to the article's reporter. The quote stays in the shared raw original;
    its literal attribution/link/date label and text digest remain sidecars.
    Unknown widgets, hidden content, inline code and mixed markup stay closed.
    """
    widgets = body.select('[class*="twitter"], [data-tweet-id], '
        'iframe[src*="twitter.com"], iframe[src*="x.com"]')
    if len(widgets) != 1:
        raise NewsError('article_identity_not_verified')
    widget = widgets[0]
    if (widget.name != 'blockquote' or widget.parent is not body
            or widget.attrs != {'class': ['twitter-tweet']}
            or script.parent is not body or script.contents
            or script.attrs != {'async': '', 'src': 'https://platform.twitter.com/widgets.js', 'charset': 'utf-8'}):
        raise NewsError('article_identity_not_verified')
    sibling = widget.next_sibling
    while type(sibling) is NavigableString and not str(sibling).strip():
        sibling = sibling.next_sibling
    if sibling is not script:
        raise NewsError('article_identity_not_verified')
    _visible_text(widget)
    _visible(script)
    parts = [n for n in widget.contents if not (type(n) is NavigableString and not str(n).strip())]
    if (len(parts) != 3 or type(parts[0]) is not Tag or parts[0].name != 'p'
            or parts[0].attrs != {'lang': 'en', 'dir': 'ltr'}
            or type(parts[1]) is not NavigableString or type(parts[2]) is not Tag
            or parts[2].name != 'a' or set(parts[2].attrs) != {'href'}
            or len(parts[2].contents) != 1 or type(parts[2].contents[0]) is not NavigableString):
        raise NewsError('article_identity_not_verified')
    paragraph, attribution, date_link = parts
    credit = re.fullmatch(r'\s*— ([^\r\n<>]{1,80}) \(@([A-Za-z0-9_]{1,15})\) ', str(attribution))
    post = re.fullmatch(r'https://twitter[.]com/([A-Za-z0-9_]{1,15})/status/([1-9][0-9]{5,24})'
        r'\?ref_src=twsrc%5Etfw', date_link['href'])
    label = str(date_link.contents[0])
    date = re.fullmatch(r'([A-Z][a-z]{2,8}) ([0-9]{1,2}), ([0-9]{4})', label)
    months = ('January', 'February', 'March', 'April', 'May', 'June', 'July',
              'August', 'September', 'October', 'November', 'December')
    if not credit or not post or credit[2] != post[1] or not date or date[1] not in months:
        raise NewsError('article_identity_not_verified')
    try:
        datetime(int(date[3]), months.index(date[1]) + 1, int(date[2]))
    except ValueError:
        raise NewsError('article_identity_not_verified') from None
    descendants = paragraph.find_all(True)
    links = paragraph.find_all('a')
    if (not paragraph.get_text().strip() or len(paragraph.get_text()) > 16000
            or len(descendants) > 64 or len(links) != 1
            or set(links[0].attrs) != {'href'} or links[0].find(True)
            or not re.fullmatch(r'https://t[.]co/[A-Za-z0-9]{1,32}', links[0]['href'])
            or any(n.name not in {'br', 'a'} or n.name == 'br' and (n.attrs or n.contents)
                for n in descendants)
            or any(type(n) is not NavigableString and type(n) is not Tag for n in paragraph.descendants)
            or not any(n.get_text(' ', strip=True) and re.search(r'[\u3400-\u9fff]', n.get_text())
                for n in body.find_all('p', recursive=False))):
        raise NewsError('article_identity_not_verified')
    # HTML input preprocessing normalizes CRLF/CR to LF. lxml applies this
    # while html.parser can retain CR. Label this DOM witness explicitly;
    # it is never the shared original-response byte digest or a post body.
    dom_text = lambda value: value.replace('\r\n', '\n').replace('\r', '\n')
    text = dom_text(widget.get_text())
    notice = {'kind': 'native_static_twitter_quote_declaration',
        'source_blockquote_attributes': dict(widget.attrs), 'source_script_attributes': dict(script.attrs),
        'source_post_link_attributes': dict(date_link.attrs), 'source_quote_link_attributes': dict(links[0].attrs),
        'source_visible_attribution': dom_text(str(attribution)), 'source_post_date_label': label,
        'source_quoted_dom_lf_text_characters': len(text),
        'source_quoted_dom_lf_text_sha256': hashlib.sha256(text.encode()).hexdigest(),
        'text_witness_basis': 'HTML_input_CRLF_CR_to_LF; exact_bytes_in_shared_original',
        'quoted_post_preserved_in_shared_original': True, 'excluded_from_newsroom_body': True,
        'post_credit_is_reporter_attribution': False, 'post_publication_time_inferred': False,
        'external_request_sent': False, 'script_executed': False, 'raw_declarations_preserved': True}
    widget.decompose()
    script.decompose()
    return [notice]


def _static_twitter_loader_declaration(body, *, allow_static_quote=False):
    """Separate one native orphan loader, never a tweet or executable code.

    The source has no accompanying Twitter widget or text. Only its exact
    empty HTTPS loader declaration is separable; unknown scripts, post
    markup, extra attributes, nesting and inline code still fail closed.
    """
    scripts = body.select('script[src="https://platform.twitter.com/widgets.js"]')
    if allow_static_quote and body.select('[class*="twitter"], [data-tweet-id], '
            'iframe[src*="twitter.com"], iframe[src*="x.com"]'):
        if len(scripts) != 1:
            raise NewsError('article_identity_not_verified')
        return _static_twitter_quote_declaration(body, scripts[0])
    if not scripts:
        return []
    if (len(scripts) != 1 or body.select('[class*="twitter"], [data-tweet-id], '
            'iframe[src*="twitter.com"], iframe[src*="x.com"]')):
        raise NewsError('article_identity_not_verified')
    script = scripts[0]
    if (script.parent is not body or script.contents
            or script.attrs != {'async': '', 'src': 'https://platform.twitter.com/widgets.js', 'charset': 'utf-8'}):
        raise NewsError('article_identity_not_verified')
    _visible(script)
    notice = {'kind': 'native_orphan_static_twitter_loader_declaration',
        'source_script_attributes': dict(script.attrs), 'native_post_markup_present': False,
        'external_request_sent': False, 'script_executed': False,
        'post_identity_or_contents_inferred': False, 'raw_declarations_preserved': True}
    script.decompose()
    return [notice]


def _strong_split_reporter_credit(node):
    """Read the proved inline word, never strip arbitrary source whitespace.

    Both exact text nodes survive in the raw original. Only this observed
    strong-wrapped first character and plain remaining signature are joined;
    names, spacing, unrelated inline tags, media and hidden markup are not
    normalized or guessed into a reporter.
    """
    if type(node) is not Tag or node.name != 'p' or node.attrs or len(node.contents) != 2:
        return None
    first, rest = node.contents
    if (type(first) is not Tag or first.name != 'strong' or first.attrs
            or len(first.contents) != 1 or type(first.contents[0]) is not NavigableString
            or str(first.contents[0]) != '記' or type(rest) is not NavigableString):
        return None
    credit = str(first.contents[0]) + str(rest)
    match = _REPORTER.fullmatch(credit)
    return (credit, match) if match else None


def _original_body(body, *, allow_caption_break_credit=False, allow_static_imgur=False,
                   allow_static_threads=False, allow_caption_credit_only=False,
                   allow_multiple_threads=False, allow_split_reporter=False,
                   allow_static_instagram=False, allow_plain_caption_padding=False,
                   allow_static_twitter_loader=False, allow_compact_threads=False,
                   allow_static_twitter_quote=False, allow_static_facebook_video=False,
                   allow_bare_reporter=False, allow_house_newsroom=False, allow_house_image_reporter=False,
                   allow_house_native_report_signature=False):
    """Whole observed story, minus verified UI, credits and image captions.

    Caption/attribution are retained separately. Do not use image alt text,
    descriptions, neighboring stories, hidden text or an arbitrary prose cutoff.
    """
    cleaned = deepcopy(body)
    captions, credit, names, credit_notices = [], None, [], []
    paragraphs = cleaned.find_all('p', recursive=False)
    newsroom = None
    prelude = (_house_image_caption_credit(paragraphs[0], allow_reporter=allow_house_image_reporter,
        allow_trailing_caption_break=allow_house_native_report_signature)
        if (allow_house_newsroom or allow_house_image_reporter) and paragraphs else None)
    image_reporter = prelude is not None and prelude[0] is None
    if prelude is not None:
        newsroom, credit, caption, trailing_caption_break = prelude
        names = [newsroom] if newsroom else _REPORTER.fullmatch(credit)[1].split('、')
        captions = [caption]
        if len(set(names)) != len(names):
            raise _attribution_failure('duplicate_reporters')
        paragraphs[0].clear()
        credit_notices.append({'kind': ('native_house_image_caption_reporter_prefix' if image_reporter
            else 'native_house_image_caption_newsroom_prefix'),
            ('caption_and_reporter_only_no_prose_in_removed_paragraph' if image_reporter
                else 'caption_and_desk_only_no_prose_in_removed_paragraph'): True,
            'following_prose_preserved_in_native_paragraphs': True, 'source_values_rewritten': False})
        if trailing_caption_break:
            credit_notices.append({'kind': 'native_house_caption_trailing_break_reporter_separator',
                'separator_inside_caption_strong': True, 'plain_standalone_named_reporter_after_caption': True,
                'source_values_rewritten': False})
    for index, node in enumerate(paragraphs):
        text = node.get_text(' ', strip=True)
        if (allow_house_native_report_signature and (credit is None or index <= 5)
                and re.match(r'^圖[、，]?文[／/]', text)):
            _visible_text(node)
            raise NewsError('article_rights_or_scope_excluded')
        if (newsroom is not None or image_reporter) and re.match(r'^(?:中央社|路透社|本文來源|來源[：:]|轉載|文[／/])', text):
            raise NewsError('article_rights_or_scope_excluded')
        match = _REPORTER.fullmatch(text)
        desk = _HOUSE_NEWSROOM_CREDITS.get(text) if allow_house_newsroom else None
        if desk:
            leaves = [c for c in node.contents if not isinstance(c, NavigableString) or str(c).strip()]
            if len(leaves) != 1 or type(leaves[0]) is not NavigableString:
                raise _attribution_failure('newsroom_credit_markup')
        if match is None and allow_bare_reporter:
            bare = _BARE_REPORTER.fullmatch(text)
            photo_reporter = bare is not None and any('（圖／記者' + bare[1] + '攝）' in caption for caption in captions)
            leaves = [child for child in node.contents
                if not isinstance(child, NavigableString) or str(child).strip()]
            native_report_signature = (allow_house_native_report_signature and len(leaves) == 1
                and type(leaves[0]) is NavigableString)
            if (bare and not _CREDIT_PREFIX.match(text) and not bare[1].endswith('中心')
                    and (photo_reporter or native_report_signature)):
                match = bare
                credit_notices.append({'kind': ('native_reporter_prefix_omitted_with_role_and_creator_witness'
                    if photo_reporter else 'native_report_signature_with_matching_creator_witness'),
                    'native_credit': text, 'native_reporter_names': [bare[1]],
                    'role_basis': ('same_literal_name_in_preceding_visible_native_reporter_photo_caption'
                        if photo_reporter else 'literal_visible_name_place_report_signature'),
                    'same_article_creator_name_and_identifier_required': True,
                    'prefix_added_or_source_values_rewritten': False})
        if match is None and allow_split_reporter:
            split = _strong_split_reporter_credit(node)
            if split is not None:
                text, match = split
                credit_notices.append({'kind': 'native_strong_split_reporter_word',
                    'source_dom_text_credit': text,
                    'literal_adjacent_text_nodes_without_added_separator': True,
                    'source_whitespace_removed_or_names_invented': False})
        if match or desk:
            if credit is not None or index > 5:
                raise _attribution_failure('duplicate_or_late_credit')
            _visible_text(node)
            credit, names = text, [desk] if desk else match[1].split('、')
            if desk:
                newsroom = desk
            if len(set(names)) != len(names):
                raise _attribution_failure('duplicate_reporters')
            node.decompose()
        elif credit is None and _CREDIT_PREFIX.match(text):
            raise NewsError('article_rights_or_scope_excluded')
        elif text.startswith('▲') and index and paragraphs[index - 1].find('img'):
            _visible_text(node)
            editor_padding = allow_static_threads and _caption_editor_padding(
                node, allow_plain_break=allow_plain_caption_padding)
            if (allow_caption_break_credit and credit is None and node.find('br')
                    and not editor_padding):
                if index > 5:
                    raise _attribution_failure('late_caption_credit')
                names, credit, caption = _caption_break_credit(node,
                    allow_caption_credit_only=allow_caption_credit_only, syntax_notices=credit_notices)
                captions.append(caption)
            else:
                if editor_padding and allow_plain_caption_padding and not _caption_editor_padding(node):
                    credit_notices.append({'kind': 'native_plain_break_caption_editor_padding',
                        'caption_is_strong_text_plain_br_and_trailing_whitespace': True,
                        'reporter_not_inferred_from_caption': True, 'source_values_rewritten': False})
                captions.append(text)
                node.decompose()
        elif credit is None and text:
            # Only the actual image prelude may precede the newsroom credit.
            raise _attribution_failure('unrecognized_precredit_paragraph')
    if credit is None:
        raise _attribution_failure('missing_visible_credit')
    if newsroom is not None:
        credit_notices.append({'kind': 'native_publisher_newsroom_signature', 'native_credit': credit,
            'native_newsroom': newsroom, 'role': 'newsroom', 'person_identity_inferred': False,
            'schema_creator_promoted_to_reporter': False, 'source_values_rewritten': False})
    for node in list(cleaned.select(':scope > div.ad_in_news')):
        labels = node.select(':scope > .ad_readmore')
        if (len(labels) != 1 or labels[0].get_text(' ', strip=True) != _AD_TEXT
                or node.get_text(' ', strip=True) != _AD_TEXT
                or any(child.name not in {'div', 'script'} for child in node.find_all(recursive=False))):
            raise NewsError('article_identity_not_verified')
        node.decompose()
    embed_notices = list(credit_notices)
    if allow_static_facebook_video:
        embed_notices += _static_facebook_video_preview(cleaned)
    if allow_static_instagram:
        from downloader.news_fulltext_static_instagram import separate_native_instagram_preview
        embed_notices += separate_native_instagram_preview(cleaned)
    embed_notices += (_static_threads_declaration(cleaned,
        allow_multiple=allow_multiple_threads, allow_compact_width=allow_compact_threads)
        if allow_static_threads else [])
    if allow_static_twitter_loader:
        embed_notices += _static_twitter_loader_declaration(cleaned, allow_static_quote=allow_static_twitter_quote)
    if allow_static_imgur:
        embed_notices += _static_imgur_declarations(cleaned)
    if cleaned.select('script,style,article,main,.story,[itemprop="articleBody"]'):
        raise NewsError('article_identity_not_verified')
    _visible_text(cleaned)
    return cleaned, names, credit, captions, embed_notices


def ettoday_verified_short_prose(body):
    """Observed complete native flashes have plain direct prose paragraphs.

    Called only after native identity, reporter, visibility, clocks and access
    checks. A caption, image, embed, linked teaser or headline cannot qualify.
    The whole cleaned story is retained; this is not a summary-length cutoff.
    """
    return any(not node.find(True) and re.search(r'[\u3400-\u9fff]', node.get_text())
               for node in body.find_all('p', recursive=False))


def ettoday_visible_article(soup, *, page_url, adapter_contract_version=1, allow_house=False):
    if type(adapter_contract_version) is not int or adapter_contract_version not in range(1, 29):
        raise NewsError('article_identity_not_verified')
    house_scope = adapter_contract_version >= 24 and allow_house is True
    identity = ettoday_request_identity(page_url, allow_finance_redirect_query=adapter_contract_version >= 16,
        allow_house=house_scope)
    if identity is None:
        raise NewsError('article_identity_not_verified')
    host, path = identity
    house = host == 'house.ettoday.net'
    house_native_repairs = house and adapter_contract_version >= 25
    original = 'https://' + host + path
    canonical = _one(soup, 'head > link[rel~="canonical"][itemprop="mainEntityOfPage"][href]')
    ogurl = _one(soup, 'head > meta[property="og:url"][content]')
    ogtitle = _one(soup, 'head > meta[property="og:title"][content]')
    scope = _one(soup, '.subject_article')
    heading = _one(scope, ':scope > header > h1.title[itemprop="headline"]')
    body = _one(scope, ETTODAY_BODY_SELECTOR, 'body_not_found')
    section = _one(soup, 'head > meta[name="subsection"][itemprop="articleSection"][content]').get('content')
    title = heading.get_text(' ', strip=True)
    # v22 admits only the observed WWW pair: articleSection=3C and the
    # social channel=3C家電. Neither label is corrected into the other.
    # An independent unique current schema must corroborate the headline,
    # literal article identity, reporter and both source clocks below.
    native_3c_channel = adapter_contract_version >= 22 and host == ETTODAY_HOSTS[1] and section == '3C'
    channel = '3C家電' if native_3c_channel else section
    social_title = ogtitle.get('content')
    # The subsection and social channel are independently declared source
    # properties, not a taxonomy crosswalk or article identity. Read the
    # publisher's own bounded suffix rather than infer a channel from the
    # subsection (e.g. 汽機車 / 消費, 民生消費 / 消費). A differing label
    # requires the unique same-article schema witness below, just as v22 does.
    # Only the already reviewed WWW route is affected; neither label nor the
    # response is rewritten and no subdomain/reporting right is inferred.
    if adapter_contract_version >= 23 and host == ETTODAY_HOSTS[1] and isinstance(social_title, str):
        declared_channel = re.search(r' \| ETtoday([\u3400-\u9fff]{1,16}|3C家電)新聞 \| ETtoday新聞雲$', social_title)
        if declared_channel:
            channel = declared_channel[1]
    native_channel_labels_distinct = native_3c_channel or channel != section
    suffix = (' | ETtoday房產雲 | ETtoday新聞雲' if house else
        ' | ETtoday財經雲 | ETtoday新聞雲' if host == ETTODAY_HOSTS[0] else f' | ETtoday{channel}新聞 | ETtoday新聞雲')
    # The social object title and the visible article headline are different
    # source fields. Native 3250308/3246970 publish distinct labels. Permit a
    # nonempty social label only in v7, with the exact publisher suffix AND a
    # unique current JSON-LD article corroborating one declared headline,
    # canonical URL, reporter and both clocks below. Never borrow a
    # description/neighboring title, use fuzzy matching, or rewrite either.
    headline_matches = _key(social_title) == _key(title + suffix)
    social_variant = (adapter_contract_version >= 7 and not headline_matches
        and isinstance(social_title, str) and _key(social_title).endswith(_key(suffix))
        and bool(_key(social_title)[:-len(_key(suffix))]))
    if (canonical.get('href') != original or ogurl.get('content') != original or not title
            or not isinstance(section, str) or not (re.fullmatch(r'[\u3400-\u9fff]{1,16}', section) or native_3c_channel)
            or not (headline_matches or social_variant)
            or len(soup.select('.story[itemprop="articleBody"]')) != 1
            or house and section != '房產'
            or soup.html.get('itemtype') not in {'http://schema.org/NewsArticle', 'https://schema.org/NewsArticle'}):
        raise NewsError('article_identity_not_verified')
    _visible_text(heading)
    _visible(body)
    cleaned, names, credit, captions, embed_notices = _original_body(body,
        allow_caption_break_credit=adapter_contract_version >= 8 and host == ETTODAY_HOSTS[0],
        allow_static_imgur=adapter_contract_version >= 9 and host == ETTODAY_HOSTS[0],
        allow_static_threads=adapter_contract_version >= 10 and host == ETTODAY_HOSTS[0],
        allow_caption_credit_only=adapter_contract_version >= 11 and host == ETTODAY_HOSTS[0],
        allow_multiple_threads=adapter_contract_version >= 12 and host == ETTODAY_HOSTS[0],
        allow_split_reporter=adapter_contract_version >= 13 and host == ETTODAY_HOSTS[0],
        allow_static_instagram=adapter_contract_version >= 14 and host == ETTODAY_HOSTS[0],
        allow_plain_caption_padding=adapter_contract_version >= 15 and host == ETTODAY_HOSTS[0] or house,
        allow_static_twitter_loader=adapter_contract_version >= 15 and host == ETTODAY_HOSTS[0] or house_native_repairs,
        allow_compact_threads=adapter_contract_version >= 15 and host == ETTODAY_HOSTS[0],
        allow_static_twitter_quote=adapter_contract_version >= 19 and host == ETTODAY_HOSTS[0] or house_native_repairs,
        allow_static_facebook_video=adapter_contract_version >= 20 and host == ETTODAY_HOSTS[0],
        allow_bare_reporter=house_native_repairs,
        allow_house_newsroom=house and adapter_contract_version >= 26,
        allow_house_image_reporter=house and adapter_contract_version >= 27,
        allow_house_native_report_signature=house and adapter_contract_version >= 28)
    newsroom = next((n['native_newsroom'] for n in embed_notices
        if n['kind'] == 'native_publisher_newsroom_signature'), None)
    published, modified, notices = _clocks(soup, scope, host, adapter_contract_version=adapter_contract_version)
    notices += _schemas(soup, original, title, published, modified, names,
        adapter_contract_version=adapter_contract_version, require_headline_witness=social_variant or native_channel_labels_distinct or house,
        social_headline_key=_key(social_title)[:-len(_key(suffix))] if social_variant else None,
        native_bare_reporters=next((tuple(n['native_reporter_names']) for n in embed_notices
            if n['kind'] in {'native_reporter_prefix_omitted_with_role_and_creator_witness',
                'native_report_signature_with_matching_creator_witness'}), ()),
        native_newsroom=newsroom is not None)
    notices += embed_notices
    if native_channel_labels_distinct:
        notices.append({'kind': 'native_subsection_and_social_channel_labels_distinct',
            'native_subsection': section, 'native_social_channel': channel,
            'native_social_title': social_title,
            'identity_basis': 'same_literal_canonical_og_url_unique_current_schema_and_visible_reporter_story',
            'source_labels_rewritten': False, 'additional_source_host_admitted': False})
    if social_variant:
        notices.append({'kind': 'native_social_headline_differs_with_same_article_schema_witness',
            'native_headline': title, 'social_title': social_title,
            'identity_basis': 'same_literal_canonical_og_url_unique_current_schema_and_visible_story',
            'source_reason_inferred': False, 'source_titles_rewritten': False})
    notices.append({'kind': 'native_newsroom_credit' if newsroom else 'native_reporter_credit', 'text': credit})
    if house:
        notice = _one(scope, ':scope > p.note', 'article_rights_or_scope_excluded')
        # Bind the native notice to the publisher's actual official policy
        # link. This is a private scope review, not a per-article licence or
        # permission to distribute, train, scrape other hosts or load media.
        links = notice.select('a[href]')
        if len(links) != 1 or links[0].get('href') != 'https://www.ettoday.net/member/clause_copyright.php':
            raise NewsError('article_rights_or_scope_excluded')
    for node in scope.select(':scope > p.note'):
        _visible_text(node)
        notice = node.get_text(' ', strip=True)
        if re.search(r'禁止(?:下載|抓取)|不得(?:下載|抓取)', notice):
            raise NewsError('article_rights_or_scope_excluded')
        notices.append({'kind': 'native_article_rights_notice', 'text': notice})
    article = {'headline': title, 'author': [{'@type': 'Organization' if newsroom else 'Person', 'name': name} for name in names],
        'datePublished': published, 'dateModified': None if modified == _REVISION_PLACEHOLDER else modified,
        'published_time_basis': 'native_pubdate_offset_seconds_verified_against_visible_release_minute; not_capture_time',
        'article_identity_basis': ('literal_host_path_canonical_og_microdata_headline_native_newsroom_and_release'
            if newsroom else 'literal_host_path_canonical_og_microdata_headline_native_reporter_and_release'),
        'body_completeness': ('verified_whole_native_story; source_newsroom_captions_and_rights_notices_retained_separately'
            if newsroom else 'verified_whole_native_story; source_reporter_captions_and_rights_notices_retained_separately'),
        'author_attribution_detail_json': json.dumps([{'name': name, 'role': 'newsroom' if newsroom else 'reporter',
            'native_credit': credit, 'basis': ('visible_current_story_publisher_newsroom_signature'
                if newsroom else 'visible_current_story_reporter_signature')} for name in names], ensure_ascii=False),
        'verified_image_captions_json': json.dumps(captions, ensure_ascii=False),
        'verified_article_notices_json': json.dumps(notices, ensure_ascii=False)}
    if any(n['kind'] == 'native_empty_static_imgur_declaration' for n in embed_notices):
        article['body_completeness'] += '; native_empty_imgur_declarations_separate; external_media_not_requested_or_transcribed'
    if any(n['kind'] == 'native_static_threads_preview_ui_declaration' for n in embed_notices):
        article['body_completeness'] += '; native_threads_preview_ui_separate; embedded_post_not_requested_or_transcribed'
    if any(n['kind'] == 'native_static_instagram_preview_ui_declaration' for n in embed_notices):
        article['body_completeness'] += '; native_instagram_preview_ui_separate; embedded_post_not_requested_or_transcribed'
    if any(n['kind'] == 'native_orphan_static_twitter_loader_declaration' for n in embed_notices):
        article['body_completeness'] += '; native_orphan_twitter_loader_separate; embedded_post_not_requested_or_transcribed'
    if any(n['kind'] == 'native_static_twitter_quote_declaration' for n in embed_notices):
        article['body_completeness'] += ('; native_attributed_twitter_quote_separate; '
            'quoted_post_preserved_in_raw_not_newsroom_body; external_post_not_requested')
    if any(n['kind'] == 'native_static_facebook_video_preview_declaration' for n in embed_notices):
        article['body_completeness'] += ('; native_facebook_video_preview_separate; '
            'video_label_preserved_in_raw_not_newsroom_body; external_media_not_requested_or_transcribed')
    # Do not copy the whole subject and the story a second time. All metadata
    # was validated against the original document above; generic extraction
    # needs only its verified heading and one unchanged-order cleaned story.
    wrapper = Tag(name='div')
    wrapper.append(deepcopy(heading))
    wrapper.append(cleaned)
    return article, wrapper


def ettoday_validation_metadata(soup, *, page_url, adapter_contract_version=1, allow_house=False):
    """Bounded failure anatomy on the already fetched response, not an article.

    Reuse the owner's persisted daily diagnostic reservation. Export numeric
    clock declarations and credit/schema shapes only, never prose, names,
    scripts, raw HTML, credentials or permission to retain a failed article.
    """
    identity = ettoday_request_identity(page_url, allow_finance_redirect_query=adapter_contract_version >= 16,
        allow_house=adapter_contract_version >= 24 and allow_house is True)
    facts = {'contract_version': 3 if adapter_contract_version >= 5 else 2,
        'literal_route_verified': identity is not None,
        'body_text_retained': False, 'raw_scope_or_article_acceptance_granted': False}
    if identity is None:
        return facts
    compact_hash = lambda value: hashlib.sha256(_key(value).encode()).hexdigest() if isinstance(value, str) else None
    safe_clock = lambda value: value if isinstance(value, str) and _CLOCK.fullmatch(value) else None
    stories = soup.select('.subject_article > .story[itemprop="articleBody"]')
    credits = []
    for story in stories[:4]:
        for index, node in enumerate(story.find_all('p', recursive=False)[:8]):
            text = node.get_text(' ', strip=True)
            match = _REPORTER.fullmatch(text)
            if match or _CREDIT_PREFIX.match(text):
                credits.append({'paragraph_index': index, 'native_reporter_signature_shape': bool(match),
                    'reporter_count': len(match[1].split('、')) if match else None,
                    'credit_compact_sha256': compact_hash(text)})
    releases = soup.select('time[itemprop="datePublished"]')
    clocks = []
    for node in releases[:4]:
        text = node.get_text(' ', strip=True)
        minute = bool(re.fullmatch(r'(?:[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}|'
            r'[0-9]{4}年[0-9]{2}月[0-9]{2}日 [0-9]{2}:[0-9]{2})', text))
        clocks.append({'visible_minute': text if minute else None,
            'native_datetime': safe_clock(node.get('datetime')),
            'native_datetime_numeric': node.get('datetime') if isinstance(node.get('datetime'), str)
                and re.fullmatch(r'[0-9]{1,2}', node.get('datetime')) else None,
            'observed_finance_datetime_31': node.get('datetime') == '31'})
    declarations = []
    syntax, schema_error = [], False
    scripts = soup.select('script[type="application/ld+json"]')
    if len(scripts) > 16:
        schema_error = True
    else:
        try:
            for script in scripts:
                if len(script.get_text()) > 262144:
                    raise ValueError('bounded schema required')
                value, anomalies = _native_json(script.get_text(),
                    allow_literal_value_controls=adapter_contract_version >= 5,
                    allow_author_url_comment=adapter_contract_version >= 18)
                syntax.extend(anomalies)
                declarations.extend(obj for obj in _objects(value) if obj.get('@type') in {'NewsArticle', 'Article'})
                if len(declarations) > 8:
                    raise ValueError('bounded declarations required')
        except (ValueError, TypeError, RecursionError):
            schema_error = True
    facts.update(host=identity[0], native_story_count=len(stories), native_credit_shapes=credits,
        story_credit_anatomy=[ettoday_credit_anatomy(story) for story in stories[:4]],
        native_release_node_count=len(releases), native_clock_labels=clocks,
        declared_publication=[safe_clock(n.get('content')) for n in soup.select('head > meta[name="pubdate"]')[:4]],
        declared_modification=[safe_clock(n.get('content')) for n in soup.select('head > meta[name="lastmod"]')[:4]],
        schema_error=schema_error, article_schema_count=len(declarations),
        schema_publication=[safe_clock(obj.get('datePublished')) for obj in declarations[:8]],
        schema_modification=[safe_clock(obj.get('dateModified')) for obj in declarations[:8]],
        observed_schema_syntax_anomalies=sorted(set(syntax)),
        metadata_truncated=len(stories) > 4 or len(releases) > 4 or len(declarations) > 8,
        declared_author_types=[[a.get('@type') if isinstance(a, dict) and a.get('@type') in {'Person', 'Organization'}
            else 'unknown' for a in (obj.get('author') if isinstance(obj.get('author'), list)
                else [obj.get('author')])[:8]] for obj in declarations[:8]])
    return facts


def ettoday_credit_anatomy(story):
    """Bounded attribution *diagnosis*, never a relaxed attribution parser.

    The old eight-direct-paragraph sample cannot establish that a whole story
    lacks a credit. Inspect up to 128 paragraph blocks, including nested ones,
    but export only shapes/counts/hashes and visibility outcomes. A whitespace
    or embedded match is a lead, not permission to accept or retain that text.
    A bounded sample cannot establish absence outside its examined scope.
    """
    paragraphs = story.find_all('p', limit=129)
    rows, candidate_count = [], 0
    direct_index = 0
    for index, node in enumerate(paragraphs[:128]):
        direct = node.parent is story
        position = direct_index if direct else None
        direct_index += int(direct)
        text = node.get_text(' ', strip=True)
        exact = _REPORTER.fullmatch(text)
        embedded = _REPORTER.search(text)
        compact = _REPORTER.fullmatch(re.sub(r'\s+', '', text))
        credit_prefix = bool(_CREDIT_PREFIX.match(text))
        candidate = bool(exact or embedded or compact or credit_prefix)
        candidate_count += int(candidate)
        # Keep the initial structure even without a credit, plus a bounded
        # set of later leads. Do not emit source-controlled classes or text.
        if index >= 8 and (not candidate or len(rows) >= 32):
            continue
        try:
            _visible_text(node)
            visible = True
        except NewsError:
            visible = False
        rows.append({'paragraph_index': index, 'direct_paragraph_index': position,
            'direct_child': direct, 'text_characters': len(text), 'visible': visible,
            'starts_caption_marker': text.startswith('▲'), 'credit_prefix': credit_prefix,
            'exact_reporter_signature': bool(exact),
            'embedded_reporter_signature': bool(embedded),
            'embedded_signature_starts_at_zero': embedded.start() == 0 if embedded else None,
            'whitespace_stripped_signature': bool(compact),
            'reporter_count': len((exact or embedded or compact)[1].split('、')) if candidate
                and (exact or embedded or compact) else None,
            'line_breaks': len(node.find_all('br')),
            'text_sha256': hashlib.sha256(text.encode()).hexdigest()})
    return {'contract_version': 1, 'paragraphs_examined': min(128, len(paragraphs)),
        'paragraph_scan_truncated': len(paragraphs) > 128,
        'candidate_paragraphs_examined': candidate_count, 'paragraph_shapes': rows,
        'candidate_rows_truncated': candidate_count > sum(bool(r['exact_reporter_signature']
            or r['embedded_reporter_signature'] or r['whitespace_stripped_signature']
            or r['credit_prefix']) for r in rows),
        'body_or_author_text_exported': False, 'attribution_verified_by_diagnostic': False}
