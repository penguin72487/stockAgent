"""Closed native Instagram preview UI, not the remote post or news prose.

ETtoday's observed original contains an empty v14 reel preview with placeholder
bars, a decorative icon, one fixed UI label and one public share-credit link.
Validate the entire tree before separating either declaration. Nothing here
executes JavaScript, fetches Instagram, infers its post contents or makes the
social account the publisher's reporter. Unreviewed widgets fail closed.
"""
import hashlib
import re

from bs4 import NavigableString, Tag

from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_public_originals import _visible


_ICON_SHA256 = 'a31ab4deb40d95a90f1c4dacb72a66bef886ace269aa5e95c96e26df7df56c31'
_ICON_CHARACTERS = 2697
_PERMALINK = re.compile(r'https://www[.]instagram[.]com/reel/[A-Za-z0-9_-]{11}/'
    r'\?utm_source=ig_embed&utm_campaign=loading')
_SHARE_CREDIT = re.compile(r'[^\x00-\x1f<>]{1,80}（@[A-Za-z0-9_.]{1,30}）分享的貼文')


def _node(tag, attrs, *children):
    return tag, attrs, children


def _div(style, *children):
    return _node('div', {'style': style}, *children)


def _preview_tree(permalink, credit):
    """Every observed descendant/attribute; dynamic fields stay source-bound.

    Styles differ only in formatting whitespace. The SVG is a bounded exact
    decorative path digest, so no downloaded media fixture is embedded in code.
    """
    return _node('blockquote', {'class': ['instagram-media'], 'data-instgrm-permalink': permalink,
        'data-instgrm-version': '14', 'style': 'background:#FFF;border:0;border-radius:3px;'
        'box-shadow:0 0 1px 0 rgba(0,0,0,0.5),0 1px 10px 0 rgba(0,0,0,0.15);margin:1px;'
        'max-width:540px;min-width:326px;padding:0;width:99.375%;'
        'width:-webkit-calc(100% - 2px);width:calc(100% - 2px);'},
        _div('padding:16px;',
            _node('a', {'href': permalink, 'target': '_blank', 'style': 'background:#FFFFFF;'
                'line-height:0;padding:0 0;text-align:center;text-decoration:none;width:100%;'},
                _div('display:flex;flex-direction:row;align-items:center;',
                    _div('background-color:#F4F4F4;border-radius:50%;flex-grow:0;height:40px;margin-right:14px;width:40px;'),
                    _div('display:flex;flex-direction:column;flex-grow:1;justify-content:center;',
                        _div('background-color:#F4F4F4;border-radius:4px;flex-grow:0;height:14px;margin-bottom:6px;width:100px;'),
                        _div('background-color:#F4F4F4;border-radius:4px;flex-grow:0;height:14px;width:60px;'))),
                _div('padding:19% 0;'),
                _div('display:block;height:50px;margin:0 auto 12px;width:50px;',
                    _node('svg', {'width': '50px', 'height': '50px', 'viewbox': '', 'version': '1.1',
                        'xmlns': 'https://www.w3.org/2000/svg', 'xmlns:xlink': 'https://www.w3.org/1999/xlink'},
                        _node('g', {'stroke': 'none', 'stroke-width': '1', 'fill': 'none', 'fill-rule': 'evenodd'},
                            _node('g', {'transform': 'translate(-511.000000, -20.000000)', 'fill': '#000000'},
                                _node('g', {}, _node('path', {'d': '$verified_decorative_icon'})))))),
                _div('padding-top:8px;',
                    _div('color:#3897f0;font-family:Arial,sans-serif;font-size:14px;'
                        'font-style:normal;font-weight:550;line-height:18px;', '在 Instagram 查看這則貼文')),
                _div('padding:12.5% 0;'),
                _div('display:flex;flex-direction:row;margin-bottom:14px;align-items:center;',
                    _node('div', {},
                        _div('background-color:#F4F4F4;border-radius:50%;height:12.5px;width:12.5px;'
                            'transform:translateX(0px) translateY(7px);'),
                        _div('background-color:#F4F4F4;height:12.5px;'
                            'transform:rotate(-45deg) translateX(3px) translateY(1px);'
                            'width:12.5px;flex-grow:0;margin-right:14px;margin-left:2px;'),
                        _div('background-color:#F4F4F4;border-radius:50%;height:12.5px;width:12.5px;'
                            'transform:translateX(9px) translateY(-18px);')),
                    _div('margin-left:8px;',
                        _div('background-color:#F4F4F4;border-radius:50%;flex-grow:0;height:20px;width:20px;'),
                        _div('width:0;height:0;border-top:2px solid transparent;border-left:6px solid #f4f4f4;'
                            'border-bottom:2px solid transparent;transform:translateX(16px) translateY(-4px) rotate(30deg)')),
                    _div('margin-left:auto;',
                        _div('width:0px;border-top:8px solid #F4F4F4;border-right:8px solid transparent;transform:translateY(16px);'),
                        _div('background-color:#F4F4F4;flex-grow:0;height:12px;width:16px;transform:translateY(-4px);'),
                        _div('width:0;height:0;border-top:8px solid #F4F4F4;border-left:8px solid transparent;'
                            'transform:translateY(-4px) translateX(8px);'))),
                _div('display:flex;flex-direction:column;flex-grow:1;justify-content:center;margin-bottom:24px;',
                    _div('background-color:#F4F4F4;border-radius:4px;flex-grow:0;height:14px;margin-bottom:6px;width:224px;'),
                    _div('background-color:#F4F4F4;border-radius:4px;flex-grow:0;height:14px;width:144px;'))),
            _node('p', {'style': 'color:#c9c8cd;font-family:Arial,sans-serif;font-size:14px;line-height:17px;'
                'margin-bottom:0;margin-top:8px;overflow:hidden;padding:8px 0 7px;text-align:center;'
                'text-overflow:ellipsis;white-space:nowrap;'},
                _node('a', {'href': permalink, 'target': '_blank', 'style': 'color:#c9c8cd;font-family:Arial,sans-serif;'
                    'font-size:14px;font-style:normal;font-weight:normal;line-height:17px;text-decoration:none;'}, credit))))


def _reject():
    raise NewsError('article_identity_not_verified')


def _validate_tree(node, expected):
    if isinstance(expected, str):
        if type(node) is not NavigableString or str(node).strip() != expected:
            _reject()
        return
    tag, expected_attrs, expected_children = expected
    if type(node) is not Tag or node.name != tag:
        _reject()
    attrs = dict(node.attrs)
    if 'd' in attrs and tag == 'path':
        value = attrs['d']
        if (not isinstance(value, str) or len(value) != _ICON_CHARACTERS
                or hashlib.sha256(value.encode()).hexdigest() != _ICON_SHA256):
            _reject()
        attrs['d'] = '$verified_decorative_icon'
    styles = dict(expected_attrs)
    for values in (attrs, styles):
        if isinstance(values.get('style'), str):
            values['style'] = re.sub(r'\s+', '', values['style'])
    if attrs != styles:
        _reject()
    _visible(node)
    children = [c for c in node.contents if type(c) is not NavigableString or str(c).strip()]
    if len(children) != len(expected_children):
        _reject()
    for child, contract in zip(children, expected_children):
        _validate_tree(child, contract)


def separate_native_instagram_preview(body):
    """Separate one exact direct widget/script pair in a working-copy story.

    Other scripts must still pass their own provider validators. On failure no
    node is removed. The original response and both source declarations remain
    unchanged in raw storage; the public share label is a sidecar, not a byline.
    """
    widgets = body.select('.instagram-media')
    if not widgets:
        return []
    if len(widgets) != 1:
        _reject()
    widget = widgets[0]
    permalink = widget.get('data-instgrm-permalink')
    script = widget.next_sibling
    while type(script) is NavigableString and not str(script).strip():
        script = script.next_sibling
    if (not isinstance(permalink, str) or not _PERMALINK.fullmatch(permalink)
            or widget.parent is not body or type(script) is not Tag or script.parent is not body
            or script.name != 'script' or script.contents
            or script.attrs not in ({'async': '', 'src': '//www.instagram.com/embed.js'},
                {'async': '', 'src': 'https://www.instagram.com/embed.js'})):
        _reject()
    _visible(script)
    credit_nodes = widget.select(':scope > div > p > a')
    if len(credit_nodes) != 1 or len(credit_nodes[0].contents) != 1:
        _reject()
    credit = credit_nodes[0].get_text().strip()
    if not _SHARE_CREDIT.fullmatch(credit):
        _reject()
    _validate_tree(widget, _preview_tree(permalink, credit))
    notice = {'kind': 'native_static_instagram_preview_ui_declaration', 'contract_version': 14,
        'source_blockquote_attributes': dict(widget.attrs), 'source_script_attributes': dict(script.attrs),
        'source_share_credit': credit, 'share_credit_is_reporter_attribution': False,
        'source_decorative_icon_sha256': _ICON_SHA256,
        'raw_declarations_preserved': True, 'external_request_sent': False,
        'script_executed': False, 'embedded_post_content_accepted': False}
    widget.decompose()
    script.decompose()
    return [notice]
