"""Bounded same-article image-only forests; never narrative, OCR or media fetching."""
from __future__ import annotations

import html
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Comment, NavigableString, Tag


def _native_ad_placeholder(node):
    """Only the entire observed GPT loading declaration, never a class hint.

    The canonical text extractor already excludes these named ad slots.
    Verify the wrapper, dimensions and blank skeleton before excluding it
    from image-forest equality; unknown text/widgets/fields stay a gap.
    """
    settings = {
        'i1y9ycug': ('div-gpt-ad-cnyes_news_article_middle_300*250',
            '--a1mzdm1v-0:300px;--a1mzdm1v-1:250px;--a1mzdm1v-2:300px;--a1mzdm1v-3:250px;'
            '--a1mzdm1v-4:0;--a1mzdm1v-5:0;--a1mzdm1v-6:auto;--a1mzdm1v-7:auto'),
        'ia49rhf': ('div-gpt-ad-cnyes_news_article_middle_1',
            '--a1mzdm1v-0:728px;--a1mzdm1v-1:90px;--a1mzdm1v-2:728px;--a1mzdm1v-3:90px;'
            '--a1mzdm1v-4:32px;--a1mzdm1v-5:32px;--a1mzdm1v-6:auto;--a1mzdm1v-7:auto'),
    }
    if (node.name != 'div' or set(node.attrs) != {'class'} or len(node.get('class', [])) != 1
            or node['class'][0] not in settings or len(node.contents) != 1):
        return False
    ad_id, style = settings[node['class'][0]]
    slot = node.contents[0]
    if (not isinstance(slot, Tag) or slot.name != 'div'
            or slot.attrs != {'class': ['a1mzdm1v'], 'id': ad_id, 'style': style} or len(slot.contents) != 1):
        return False
    busy = slot.contents[0]
    if (not isinstance(busy, Tag) or busy.name != 'span'
            or busy.attrs != {'aria-busy': 'true', 'aria-live': 'polite'} or len(busy.contents) != 2):
        return False
    skeleton, newline = busy.contents
    return bool(isinstance(skeleton, Tag) and skeleton.name == 'span'
        and skeleton.attrs == {'class': ['react-loading-skeleton'], 'style': 'border-radius:6px;width:100%;height:100%'}
        and len(skeleton.contents) == 1 and type(skeleton.contents[0]) is NavigableString
        and str(skeleton.contents[0]) == '\u200c' and isinstance(newline, Tag)
        and newline.name == 'br' and not newline.attrs and not newline.contents)


def _measured_image_dimensions(attributes):
    """Validate source dimensions without erasing their original attributes.

    Native 2026844 declares 600x358, whereas the earlier measured chart was
    600x546. Dimensions are presentation metadata, not a fixed article type.
    Require bounded canonical positive integers and the source's exact
    width/height ratio string; forest equality still compares every raw field.
    Never infer dimensions from image bytes, request media or accept NaN/inf.
    """
    if set(attributes) != {'src', 'data-height', 'data-width', 'data-ratio'}:
        return False
    dimensions = []
    for key in ('data-width', 'data-height'):
        value = attributes[key]
        if not isinstance(value, str) or re.fullmatch(r'[1-9][0-9]{0,4}', value) is None:
            return False
        dimension = int(value)
        if dimension > 32768:
            return False
        dimensions.append(dimension)
    return attributes['data-ratio'] == str(dimensions[0] / dimensions[1])


def same_image_only_view(container, full_content, source, article_id, visible):
    """Match a bounded whole forest, never turn image alt into narrative."""
    if not isinstance(full_content, str) or len(full_content) > 65536:
        return False
    current = BeautifulSoup(html.unescape(full_content), source.html_parser)
    if current.head is not None:
        return False
    current_root = current.body or current
    count = [0, 0]

    def image(node):
        attributes = node.attrs
        if not isinstance(attributes.get('src'), str) or node.contents:
            return None
        src = urlsplit(attributes['src'])
        if (src.scheme not in {'http', 'https'} or src.netloc != 'cimg.cnyes.cool'
                or src.query or src.fragment or re.fullmatch(
                    '/prod/news/' + str(article_id) + '/[ol]/[0-9a-f]{32}[.]jpg', src.path) is None):
            return None
        keys = set(attributes)
        ordinary = keys == {'src', 'alt'} and isinstance(attributes['alt'], str)
        fixed_width = (keys == {'src', 'alt', 'width'} and isinstance(attributes['alt'], str)
                       and attributes['width'] == '440')
        styled_chart = (keys == {'src', 'alt', 'style', 'border', 'hspace', 'width', 'height'}
            and isinstance(attributes['alt'], str)
            and attributes['style'] == 'height: 1420px; width: 300px; filter: none; border: #aaaaaa 1px solid;'
            and attributes['border'] == attributes['hspace'] == '0'
            and attributes['width'] == '300' and attributes['height'] == '1420')
        measured_chart = _measured_image_dimensions(attributes)
        plain_border = keys == {'src', 'border'} and attributes['border'] == '0'
        measured_border = (keys == {'src', 'border', 'alt', 'width', 'height'}
            and attributes['border'] == '0' and isinstance(attributes['alt'], str)
            and attributes['width'] == '590' and attributes['height'] == '725')
        historic_border = (keys == {'src', 'style', 'border', 'alt'}
            and attributes['border'] == '1' and isinstance(attributes['alt'], str)
            and attributes['style'] == 'BORDER-BOTTOM: #000000 1px solid; BORDER-LEFT: #000000 1px solid; '
                'BORDER-TOP: #000000 1px solid; BORDER-RIGHT: #000000 1px solid')
        if not (ordinary or fixed_width or styled_chart or measured_chart
                or plain_border or measured_border or historic_border):
            return None
        return ('img', tuple(sorted(attributes.items())), ())

    def forest(nodes, native, depth=0):
        if depth > 8:
            raise ValueError('unreviewed_image_depth')
        result = []
        for node in nodes:
            count[native] += 1
            if count[native] > 192 or isinstance(node, Comment):
                raise ValueError('unreviewed_image_node_or_size')
            if isinstance(node, NavigableString):
                if node.strip():
                    raise ValueError('image_view_has_text')
                continue
            if not isinstance(node, Tag):
                raise ValueError('unreviewed_image_node')
            if native:
                visible(node)
            if native and _native_ad_placeholder(node):
                continue
            if node.name == 'img':
                token = image(node)
                if token is None:
                    raise ValueError('unreviewed_source_image')
                result.append(token)
                continue
            children = forest(node.contents, native, depth + 1)
            if native and node.name == 'section' and node.attrs == {'style': 'margin-top:30px'}:
                result.extend(children)  # Only the observed renderer wrapper.
            elif node.name == 'div' and not node.attrs:
                if children:
                    result.append(('div', (), children))
            elif node.name == 'p' and (not node.attrs or node.attrs == {'align': 'center'}
                                      or node.attrs == {'class': ['p_center']}
                                      or node.attrs == {'style': 'text-align: center;'}):
                if children:
                    # Every reviewed paragraph contains images and whitespace,
                    # not arbitrary nested/mixed media or stripped prose.
                    if any(token[0] != 'img' for token in children):
                        raise ValueError('mixed_image_paragraph')
                    attrs = tuple((key, tuple(value) if isinstance(value, list) else value)
                                  for key, value in sorted(node.attrs.items()))
                    result.append(('p', attrs, children))
                elif node.attrs:
                    # Retain exact empty alignment fields on both forests;
                    # deleting them would mask a changed native structure.
                    attrs = tuple((key, tuple(value) if isinstance(value, list) else value)
                                  for key, value in sorted(node.attrs.items()))
                    result.append(('p', attrs, ()))
            else:
                raise ValueError('unreviewed_image_forest')
        return tuple(result)

    try:
        expected = forest(current_root.contents, False)
        actual = forest(container.contents, True)
        images = current_root.find_all('img')
        return bool(expected and expected == actual and 1 <= len(images) <= 32)
    except (ValueError, TypeError, AttributeError):
        return False
