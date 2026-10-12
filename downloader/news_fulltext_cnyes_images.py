"""Bounded same-article image-only forests; never narrative, OCR or media fetching."""
from __future__ import annotations

import html
import re
from urllib.parse import urlsplit

from bs4 import BeautifulSoup, Comment, NavigableString, Tag


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
                    '/prod/news/' + str(article_id) + '/o/[0-9a-f]{32}[.]jpg', src.path) is None):
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
        measured_chart = (keys == {'src', 'data-height', 'data-width', 'data-ratio'}
            and attributes['data-height'] == '546' and attributes['data-width'] == '600'
            and attributes['data-ratio'] == '1.098901098901099')
        if not (ordinary or fixed_width or styled_chart or measured_chart):
            return None
        return ('img', tuple(sorted(attributes.items())), ())

    def forest(nodes, native, depth=0):
        if depth > 8:
            raise ValueError('unreviewed_image_depth')
        result = []
        for node in nodes:
            count[native] += 1
            if count[native] > 64 or isinstance(node, Comment):
                raise ValueError('unreviewed_image_node_or_size')
            if isinstance(node, NavigableString):
                if node.strip():
                    raise ValueError('image_view_has_text')
                continue
            if not isinstance(node, Tag):
                raise ValueError('unreviewed_image_node')
            if native:
                visible(node)
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
                                      or node.attrs == {'class': ['p_center']}):
                if children:
                    # Every reviewed paragraph contains images and whitespace,
                    # not arbitrary nested/mixed media or stripped prose.
                    if any(token[0] != 'img' for token in children):
                        raise ValueError('mixed_image_paragraph')
                    attrs = tuple((key, tuple(value) if isinstance(value, list) else value)
                                  for key, value in sorted(node.attrs.items()))
                    result.append(('p', attrs, children))
                elif node.attrs:
                    raise ValueError('empty_attributed_paragraph')
            else:
                raise ValueError('unreviewed_image_forest')
        return tuple(result)

    try:
        expected = forest(current_root.contents, False)
        actual = forest(container.contents, True)
        images = current_root.find_all('img')
        return bool(expected and expected == actual and len(images) in {1, 3})
    except (ValueError, TypeError, AttributeError):
        return False
