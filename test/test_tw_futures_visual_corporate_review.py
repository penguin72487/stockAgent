import hashlib

import pymupdf
import pytest

from scripts.audit_tw_futures_visual_corporate_review import verify_reference_image, verify_review_renders


def test_review_image_hash_must_match_the_actual_pinned_pdf_page():
    document = pymupdf.open()
    document.new_page(width=100, height=100)
    document.new_page(width=120, height=120)
    pdf = document.tobytes()
    image_sha = hashlib.sha256(document[0].get_pixmap(dpi=100, alpha=False).tobytes('png')).hexdigest()
    document.close()
    review = {'pages': [{'page': 1, 'render_dpi': 100, 'image_sha256': image_sha}]}
    verify_review_renders(pdf, review)
    review['pages'][0]['page'] = 2
    with pytest.raises(ValueError, match='original page'):
        verify_review_renders(pdf, review)


def test_rounding_reference_binds_pdf_image_and_page(tmp_path):
    document = pymupdf.open()
    document.new_page(width=100, height=100)
    document.new_page(width=120, height=120)
    pdf = document.tobytes()
    image = document[0].get_pixmap(dpi=100, alpha=False).tobytes('png')
    document.close()
    (tmp_path / 'page.png').write_bytes(image)
    reference = {'content_sha256': hashlib.sha256(pdf).hexdigest(),
                 'image_sha256': hashlib.sha256(image).hexdigest(),
                 'image_path': 'page.png', 'page': 1, 'render_dpi': 100}
    assert verify_reference_image(pdf, tmp_path, reference) == tmp_path / 'page.png'
    with pytest.raises(ValueError, match='content hash'):
        verify_reference_image(pdf + b'extra', tmp_path, reference)
    with pytest.raises(ValueError, match='original page'):
        verify_reference_image(pdf, tmp_path, dict(reference, page=2))
    with pytest.raises(ValueError, match='source-bound receipt'):
        verify_reference_image(pdf, tmp_path, dict(reference, image_path='../page.png'))
