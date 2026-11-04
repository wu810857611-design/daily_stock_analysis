"""Real PDF decoding, with the same missing Adobe GB1 map as official HK forms."""
import io

import pytest
from pypdf import PdfWriter
from pypdf.generic import (
    ArrayObject, DecodedStreamObject, DictionaryObject, NameObject,
    NumberObject, TextStringObject,
)

from src.services.research_evidence import announcement_excerpt


def cid_pdf(*, codes=(17, 19, 17, 19, 17), pages=1, identity_page=0, prefix=0, ordering='GB1'):
    writer = PdfWriter()
    descendant = DictionaryObject({
        NameObject('/Type'): NameObject('/Font'),
        NameObject('/Subtype'): NameObject('/CIDFontType0'),
        NameObject('/BaseFont'): NameObject('/AdobeHeitiStd-Regular'),
        NameObject('/CIDSystemInfo'): DictionaryObject({
            NameObject('/Registry'): TextStringObject('Adobe'),
            NameObject('/Ordering'): TextStringObject(ordering),
            NameObject('/Supplement'): NumberObject(5),
        }),
    })
    font = DictionaryObject({
        NameObject('/Type'): NameObject('/Font'),
        NameObject('/Subtype'): NameObject('/Type0'),
        NameObject('/BaseFont'): NameObject('/AdobeHeitiStd-Regular'),
        NameObject('/Encoding'): NameObject('/Identity-H'),
        NameObject('/DescendantFonts'): ArrayObject([writer._add_object(descendant)]),
    })
    ref = writer._add_object(font)
    # Adobe GB1 CIDs: 安踏體育用品有限公司。 No font/issuer guesses in production.
    body = [957, 3575, 8564, 4322, 4264, 3030, 4277, 3915, 1796, 3507, 98] * 10
    for index in range(pages):
        glyphs = [957] * prefix
        if index == identity_page:
            glyphs += [1826, 1658, 1405, 8002, 287, *codes]  # 股份代號：02020
        glyphs += body
        encoded = b''.join(cid.to_bytes(2, 'big') for cid in glyphs).hex().encode()
        stream = DecodedStreamObject()
        stream.set_data(b'BT /F1 12 Tf 14 TL 50 750 Td <' + encoded + b'> Tj ET')
        page = writer.add_blank_page(width=600, height=800)
        page[NameObject('/Resources')] = DictionaryObject({
            NameObject('/Font'): DictionaryObject({NameObject('/F1'): ref}),
        })
        page[NameObject('/Contents')] = writer._add_object(stream)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def serve(monkeypatch, data):
    from tests.test_official_announcement_excerpt import Response
    monkeypatch.setattr('requests.get', lambda *args, **kwargs: Response(data))


def test_real_adobe_gb1_pdf_decodes_before_unchanged_identity_check(monkeypatch):
    serve(monkeypatch, cid_pdf())
    result = announcement_excerpt('https://static.cninfo.com.cn/a.pdf', 'HK02020', '')
    assert '02020' in result['excerpt'] and '安踏體育用品有限公司' in result['excerpt']
    assert result['text_extractor'] == 'pdfminer.six_adobe_gb1'
    assert result['content_reviewed'] is False
    assert result['extraction'] == 'official_pdf_first_three_pages'


@pytest.mark.parametrize('code', ['HK02021', '002020', 'HK09988'])
def test_decoding_does_not_admit_another_issuer(monkeypatch, code):
    serve(monkeypatch, cid_pdf())
    with pytest.raises(ValueError, match='identity_mismatch'):
        announcement_excerpt('https://static.cninfo.com.cn/a.pdf', code, '')


@pytest.mark.parametrize('pages,identity_page,prefix', [(4, 3, 0), (1, 0, 10001)])
def test_cid_decoder_keeps_three_page_and_per_page_text_limits(monkeypatch, pages, identity_page, prefix):
    serve(monkeypatch, cid_pdf(pages=pages, identity_page=identity_page, prefix=prefix))
    with pytest.raises(ValueError, match='identity_mismatch'):
        announcement_excerpt('https://static.cninfo.com.cn/a.pdf', 'HK02020', '')


def test_cid_rejection_keeps_decoded_diagnostics_across_real_worker(monkeypatch):
    from src.services.research_execution import ResearchCallError, run_isolated
    serve(monkeypatch, cid_pdf())
    with pytest.raises(ResearchCallError) as caught:
        run_isolated(lambda: announcement_excerpt('https://static.cninfo.com.cn/a.pdf', 'HK02021', ''), 2)
    assert caught.value.error_class == 'announcement_document_identity_mismatch'
    evidence = caught.value.evidence_diagnostics
    assert evidence['text_extractor'] == 'pdfminer.six_adobe_gb1'
    assert '02020' in evidence['header_excerpt']
    assert len(evidence['document_sha256']) == 64


def test_unknown_collection_is_not_guessed_from_the_same_cid_bytes(monkeypatch):
    serve(monkeypatch, cid_pdf(ordering='Unknown'))
    with pytest.raises(ValueError, match='identity_mismatch'):
        announcement_excerpt('https://static.cninfo.com.cn/a.pdf', 'HK02020', '')
