import json

from scripts import stage_futures_aux_e_working_copy as auxiliary


def test_auxiliary_staging_continues_after_one_failure_and_reports_partial(tmp_path, monkeypatch):
    monkeypatch.setattr(auxiliary.stage, 'ROOT', tmp_path)
    monkeypatch.setattr(auxiliary, 'SOURCES', (('data_one', 'marker'), ('data_two', 'marker')))
    proof = tmp_path/'proof.json'; proof.write_text('{"verified":true}')
    calls = []
    def run(*, source, report, **kwargs):
        calls.append(source.name)
        if source.name == 'data_one':
            raise RuntimeError('original owner is busy')
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps({'state':'verified_temporary_e_source_copy', 'source':str(source),
            'source_files_deleted':0, 'relocation_evidence':str(proof),
            'relocation_evidence_sha256':auxiliary.stage.relocation.digest(proof)}))
    monkeypatch.setattr(auxiliary.stage, 'main', run)
    assert auxiliary.main() == 1
    assert calls == ['data_one', 'data_two']
    output = tmp_path/'artifacts/markets/tw_futures_full_history_v22_official10_20261030_v1/futures-aux-sources-e-20261103-v1.json'
    result = json.loads(output.read_text())
    assert result['state'] == 'partial'
    assert [row['state'] for row in result['sources']] == ['deferred_or_failed', 'verified_e_copy']
    assert result['source_files_deleted'] == 0 and not result['financial_or_training_admission']
