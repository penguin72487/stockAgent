import hashlib

import pytest

from stockagent.data.factorized_panel import verify_factorized_members


def _manifest(tmp_path):
    proofs=[]
    for i in range(12):
        path=tmp_path/f"{i}.bin"
        path.write_bytes(bytes([i])*8192)
        proofs.append({"path":path.name,"sha256":hashlib.sha256(path.read_bytes()).hexdigest()})
    return {"common":proofs[0],"observations":proofs[1:4],
            "annual_events":proofs[4:8],"blocks":[{"symbol_blocks":proofs[8:]}]}


@pytest.mark.parametrize("workers",[1,4])
@pytest.mark.parametrize("failure",[None,"raw_corrupt","raw_missing","block_corrupt","escape"])
def test_all_raw_and_model_members_are_still_verified(tmp_path,workers,failure):
    manifest=_manifest(tmp_path)
    if failure=="raw_corrupt":
        (tmp_path/"2.bin").write_bytes(b"corrupt")
    elif failure=="raw_missing":
        (tmp_path/"2.bin").unlink()
    elif failure=="block_corrupt":
        (tmp_path/"11.bin").write_bytes(b"corrupt")
    elif failure=="escape":
        outside=tmp_path.parent/(tmp_path.name+".outside.bin")
        manifest["observations"][0]["path"]="../"+outside.name
        outside.write_bytes(b"outside")
        manifest["observations"][0]["sha256"]=hashlib.sha256(b"outside").hexdigest()
    if failure is None:
        verify_factorized_members(tmp_path,manifest,workers=workers)
    elif failure=="raw_missing":
        with pytest.raises(FileNotFoundError):
            verify_factorized_members(tmp_path,manifest,workers=workers)
    else:
        with pytest.raises(ValueError,match="identity mismatch"):
            verify_factorized_members(tmp_path,manifest,workers=workers)


@pytest.mark.parametrize("workers",[0,-1,17])
def test_worker_budget_is_explicit_and_bounded(tmp_path,workers):
    with pytest.raises(ValueError,match="workers"):
        verify_factorized_members(tmp_path,_manifest(tmp_path),workers=workers)


def test_environment_is_operational_only(tmp_path,monkeypatch):
    monkeypatch.setenv("STOCKAGENT_FEATURE_VERIFY_THREADS","4")
    verify_factorized_members(tmp_path,_manifest(tmp_path))
