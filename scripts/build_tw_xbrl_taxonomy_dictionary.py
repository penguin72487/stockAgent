#!/usr/bin/env python3
"""Read official taxonomy definitions; never equate concepts by name alone.

These public schema documents are not quarterly issuer observation archives.
Only ZIP links advertised by the official taxonomy page are requested. Exact
schema attributes AND standard labels must agree before version aliases merge.
"""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import sys
from urllib.parse import urljoin, urlparse
import zipfile

from defusedxml import ElementTree as ET
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json
from downloader.download_tw_mops_xbrl import _safe_members, validate_zip
from downloader.common import SharedRateLimiter

INDEX = "https://mopsov.twse.com.tw/mops/web/t203sb03"
XLINK = "{http://www.w3.org/1999/xlink}"
XSD = "{http://www.w3.org/2001/XMLSchema}"
XBRLI = "{http://www.xbrl.org/2003/instance}"
CONTRACT = "tw_xbrl_same_family_element_schema_standard_labels_v1"


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for data in iter(lambda: stream.read(1 << 20), b""): h.update(data)
    return h.hexdigest()


def family(uri):
    if uri.startswith("http://xbrl.iasb.org/taxonomy/") or "xbrl.ifrs.org/taxonomy/" in uri:
        return "ifrs-full"
    match = re.search(r"/(?:tifrs/)?(bsci/(?:ci|basi|bd|ins|fh|mim)|scf|es|ar|notes)/", uri)
    return "tifrs-" + match[1].replace("/", "-") if match else None


def parse_archive(path):
    elements, identifiers, documents, labels = {}, {}, {}, {}
    with zipfile.ZipFile(path) as archive:
        for member in _safe_members(archive):
            if member.filename.lower().endswith((".xsd", ".xml")):
                try: document = ET.fromstring(archive.read(member))
                except ET.ParseError: continue
                documents[member.filename] = document
                if document.tag != XSD + "schema": continue
                namespace = document.attrib.get("targetNamespace", "")
                if family(namespace) is None: continue
                for node in document.findall(XSD + "element"):
                    name, identifier = node.get("name"), node.get("id")
                    if not name or not identifier: continue
                    key = "{" + namespace + "}" + name
                    elements[key] = {"family":family(namespace), "name":name,
                        "type":node.get("type"), "periodType":node.get(XBRLI+"periodType"),
                        "balance":node.get(XBRLI+"balance"), "abstract":node.get("abstract", "false"),
                        "nillable":node.get("nillable", "false")}
                    identifiers[(member.filename,identifier)] = key
        for member, document in documents.items():
            for link in document.iter():
                if link.tag.rsplit("}",1)[-1] != "labelLink": continue
                locs, resources = {}, {}
                for node in link:
                    label = node.get(XLINK+"label")
                    if node.tag.rsplit("}",1)[-1] == "loc":
                        href = node.get(XLINK+"href", "")
                        file, _, fragment = href.partition("#")
                        # ZIP-relative POSIX lookup, not arbitrary filesystem I/O.
                        parts = list(PurePosixPath(member).parent.parts)
                        for part in PurePosixPath(file).parts:
                            if part == "..":
                                if parts: parts.pop()
                            elif part != ".": parts.append(part)
                        locs[label] = identifiers.get(("/".join(parts),fragment))
                    elif node.tag.rsplit("}",1)[-1] == "label":
                        role = node.get(XLINK+"role", "")
                        if role == "http://www.xbrl.org/2003/role/label":
                            resources[label] = (node.get("{http://www.w3.org/XML/1998/namespace}lang", ""),
                                                " ".join("".join(node.itertext()).split()))
                for node in link:
                    if node.tag.rsplit("}",1)[-1] != "labelArc": continue
                    key = locs.get(node.get(XLINK+"from"))
                    value = resources.get(node.get(XLINK+"to"))
                    if key and value: labels.setdefault(key,set()).add(value)
    result = {}
    for key, definition in elements.items():
        if not labels.get(key) or not definition["periodType"] or definition["abstract"] == "true": continue
        definition["standard_labels"] = sorted(labels[key])
        encoded = json.dumps(definition,sort_keys=True,ensure_ascii=False,separators=(",",":"))
        result[key] = {"semantic_key": "xbrl:"+hashlib.sha256(encoded.encode()).hexdigest()[:24],
                       "definition":definition}
    return result


def build(out):
    if out.exists(): raise FileExistsError("use a new taxonomy evidence root")
    out.mkdir(parents=True)
    limiter = SharedRateLimiter(1., name="tw_public")
    proofs, definitions, failures = {}, {}, []
    with requests.Session() as session:
        limiter.wait()
        page = session.get(INDEX,timeout=(10,30)); page.raise_for_status()
        index = out/"index.html"; index.write_bytes(page.content)
        proofs[index.name] = {"sha256":digest(index),"bytes":index.stat().st_size,"url":INDEX}
        links = sorted(set(urljoin(INDEX,p) for p in re.findall(r"[\"']([^\"']*tifrs-\d{8}\.zip)[\"']",page.text)))
        if not links or len(links)>16: raise ValueError("unexpected official taxonomy inventory")
        for url in links:
            parsed=urlparse(url)
            if parsed.scheme!="https" or parsed.hostname!="mopsov.twse.com.tw" or not parsed.path.startswith("/nas/taxonomy/"):
                raise ValueError("taxonomy escaped the official advertised path")
            path=out/Path(parsed.path).name
            try:
                limiter.wait()
                response=session.get(url,timeout=(10,45));response.raise_for_status()
                if len(response.content)>64*1024**2:raise ValueError("taxonomy exceeds evidence byte bound")
                path.write_bytes(response.content);validate_zip(path)
                parsed_definitions=parse_archive(path)
                for key, definition in parsed_definitions.items():
                    if key in definitions and definitions[key]["semantic_key"]!=definition["semantic_key"]:
                        # An updated document under the SAME QName is ambiguous.
                        definitions[key]["ambiguous"]=True
                    else: definitions[key]=definition
                proofs[path.name]={"sha256":digest(path),"bytes":path.stat().st_size,"url":url,
                                   "numeric_concepts_with_standard_labels":len(parsed_definitions)}
            except (requests.RequestException,ValueError,zipfile.BadZipFile) as error:
                failures.append({"url":url,"error":type(error).__name__+": "+str(error)})
                print(json.dumps(failures[-1]),flush=True)
    # Inline XBRL source parsers retain prefix names. Only map such a prefix
    # when every archived definition for that family/name agrees; otherwise
    # keep it separate instead of guessing which version the prefix denotes.
    prefixed={}
    for key, definition in list(definitions.items()):
        if definition.get("ambiguous"):continue
        d=definition["definition"];name=d["family"]+":"+d["name"]
        prefixed.setdefault(name,{}).setdefault(definition["semantic_key"],definition)
    aliases={k:v for k,v in definitions.items() if not v.get("ambiguous")}
    for key, values in prefixed.items():
        if len(values)==1:aliases[key]=next(iter(values.values()))
    atomic_write_json(out/"dictionary.json",{"contract":CONTRACT,"created_at_utc":datetime.now(UTC).isoformat(),
        "aliases":aliases,"files":proofs,"failures":failures,
        "merge_rule":"same_official_family_and_element_schema_attributes_and_standard_labels;unit_and_report_grain_still_separate"})
    return {"aliases":len(aliases),"semantic_keys":len({v['semantic_key'] for v in aliases.values()}),
            "files":len(proofs),"failures":len(failures),"dictionary_sha256":digest(out/"dictionary.json")}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--output-root",type=Path,required=True)
    a=p.parse_args();print(json.dumps(build(a.output_root)))


if __name__=="__main__":main()
