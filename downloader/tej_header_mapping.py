"""Versioned column identity, never a transformation of observed values."""

CONTRACT = 'injective_literal_comma_to_space_preview_headers_v1'


def validate_headers(request: dict, payload: dict, keys: list[str]) -> tuple[list[str], str | None]:
    canonical = [*keys, *request['fields']]
    native = payload['cells'][0]
    contract = payload.get('preview_header_mapping_contract')
    if contract not in (None, CONTRACT):
        raise ValueError('Unreviewed Preview header mapping contract')
    if len(set(canonical)) != len(canonical) or not isinstance(native, list) or len(native) != len(canonical):
        raise ValueError('Export schema mismatch')
    if native == canonical:
        return canonical, contract
    if (contract != CONTRACT or payload.get('capture_method') != 'native_msaa_preview_full'
            or payload.get('selected_field_order_readback_verified') is not True
            or payload.get('fields') != request['fields'] or native[:len(keys)] != keys
            or any(not isinstance(name, str) for name in native)):
        raise ValueError('Unverified native feature header correspondence')
    fields = request['fields']
    mapped = [name.replace(',', ' ') for name in fields]
    if (len(set(mapped)) != len(mapped) or len(set(native)) != len(native)
            or any(actual not in (expected, rendered) for actual, expected, rendered in
                   zip(native[len(keys):], fields, mapped, strict=True))):
        raise ValueError('Ambiguous or nonliteral Preview header mapping')
    return canonical, contract
