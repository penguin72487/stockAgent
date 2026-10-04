"""Mouse-free input contract and no-query operator acceptance boundaries.

Source checks cover interop paths on Linux. The owned Windows fixture and
private TEJ input receipts separately verify actual addressed-input behavior.
"""
from contextlib import closing
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import DESKTOP_INPUT_CONTRACT, connect, verify_desktop_input


REPO = Path(__file__).resolve().parents[1]
BRIDGE = (REPO / 'scripts/tej_smart_wizard_bridge.ps1').read_text()
CATALOG = (REPO / 'scripts/inventory_tej_smart_wizard_catalog.ps1').read_text()


@pytest.mark.parametrize('source', [BRIDGE, CATALOG])
@pytest.mark.parametrize('forbidden', ['SetCursorPos', 'mouse_event', 'SendInput', 'WM_LBUTTON', 'pyautogui'])
def test_no_mouse_injection_or_coordinate_fallback(source, forbidden):
    assert forbidden not in source


def test_tab_changes_use_normal_notifying_native_event():
    assert BRIDGE.count('Message(tab,0x1330,index,0)') == 1
    assert BRIDGE.count('[TejBridgeNative]::SelectTab(') == 2
    assert 'Message(tab,0x1330,0,0)' in CATALOG
    for source in (BRIDGE, CATALOG):
        assert 'Message(tab,0x1304,0,0)!=4' in source
        assert '(GetWindowLong(' in source and '&0x100)!=0' in source
        assert 'Message(tab,0x130C' not in source  # TCM_SETCURSEL omits notification.


def test_focus_is_real_owner_scoped_and_detached_even_on_failure():
    scope = BRIDGE.split('private static void DateScope', 1)[1].split('private static long VerifiedDateFocus', 1)[0]
    for proof in ('IsChild(r,g)', 'IsChild(g,e)', 'rp!=ep', 'rp!=gp', 'rt!=et', 'rt!=gt',
                  'IsWindowEnabled(e)', 'Dialogs(root).Length!=0'):
        assert proof in scope
    focus = BRIDGE.split('public static long FocusDate', 1)[1].split('public static string[] WriteDateText', 1)[0]
    assert focus.index('DateScope(') < focus.index('Activate(') < focus.index('AttachThreadInput(current,owner,true)')
    assert focus.index('IsActiveOwner(root)') < focus.index('SetFocus(new IntPtr(edit))')
    assert 'finally {if(attached)AttachThreadInput(current,owner,false);}' in focus
    assert 'VerifiedDateFocus(root,edit,group)' in focus
    assert 'WindowFromPoint' not in focus


def test_no_input_focus_refusal_is_availability_not_an_exhaustible_keyboard_bug():
    focus=BRIDGE.split('public static long FocusDate',1)[1].split('public static string[] WriteDateText',1)[0]
    assert focus.count('DATE_FOCUS_AVAILABILITY_BEFORE_INPUT:')==4
    assert 'SendWait(' not in focus
    outcomes=BRIDGE.split(".Contains('DATE_INPUT_BEFORE_QUERY:')",1)[1].split(".Contains('LOCAL_SELECTION_BEFORE_QUERY:')",1)[0]
    assert "Contains('DATE_FOCUS_AVAILABILITY_BEFORE_INPUT:')" in outcomes
    assert "'desktop_foreground_unavailable_before_preview'" in outcomes
    # An actual unacknowledged digit/caret remains a local-input defect, not
    # availability or permission to carry on typing/replay a provider query.
    digits=BRIDGE.split('public static string[] WriteDateText',1)[1].split('public static void DateText',1)[0]
    assert 'DATE_FOCUS_AVAILABILITY_BEFORE_INPUT:' not in digits


def test_date_model_uses_acknowledged_owned_messages_without_foreground_or_keyboard():
    write = BRIDGE.split('public static string[] WriteDateText', 1)[1].split('public static void DateText', 1)[0]
    assert 'return WriteDateMessages(root,edit,group,digits)' in write
    scoped=BRIDGE.split('public static string[] WriteDateMessages',1)[1].split('public static string[] PreviewHeader',1)[0]
    for proof in ('DateTime.TryParseExact', 'DateScope(root,edit,group)', 'acknowledged', 'previous=Text(edit)',
                  'Message(edit,0x303,0,0)', 'Message(edit,0x102,c,1)', 'result.Replace("/","")'):
        assert proof in scoped
    for forbidden in ('WriteText(', 'WM_SETFOCUS', 'WM_KILLFOCUS', 'FocusDate(', 'Activate(',
                      'SendWait(', 'AttachThreadInput(', 'SetFocus(', 'PostMessageW('):
        assert forbidden not in scoped
    assert 'SendKeys.SendWait(' not in BRIDGE
    production = BRIDGE.split('public static void DateText', 1)[1].split('public static string[] PreviewHeader', 1)[0]
    assert 'WriteDateText(' in production and 'return;' not in production


def test_blank_source_mask_can_be_filled_without_accepting_an_arbitrary_empty_edit():
    production=BRIDGE.split("$dates=Find-QueryGroup 'Date Setting'",1)[1]
    assert "(\\d{4}/\\d{2}/\\d{2}|____/__/__)" in production
    write=BRIDGE.split('public static string[] WriteDateMessages',1)[1].split('public static string[] PreviewHeader',1)[0]
    assert 'before!="________"' in write and 'cleared==before&&before!=""' in write
    assert 'DateScope(root,edit,group)' in write and 'Scoped date character not acknowledged' in write
    fixture=(REPO/'scripts/verify_tej_mouse_free_input.ps1').read_text()
    assert 'blank_mask_real_digits_and_model_commit' in fixture


def test_unknown_notice_is_retained_privately_without_acknowledgement_or_edit_value_reads():
    capture=BRIDGE.split('A transient notice may disappear',1)[1].split("throw 'Unrecognized vendor dialog",1)[0]
    assert "@('Static','Button')" in capture and 'notice_acknowledged=$false' in capture
    assert 'GetWindowThreadProcessId' in capture and "($Output+'.notice.json')" in capture
    for forbidden in ('Activate(', '0xF5', 'SendWait(', "'Edit'", 'ValuePattern'):
        assert forbidden not in capture


def test_ownerless_modal_requires_exact_thread_disabled_root_and_unique_source_process_windows():
    method=BRIDGE.split('public static bool IsUniqueOwnerlessModal',1)[1].split('public static void VerifyNormalEmptyDialog',1)[0]
    for proof in ('rp!=dp','rt!=dt','IsWindowEnabled(r)','GetWindow(d,4)!=IntPtr.Zero',
                  'name.ToString()!="#32770"','windows.Length==2','Array.IndexOf(windows,root)'):
        assert proof in method
    acknowledgement=BRIDGE.split('public static void VerifyNormalEmptyDialog',1)[1].split('public static long[] VisibleProcessWindows',1)[0]
    assert 'ERROR1:No data' in acknowledgement and 'matching!=1' in acknowledgement
    assert 'buttons.Length!=1' in acknowledgement and 'a.accDoDefaultAction(0)' not in acknowledgement
    assert 'Activate(' not in acknowledgement and '0xF5' not in acknowledgement
    production=BRIDGE.split('function Empty-Response',1)[1].split("if($requestDoc.action -in @('resolve_empty'",1)[0]
    assert 'IsUniqueOwnerlessModal' in production and 'AcknowledgeNormalEmptyDialog' in production
    ack=BRIDGE.split('public static void AcknowledgeNormalEmptyDialog',1)[1].split('public static long[] VisibleProcessWindows',1)[0]
    assert 'VerifyNormalEmptyDialog(root,dialog,button)' in ack and 'Message(dialog,0x111u,id,button)' in ack
    assert 'GetDlgCtrlID(new IntPtr(button))' in ack and '(id!=1&&id!=2)' in ack
    assert 'Activate(' not in ack and '0xF5' not in ack
    assert 'Activate(' not in production


def test_query_uses_exact_pinned_workbook_not_users_active_excel_view():
    binding=BRIDGE.split("$owns=$mutex.WaitOne(0)",1)[1].split("if($requestDoc.action -eq 'inspect_notices')",1)[0]
    for proof in ('$excel.Workbooks.Item($ExpectedWorkbook)', '$book.Name -cne $ExpectedWorkbook',
                  '$book.Windows.Count -ne 1', '$book.Windows.Item(1).Hwnd -ne $ExpectedExcelWindow',
                  'IsWindow([IntPtr]$ExpectedExcelWindow)', 'DESKTOP_CONTEXT_BEFORE_QUERY:'):
        assert proof in binding
    for forbidden in ('$excel.ActiveWorkbook', '$book.Activate()', '$excel.Hwnd -ne'):
        assert forbidden not in binding
    outcome=BRIDGE.split(".Contains('DESKTOP_CONTEXT_BEFORE_QUERY:')",1)[1].split(".Contains('DATE_INPUT_BEFORE_QUERY:')",1)[0]
    assert 'market_data_query_submission_possible=$false' in outcome
    assert "error_code='desktop_context_unavailable_before_preview'" in outcome


@pytest.mark.parametrize('code',['desktop_foreground_unavailable_before_preview','desktop_context_unavailable_before_preview'])
def test_desktop_availability_wait_does_not_consume_local_bug_retry_budget(tmp_path,code):
    from downloader.tej_history import _mark_prequery_failure
    root=tmp_path/'data_tej'
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state) VALUES(?,?,?,?,?,?,?)",
                    ('task','table','download','P1',1,'{}','running'))
        for _ in range(5):
            assert _mark_prequery_failure(con,{'task_id':'task','table_id':'table'},code)=='desktop_unavailable'
        row=con.execute('SELECT state,safe_prequery_retries,next_attempt_at_utc,actual_rows FROM tasks').fetchone()
        assert row['state']=='pending' and row['safe_prequery_retries']==0
        assert row['next_attempt_at_utc'] is not None and row['actual_rows'] is None


def test_preview_normal_owned_action_does_not_require_foreground():
    verify = BRIDGE.split('public static void VerifyPreviewAction', 1)[1].split('public static void BeginPreviewDefaultAction', 1)[0]
    assert 'IsChild(r,b)' in verify and 'rp!=bp' in verify and 'Dialogs(root).Length!=0' in verify
    assert 'IsActiveOwner' not in verify
    production = BRIDGE.split('$previewButton=Control', 1)[1].split('$querySubmissionPossible=$true', 1)[0]
    assert 'Activate(' not in production and '$stage.preview_foreground_required=$false' in production


def test_resolved_selection_controls_are_guarded_per_click_and_rechecked_per_batch():
    resolved = BRIDGE.split('function Click-ResolvedButton',1)[1].split('function Select-FieldList',1)[0]
    for proof in ('Assert-Scope $parent', 'Assert-Scope $h', 'IsChild', 'WindowsForms10.BUTTON.',
                  'Current.Name', 'Dialogs($ExpectedWindow)', 'IsWindowEnabled', 'IsWindowVisible'):
        assert proof in resolved
    assert resolved.count('Message $h 0xF5') == 1
    companies = BRIDGE.split('$companySelectButton=Company-SelectButton',1)[1].split('$dateSelectButton=',1)[0]
    assert 'Select-Company $companySelectButton' in companies
    assert '(Company-SelectButton) -ne $companySelectButton' in companies
    assert 'WaitSelectedList' in companies
    for group, variable in (('source','fieldSelectButton'),('dates','dateSelectButton')):
        assert f"${variable}=Control ${group} 'Select' '*BUTTON*'" in BRIDGE
        assert f"Click-ResolvedButton ${group} ${variable} 'Select'" in BRIDGE
        assert f"(Control ${group} 'Select' '*BUTTON*') -ne ${variable}" in BRIDGE


def test_native_scope_guard_is_fresh_per_operation_not_cached_owner_authorization():
    native = BRIDGE.split('public static void AssertControlScope',1)[1].split('[DllImport',1)[0]
    for proof in ('GetWindowThreadProcessId(r,out rp)', 'GetWindowThreadProcessId(c,out cp)',
                  '!IsWindow(r)', '!IsWindow(c)', 'rp!=expectedPid', 'cp!=expectedPid', '!IsChild(r,c)',
                  'WindowTitle(root)!=title'):
        assert proof in native
    powershell = BRIDGE.split('function Assert-Scope',1)[1].split("if($root.Current.Name",1)[0]
    assert 'AssertControlScope($ExpectedWindow,$h,$TejProcessId,$ExpectedTitle)' in powershell
    assert '$root.Current' not in powershell


def test_current_binding_readback_reads_selected_combo_not_the_entire_menu():
    verify=BRIDGE.split('function Verify-EditableScope',1)[1].split('function Verify-SourceSelectors',1)[0]
    assert 'SelectedComboText($ExpectedWindow,$h)' in verify
    assert 'Items $h $true' not in verify
    production=BRIDGE.split('$bindingAlreadyMatches=$true',1)[1].split('$sourceKeyMode=Read-SourceKeyMode',1)[0]
    assert 'SelectedComboText($ExpectedWindow,$h)' in production and 'Items $h $true' not in production
    selection=BRIDGE.split('function Select-Combo',1)[1].split('function Click-Button',1)[0]
    assert 'ExactComboIndex($ExpectedWindow,$h,$name)' in selection
    assert selection.count('NotifyBinding(')==1 and 'Items $h $true' not in selection


def test_native_combo_lookup_keeps_case_sensitive_unicode_and_duplicate_safety():
    lookup=BRIDGE.split('public static int ExactComboIndex',1)[1].split('public static string Text',1)[0]
    for proof in ('ComboScope(root,control,true)', '0x158u', 'ComboItem(control,index)==label',
                  'duplicate exact combo label', 'index==first', 'scanned<count'):
        assert proof in lookup
    assert '0x14Du' not in lookup and '0x14Eu' not in lookup  # read-only find, never selection.
    selected=BRIDGE.split('public static string SelectedComboText',1)[1].split('public static int ExactComboIndex',1)[0]
    assert 'ComboItem(control,index)' in selected and '0x147u' in selected
    assert 'Items(' not in selected
    assert 'ComboScope(root,control,false)' in selected


def test_all_axis_shortcut_still_requires_exact_source_names_before_preview():
    company=BRIDGE.split('$companyIndices=@{}',1)[1].split('$availableDates=@()',1)[0]
    dates=BRIDGE.split('$dateIndices=@{}',1)[1].split("if($requestDoc.action -in @('plan','recover_plan'",1)[0]
    assert '$requestDoc.company_labels.Count -ne $availableCompanyCount' in company
    assert '$requestDoc.date_labels.Count -ne $availableDateCount' in dates
    assert 'ExactListIndex' in company and 'ExactListIndex' in dates
    submission=BRIDGE.split("if($requestDoc.company_labels.Count -lt 1",1)[1].split('$querySubmissionPossible=$true',1)[0]
    for proof in ('SameItems([string[]]$selectedCompanies,[string[]]$requestDoc.company_labels)',
                  'SameItems([string[]]$selectedDates,[string[]]$requestDoc.date_labels)', 'Verify-EditableScope -BindingReadback'):
        assert proof in submission
    assert "scope_preparation_timings_contract='monotonic_complete_scope_stages_v1'" in submission


def test_date_foreground_and_keyboard_telemetry_match_the_actual_message_contract():
    for proof in ('$stage.date_input_requires_foreground=$false', '$stage.global_keyboard_input_sent=$false'):
        assert proof in BRIDGE
    scoped=BRIDGE.split('public static string[] WriteDateMessages',1)[1].split('public static string[] PreviewHeader',1)[0]
    assert scoped.count('Message(edit,0x102,c,1)')==1
    assert 'while(' not in scoped and 'SendKeys' not in scoped
    fixture=(REPO/'scripts/verify_tej_mouse_free_input.ps1').read_text()
    for proof in ('date_writes_never_activated_owner','focus_loss_does_not_interrupt_scoped_input',
                  'focus_loss_never_typed_into_foreground_window','midwrite_disabled_group_stops_before_digits'):
        assert proof in fixture


def test_notice_diagnostic_distinguishes_disabled_root_from_no_direct_dialog():
    diagnostic = BRIDGE.split("if($requestDoc.action -eq 'inspect_notices')", 1)[1].split(
        "if($requestDoc.action -eq 'ack_preview_column_limit')", 1)[0]
    for proof in ('root_enabled=', 'root_automation_enabled=', 'root_visible=',
                  'VisibleProcessWindows($ExpectedWindow)', 'process_windows=$processWindows'):
        assert proof in diagnostic
    for forbidden in ('Activate(', '0xF5', 'Select-Combo', 'Click-Button', 'Set-Checkbox'):
        assert forbidden not in diagnostic
    enumerate_windows = BRIDGE.split('public static long[] VisibleProcessWindows', 1)[1].split(
        'public static string[] Items', 1)[0]
    assert 'return ProcessWindows(root,true)' in enumerate_windows
    assert 'pid==sourcePid&&(!visibleOnly||IsWindowVisible(h))' in enumerate_windows
    assert 'handles.Count>32' in enumerate_windows
    assert 'GetWindowThreadProcessId(new IntPtr(root),out sourcePid)' in enumerate_windows


def test_explicit_probe_has_no_query_or_source_selection_calls():
    probe = BRIDGE.split("if($requestDoc.action -eq 'probe_date_input')", 1)[1].split('[IO.File]::WriteAllText($Output', 1)[0]
    for forbidden in ('Click-Button', 'Select-Combo', 'PostMessageW', '0xF5', 'Set-Checkbox'):
        assert forbidden not in probe
    for proof in ('original dates/schema', 'WriteDateText', 'query_axes_unchanged', 'field_selection_unchanged',
                  'intermediate_edit_observed', 'early_noop_path_used=$false'):
        assert proof in probe


def test_input_contract_matches_python_and_actual_windows_bridge():
    assert f'InputContract="{DESKTOP_INPUT_CONTRACT}"' in BRIDGE
    assert '$stage.desktop_input_contract=[TejBridgeNative]::InputContract' in BRIDGE
    cli = (REPO / 'downloader/download_tej_history.py').read_text()
    branch = cli.split("elif a.action == 'verify-input':", 1)[1].split('elif ', 1)[0]
    assert 'verify_desktop_input(' in branch
    assert 'run_one(' not in branch and 'recover' not in branch.split('p.error(', 1)[0]


@pytest.fixture
def probe_registry(tmp_path):
    root = tmp_path / 'data_tej'
    request = {'action':'download', 'type':'LISTED & DELISTED', 'smart_id':'TEJEquity', 'table':'Fixture',
               'fields':['Volume(1000S)'], 'start':'2020-03-02', 'end':'2026-10-01'}
    with closing(connect(root)) as con, con:
        con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,last_error_code) VALUES(?,?,?,?,?,?,?,?)',
                    ('fixture', 'table', 'download', 'P1', 100, json.dumps(request), 'blocked', 'unknown_outcome_no_auto_retry'))
    return root, request


class ProbeBridge:
    def __init__(self, request, mutation=None):
        self.request, self.mutation, self.calls = request, mutation, []

    def execute(self, root, task):
        call = json.loads(task['request_json'])
        self.calls.append(call['action'])
        assert call['action'] == 'probe_date_input'
        assert all(call[k] == self.request[k] for k in ('start', 'end', 'fields'))
        payload = {'contract_version':4, 'provider':'tej_smart_wizard', 'action':'probe_date_input', 'task_id':'fixture',
                   'desktop_input_contract':DESKTOP_INPUT_CONTRACT,
                   **{k:call[k] for k in ('type', 'smart_id', 'table')},
                   'market_data_query_submitted':False, 'early_noop_path_used':False,
                   'global_keyboard_input_sent':False, 'date_foreground_required':False,
                   **{k:True for k in ('date_text_input_sent', 'binding_matches_failed_plan', 'query_axes_unchanged',
                       'source_binding_unchanged', 'field_selection_unchanged')}, 'date_input_results':[]}
        for key in ('start', 'end'):
            date = call[key].replace('-', '/')
            payload['date_input_results'].append({'input_trace':[date, '____/__/__', date, date],
                'target_text_before':date, 'target_text_after':date, 'same_value_write':True,
                'verified_exact_box':True, 'intermediate_edit_observed':True})
        if self.mutation == 'no_intermediate_edit':
            payload['date_input_results'][0]['input_trace'][1] = payload['date_input_results'][0]['input_trace'][0]
        elif self.mutation == 'wrong_final_date':
            payload['date_input_results'][1]['target_text_after'] = '2026/10/02'
        elif self.mutation == 'missing_date':
            payload['date_input_results'].pop()
        elif self.mutation == 'queue_changed':
            with closing(connect(root)) as con, con:
                con.execute("UPDATE tasks SET state='complete'")
        elif self.mutation:
            key = self.mutation
            payload[key] = 'wrong' if isinstance(payload[key], str) else None
        output = root / 'raw' / 'fixture-input.json'
        atomic_write_json(output, payload)
        return payload, output, 1.25


def test_verified_input_never_resets_unknown_or_adopts_data(probe_registry):
    root, request = probe_registry
    with closing(connect(root)) as con:
        before = dict(con.execute('SELECT * FROM tasks').fetchone())
    bridge = ProbeBridge(request)
    result = verify_desktop_input(root, 'fixture', bridge)
    assert result['both_date_writes_verified'] and result['intermediate_edits_observed']
    assert result['queue_unchanged'] and not result['unknown_outcome_barrier_reset']
    assert not result['data_query_repeated'] and not result['source_rows_adopted']
    assert bridge.calls == ['probe_date_input']
    with closing(connect(root)) as con:
        assert dict(con.execute('SELECT * FROM tasks').fetchone()) == before
    audit = json.loads(next((root / 'input_acceptance').glob('*.json')).read_text())
    assert audit['source_evidence_sha256'] and audit['state_before'] == audit['state_after'] == 'blocked'
    assert audit['contract_version']==2 and audit['global_keyboard_input_sent'] is False
    assert audit['date_foreground_required'] is False


@pytest.mark.parametrize('mutation', ['task_id', 'action', 'table', 'provider', 'desktop_input_contract',
    'market_data_query_submitted', 'global_keyboard_input_sent', 'date_foreground_required',
    'early_noop_path_used', 'query_axes_unchanged', 'field_selection_unchanged',
    'no_intermediate_edit', 'wrong_final_date', 'missing_date', 'queue_changed'])
def test_incomplete_input_proof_is_rejected_without_recovery(probe_registry, mutation):
    root, request = probe_registry
    bridge = ProbeBridge(request, mutation)
    with pytest.raises(ValueError):
        verify_desktop_input(root, 'fixture', bridge)
    assert bridge.calls == ['probe_date_input']
    assert not (root / 'input_acceptance').exists()


@pytest.mark.parametrize('kind', ['discover', 'missing'])
def test_probe_requires_exact_existing_download_before_any_action(probe_registry, kind):
    root, request = probe_registry
    if kind == 'discover':
        with closing(connect(root)) as con, con:
            con.execute("UPDATE tasks SET kind='discover'")
    bridge = ProbeBridge(request)
    with pytest.raises(ValueError, match='exact registered download'):
        verify_desktop_input(root, 'missing' if kind == 'missing' else 'fixture', bridge)
    assert bridge.calls == []
