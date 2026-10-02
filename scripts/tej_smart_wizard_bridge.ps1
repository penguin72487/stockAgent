<#
One scoped, serial desktop query. Does not log in, read credentials/comments,
save settings, change Office/TEJ policies, write worksheets, or close a workbook.
Plan reads the vendor's current company/date axes. Download reads a bounded
complete native MSAA Preview grid, preserving exact VENDOR DISPLAY STRINGS.
This is not an Excel Value2 export or proof of underlying numeric precision.
All dates describe the Smart Wizard query grid, NOT proven native observations
or publication times. Run only against an explicitly selected scratch query.
#>
param(
    [Parameter(Mandatory=$true)][string]$Request,
    [Parameter(Mandatory=$true)][string]$Output,
    [Parameter(Mandatory=$true)][int]$TejProcessId,
    [Parameter(Mandatory=$true)][long]$ExpectedWindow,
    [Parameter(Mandatory=$true)][string]$ExpectedTitle,
    [Parameter(Mandatory=$true)][string]$ExpectedWorkbook,
    [Parameter(Mandatory=$true)][long]$ExpectedExcelWindow
)
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName Accessibility
Add-Type -TypeDefinition @'
using System;
using System.Text;
using System.Runtime.InteropServices;
public static class TejBridgeNative {
    public const string InputContract="native_acknowledged_date_model_commit_blank_mask_no_mouse_v5";
    public const string SubmissionContract="owned_msaa_default_action_once_no_foreground_result_transition_v2";
    private static int previewStarted,previewActionState;
    public delegate bool ChildCallback(IntPtr h,IntPtr state);
    [StructLayout(LayoutKind.Sequential)] public struct Rect {public int Left,Top,Right,Bottom;}
    [StructLayout(LayoutKind.Sequential)] public struct Point {public int X,Y;public Point(int x,int y){X=x;Y=y;}}
    [StructLayout(LayoutKind.Sequential)] public struct Gui {public int Size;public uint Flags;public IntPtr Active,Focus,Capture,Menu,Move,Caret;public int L,T,R,B;}
    [StructLayout(LayoutKind.Sequential)] public struct NativeMsg {public IntPtr Hwnd;public uint Id;public UIntPtr WParam;public IntPtr LParam;public uint Time;public Point Position;public uint Private;}
    [DllImport("user32.dll")] public static extern bool IsChild(IntPtr p,IntPtr c);
    [DllImport("user32.dll")] public static extern bool IsWindow(IntPtr h);
    [DllImport("user32.dll")] private static extern bool EnumChildWindows(IntPtr h,ChildCallback callback,IntPtr state);
    [DllImport("user32.dll")] private static extern bool EnumWindows(ChildCallback callback,IntPtr state);
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] private static extern int GetClassNameW(IntPtr h,StringBuilder value,int limit);
    [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr h);
    [DllImport("user32.dll")] public static extern bool IsWindowEnabled(IntPtr h);
    [DllImport("user32.dll")] public static extern bool IsHungAppWindow(IntPtr h);
    [DllImport("user32.dll")] public static extern bool IsIconic(IntPtr h);
    [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr h,int c);
    [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr h,out uint p);
    [DllImport("user32.dll")] public static extern IntPtr GetParent(IntPtr h);
    [DllImport("user32.dll")] public static extern IntPtr GetWindow(IntPtr h,uint command);
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] public static extern bool PostMessageW(IntPtr h,uint m,IntPtr w,IntPtr l);
    [DllImport("user32.dll")] public static extern int GetDlgCtrlID(IntPtr h);
    [DllImport("user32.dll")] public static extern int GetWindowLong(IntPtr h,int index);
    [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr h,out Rect r);
    [DllImport("user32.dll")] public static extern IntPtr SetThreadDpiAwarenessContext(IntPtr c);
    [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
    [DllImport("user32.dll")] private static extern bool SetForegroundWindow(IntPtr h);
    [DllImport("user32.dll")] private static extern IntPtr SetFocus(IntPtr h);
    [DllImport("user32.dll")] private static extern bool AttachThreadInput(uint a,uint b,bool attach);
    [DllImport("user32.dll")] private static extern bool PeekMessageW(out NativeMsg message,IntPtr window,uint minimum,uint maximum,uint remove);
    [DllImport("kernel32.dll")] private static extern uint GetCurrentThreadId();
    [DllImport("user32.dll")] private static extern IntPtr WindowFromPoint(Point p);
    [DllImport("user32.dll")] private static extern IntPtr GetAncestor(IntPtr h,uint f);
    [DllImport("user32.dll")] private static extern bool GetGUIThreadInfo(uint thread,ref Gui info);
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] private static extern IntPtr SendMessageTimeoutW(IntPtr h,uint m,IntPtr w,IntPtr l,uint f,uint t,out IntPtr r);
    [DllImport("user32.dll",CharSet=CharSet.Unicode,EntryPoint="SendMessageTimeoutW")] private static extern IntPtr ReadText(IntPtr h,uint m,IntPtr w,StringBuilder l,uint f,uint t,out IntPtr r);
    [DllImport("user32.dll",CharSet=CharSet.Unicode,EntryPoint="SendMessageTimeoutW")] private static extern IntPtr WriteText(IntPtr h,uint m,IntPtr w,string l,uint f,uint t,out IntPtr r);
    [DllImport("oleacc.dll")] private static extern int AccessibleObjectFromWindow(IntPtr h,uint id,ref Guid iid,[MarshalAs(UnmanagedType.Interface)]out Accessibility.IAccessible a);
    public static long Message(long h,uint m,long w,long l) {
        IntPtr r;if(SendMessageTimeoutW(new IntPtr(h),m,new IntPtr(w),new IntPtr(l),2,30000,out r)==IntPtr.Zero)
            throw new TimeoutException("Vendor UI timed out; outcome unknown; no action retry");
        return r.ToInt64();
    }
    public static void NotifyBinding(long parent,long control,long notify) {
        // Wait for the ONE normal selection event to finish. Reading an
        // already-set combo index after PostMessage did not prove its source
        // company/date menus were refreshed; sent read messages can overtake
        // a posted event. Never repeat a timed-out metadata notification.
        IntPtr r;if(SendMessageTimeoutW(new IntPtr(parent),0x111,new IntPtr(notify),new IntPtr(control),2,150000,out r)==IntPtr.Zero)
            throw new TimeoutException("Binding event unresolved; no notification retry");
    }
    public static long[] Children(long root,string classPrefix) {
        var handles=new System.Collections.Generic.List<long>();
        bool exceeded=false;
        EnumChildWindows(new IntPtr(root),(h,state)=>{
            var name=new StringBuilder(256);GetClassNameW(h,name,256);
            if(name.ToString().StartsWith(classPrefix,StringComparison.Ordinal))handles.Add(h.ToInt64());
            if(handles.Count>512){exceeded=true;return false;}
            return true;
        },IntPtr.Zero);
        if(exceeded)throw new Exception("Native query control bound exceeded");
        return handles.ToArray();
    }
    public static long[] Dialogs(long root) {
        var handles=new System.Collections.Generic.HashSet<long>();
        foreach(long child in Children(root,"#32770"))
            if(IsWindowVisible(new IntPtr(child)))handles.Add(child);
        IntPtr source=new IntPtr(root);uint sourcePid;
        GetWindowThreadProcessId(source,out sourcePid);
        EnumWindows((h,state)=>{
            uint pid;GetWindowThreadProcessId(h,out pid);
            var name=new StringBuilder(256);GetClassNameW(h,name,256);
            if(pid==sourcePid&&IsWindowVisible(h)&&name.ToString()=="#32770"&&GetWindow(h,4)==source)
                handles.Add(h.ToInt64());
            return true;
        },IntPtr.Zero);
        return new System.Collections.Generic.List<long>(handles).ToArray();
    }
    public static long[] VisibleProcessWindows(long root) {
        // Diagnostics only: a WinForms modal can disable the query without
        // being a directly-owned #32770. Never infer acknowledgement rights
        // from this broader enumeration or use it to force-enable controls.
        var handles=new System.Collections.Generic.List<long>();
        uint sourcePid;GetWindowThreadProcessId(new IntPtr(root),out sourcePid);
        if(sourcePid==0)throw new Exception("Missing diagnostic query owner");
        bool exceeded=false;
        EnumWindows((h,state)=>{
            uint pid;GetWindowThreadProcessId(h,out pid);
            if(pid==sourcePid&&IsWindowVisible(h))handles.Add(h.ToInt64());
            if(handles.Count>32){exceeded=true;return false;}
            return true;
        },IntPtr.Zero);
        if(exceeded)throw new Exception("Visible process window diagnostic bound exceeded");
        return handles.ToArray();
    }
    public static bool WindowResponds(long root) {
        // WM_NULL is a bounded responsiveness diagnostic, not input/query.
        IntPtr result;
        return SendMessageTimeoutW(new IntPtr(root),0,IntPtr.Zero,IntPtr.Zero,3,1000,out result)!=IntPtr.Zero;
    }
    public static string[] Items(long h,bool combo) {
        long n=Message(h,combo?0x146u:0x18Bu,0,0);
        if(n<0||n>150000)throw new Exception("Catalog axis exceeds reviewed bound");
        string[] items=new string[n];
        for(int i=0;i<n;i++) {
            long len=Message(h,combo?0x149u:0x18Au,i,0);
            if(len<0||len>4096)throw new Exception("Unexpected catalog text bound");
            StringBuilder b=new StringBuilder((int)len+1);IntPtr result;
            if(ReadText(new IntPtr(h),combo?0x148u:0x189u,new IntPtr(i),b,2,10000,out result)==IntPtr.Zero)
                throw new TimeoutException("Vendor catalog read timed out");
            items[i]=b.ToString();
        } return items;
    }
    public static string Text(long h) {
        long n=Message(h,0xEu,0,0);
        if(n<0||n>256)throw new Exception("Unexpected date text bound");
        StringBuilder b=new StringBuilder((int)n+1);IntPtr result;
        if(ReadText(new IntPtr(h),0xDu,new IntPtr(n+1),b,2,10000,out result)==IntPtr.Zero)
            throw new TimeoutException("Date readback timed out");
        return b.ToString();
    }
    public static bool SameItems(string[] a,string[] b) {
        var seen=new System.Collections.Generic.HashSet<string>(a,StringComparer.Ordinal);
        return a.Length==b.Length&&seen.Count==a.Length&&seen.SetEquals(b);
    }
    public static int[] AccessibleState(long h) {
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(h),0xFFFFFFFCu,ref iid,out a));
        return new int[]{Convert.ToInt32(a.get_accRole(0)),Convert.ToInt32(a.get_accState(0))};
    }
    public static string AccessibleDefaultAction(long h) {
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(h),0xFFFFFFFCu,ref iid,out a));
        try {return a.get_accDefaultAction(0)??"";}
        finally {if(Marshal.IsComObject(a))Marshal.ReleaseComObject(a);}
    }
    public static void VerifyPreviewAction(long root,long button) {
        IntPtr r=new IntPtr(root),b=new IntPtr(button);uint rp,bp;
        uint rt=GetWindowThreadProcessId(r,out rp),bt=GetWindowThreadProcessId(b,out bp);
        if(!IsWindow(r)||!IsChild(r,b)||rp!=bp||rt!=bt||
           !IsWindowEnabled(r)||!IsWindowVisible(r)||!IsWindowEnabled(b)||!IsWindowVisible(b)||Dialogs(root).Length!=0)
            throw new Exception("Owned Preview default action unavailable; no action sent");
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(b,0xFFFFFFFCu,ref iid,out a));
        try {
            if(Convert.ToInt32(a.get_accRole(0))!=43||((Convert.ToInt32(a.get_accState(0)))&0x18001)!=0||
               (a.get_accName(0)??"").Trim()!="Preview"||a.get_accDefaultAction(0)!="Press")
                throw new Exception("Unreviewed Preview role/name/default action; no action sent");
        }finally {if(Marshal.IsComObject(a))Marshal.ReleaseComObject(a);}
    }
    public static void BeginPreviewDefaultAction(long root,long button) {
        VerifyPreviewAction(root,button);
        if(System.Threading.Interlocked.CompareExchange(ref previewStarted,1,0)!=0)
            throw new Exception("Preview action already started; never submit again");
        // Some MSAA servers block until their modal reply is dismissed. Keep
        // polling/normal exact No-data acknowledgement on the caller thread;
        // ONE owned STA performs the action, with no BM_CLICK fallback.
        var thread=new System.Threading.Thread(()=> {
            Accessibility.IAccessible a=null;
            try {
                VerifyPreviewAction(root,button);
                Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");
                Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(button),0xFFFFFFFCu,ref iid,out a));
                a.accDoDefaultAction(0);
                System.Threading.Interlocked.Exchange(ref previewActionState,1);
            }catch {System.Threading.Interlocked.Exchange(ref previewActionState,-1);}
            finally {if(a!=null&&Marshal.IsComObject(a))Marshal.ReleaseComObject(a);}
        });
        thread.IsBackground=true;thread.SetApartmentState(System.Threading.ApartmentState.STA);thread.Start();
    }
    public static int PreviewActionState() {return System.Threading.Interlocked.CompareExchange(ref previewActionState,0,0);}
    public static void Activate(long root) {
        SetThreadDpiAwarenessContext(new IntPtr(-4));IntPtr h=new IntPtr(root);uint pid;
        if(IsIconic(h))ShowWindow(h,9);
        uint current=GetCurrentThreadId(),fg=GetWindowThreadProcessId(GetForegroundWindow(),out pid),owner=GetWindowThreadProcessId(h,out pid);
        // A non-interactive PowerShell caller may have no input queue yet.
        // Inspect (PM_NOREMOVE) to initialize it before normal thread linking.
        // Never fabricate focus or type Alt to evade a refused foreground.
        NativeMsg message;PeekMessageW(out message,IntPtr.Zero,0,0,0);
        bool attached=fg!=0&&fg!=current&&AttachThreadInput(current,fg,true);
        bool attachedOwner=owner!=0&&owner!=current&&owner!=fg&&AttachThreadInput(current,owner,true);
        try{SetForegroundWindow(h);}finally{
            if(attachedOwner)AttachThreadInput(current,owner,false);
            if(attached)AttachThreadInput(current,fg,false);
        }
        for(int i=0;i<40&&GetForegroundWindow()!=h;i++)System.Threading.Thread.Sleep(50);
        if(GetForegroundWindow()!=h)throw new Exception("Foreground unavailable; no input sent");
    }
    public static bool IsActiveOwner(long root) {
        IntPtr h=new IntPtr(root);uint pid;uint thread=GetWindowThreadProcessId(h,out pid);
        Gui info=new Gui();info.Size=Marshal.SizeOf(info);
        return GetForegroundWindow()==h&&GetGUIThreadInfo(thread,ref info)&&info.Active==h;
    }
    public static int[] AccessibleRect(long control,int child) {
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(control),0xFFFFFFFCu,ref iid,out a));
        int l,t,w,h;a.accLocation(out l,out t,out w,out h,child);return new int[]{l,t,w,h};
    }
    public static object[] Geometry(long control) {
        Rect r;GetWindowRect(new IntPtr(control),out r);int[] a=null;
        try{a=AccessibleRect(control,0);}catch{}
        IntPtr nativeHit=WindowFromPoint(new Point((r.Left+r.Right)/2,(r.Top+r.Bottom)/2));
        IntPtr accHit=a!=null?WindowFromPoint(new Point(a[0]+a[2]/2,a[1]+a[3]/2)):IntPtr.Zero;
        return new object[]{new int[]{r.Left,r.Top,r.Right,r.Bottom},a,
            new long[]{nativeHit.ToInt64(),GetAncestor(nativeHit,2).ToInt64(),accHit.ToInt64(),GetAncestor(accHit,2).ToInt64()}};
    }
    public static void SelectTab(long root,long tab,int index) {
        IntPtr h=new IntPtr(tab);
        if(!IsChild(new IntPtr(root),h)||!IsWindowVisible(h)||!IsWindowEnabled(h)||
           index<0||index>=4||Message(tab,0x1304,0,0)!=4||(GetWindowLong(h,-16)&0x100)!=0)
            throw new Exception("Unreviewed tab scope/count/style; no navigation");
        // TCM_SETCURFOCUS (no TCS_BUTTONS) emits the normal selection event.
        // TCM_SETCURSEL alone would omit it. Never use a coordinate fallback.
        if(Message(tab,0x130B,0,0)==index)return;
        Message(tab,0x1330,index,0);
        for(int i=0;i<40;i++) {
            if(Message(tab,0x130B,0,0)==index)return;
            System.Threading.Thread.Sleep(50);
        }
        throw new Exception("Tab selection did not resolve; action not repeated");
    }
    public static void SelectListItem(long root,long control,int index,string expectedName) {
        // Select an exact, named native item, independent of foreground/DPI.
        // LB_SETSEL does not emit LBN_SELCHANGE; without the one normal
        // notification WinForms SelectedItems can retain the previous item.
        // Certify both native selection and the destination after BM_CLICK.
        IntPtr h=new IntPtr(control);
        if(!IsChild(new IntPtr(root),h)||!IsWindowVisible(h)||!IsWindowEnabled(h))
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: source list unavailable");
        long count=Message(control,0x18B,0,0);
        if(index<0||index>=count)throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: item index out of scope");
        long length=Message(control,0x18A,index,0);IntPtr nativeResult;
        if(length<0||length>4096)throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: item text bound differs");
        StringBuilder actualName=new StringBuilder((int)length+1);
        if(ReadText(h,0x189,new IntPtr(index),actualName,2,10000,out nativeResult)==IntPtr.Zero||actualName.ToString()!=expectedName)
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: native item name differs");
        int style=GetWindowLong(h,-16);bool multi=(style&0x808)!=0;
        IntPtr parent=GetParent(h);
        if((style&1)==0||!IsChild(new IntPtr(root),parent))
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: unreviewed list notification scope");
        if(multi) {
            if(Message(control,0x185,0,-1)<0||Message(control,0x185,1,index)<0||Message(control,0x19E,index,0)<0)
                throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: native selection refused");
        } else if(Message(control,0x186,index,0)!=index)
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: native selection refused");
        long notify=(GetDlgCtrlID(h)&65535)|65536; // LBN_SELCHANGE, once
        NotifyBinding(parent.ToInt64(),control,notify);
        for(int i=0;i<100;i++) {
            bool selected=multi ? Message(control,0x190,0,0)==1&&Message(control,0x187,index,0)>0
                                : Message(control,0x188,0,0)==index;
            if(selected)return;
            System.Threading.Thread.Sleep(20);
        }
        throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: native list selection did not resolve; expected="+index+
            " current="+Message(control,0x188,0,0)+" count="+(multi?Message(control,0x190,0,0):1)+"; no Select action sent");
    }
    public static int ExactListIndex(long root,long control,string label) {
        // Ask the real source list for one exact name, then independently read
        // it back. Downloads need <=32 companies, not 56,000 cross-process
        // text reads on every query. Discovery still inventories the full list.
        IntPtr r=new IntPtr(root),h=new IntPtr(control);uint rp,cp;
        uint rt=GetWindowThreadProcessId(r,out rp),ct=GetWindowThreadProcessId(h,out cp);
        if(!IsWindow(r)||!IsChild(r,h)||rp==0||rp!=cp||rt!=ct||
           !IsWindowEnabled(h)||!IsWindowVisible(h)||String.IsNullOrEmpty(label)||label.Length>4096||Dialogs(root).Length!=0)
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: exact list lookup scope unavailable");
        var cls=new StringBuilder(256);GetClassNameW(h,cls,256);
        if(!cls.ToString().StartsWith("WindowsForms10.LISTBOX.",StringComparison.Ordinal))
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: unreviewed lookup control class");
        long count=Message(control,0x18B,0,0);IntPtr result;
        if(count<1||count>150000||WriteText(h,0x1A2u,new IntPtr(-1),label,2,10000,out result)==IntPtr.Zero)
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: exact list lookup unresolved");
        long index=result.ToInt64();
        if(index<0||index>=count)throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: requested source label absent");
        long length=Message(control,0x18A,index,0);
        if(length<0||length>4096)throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: exact source label bound differs");
        var actual=new StringBuilder((int)length+1);
        if(ReadText(h,0x189,new IntPtr(index),actual,2,10000,out result)==IntPtr.Zero||actual.ToString()!=label)
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: exact source label readback differs");
        if(WriteText(h,0x1A2u,new IntPtr(index),label,2,10000,out result)==IntPtr.Zero||result.ToInt64()!=index)
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: duplicate or unresolved exact source label");
        return (int)index;
    }
    public static void WaitSelectedList(long root,long control,int expectedCount,string appendedName) {
        // A normal Select button may enqueue the WinForms update. Sent read
        // messages can overtake it: do not select another source item until
        // this one action has appeared in the actual destination list.
        IntPtr h=new IntPtr(control);
        if(!IsChild(new IntPtr(root),h)||!IsWindowVisible(h)||!IsWindowEnabled(h))
            throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: destination list unavailable");
        long count=-1;
        for(int i=0;i<100;i++) {
            count=Message(control,0x18B,0,0);
            if(count==expectedCount) {
                // PowerShell binds $null to an empty C# string. Count-only
                // Clear All/Select All must not read item -1 from an empty list.
                if(expectedCount==0||String.IsNullOrEmpty(appendedName))return;
                long length=Message(control,0x18A,expectedCount-1,0);IntPtr result;
                if(length>4096)throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: destination text bound differs");
                if(length<0){System.Threading.Thread.Sleep(20);continue;}
                StringBuilder text=new StringBuilder((int)length+1);
                if(ReadText(h,0x189,new IntPtr(expectedCount-1),text,2,10000,out result)!=IntPtr.Zero&&text.ToString()==appendedName)return;
            }
            if(count>expectedCount)break;
            System.Threading.Thread.Sleep(20);
        }
        throw new Exception("LOCAL_SELECTION_BEFORE_QUERY: destination did not acknowledge one Select; expected="+
            expectedCount+" actual="+count+"; no repeated action or query");
    }
    private static void DateScope(long root,long edit,long group) {
        IntPtr r=new IntPtr(root),e=new IntPtr(edit),g=new IntPtr(group);uint rp,ep,gp;
        uint rt=GetWindowThreadProcessId(r,out rp),et=GetWindowThreadProcessId(e,out ep),gt=GetWindowThreadProcessId(g,out gp);
        if(!IsWindow(r)||!IsChild(r,g)||!IsChild(g,e)||rp==0||rp!=ep||rp!=gp||rt!=et||rt!=gt||
           !IsWindowVisible(r)||!IsWindowVisible(g)||!IsWindowVisible(e)||
           !IsWindowEnabled(r)||!IsWindowEnabled(g)||!IsWindowEnabled(e)||(GetWindowLong(e,-16)&0x800)!=0||Dialogs(root).Length!=0)
            throw new Exception("Date input scope unavailable; no input or cached-date adoption");
    }
    private static long VerifiedDateFocus(long root,long edit,long group) {
        DateScope(root,edit,group);
        IntPtr r=new IntPtr(root);uint pid,thread=GetWindowThreadProcessId(r,out pid);
        Gui info=new Gui();info.Size=Marshal.SizeOf(info);Rect target,focused;
        if(GetForegroundWindow()!=r||!GetGUIThreadInfo(thread,ref info)||info.Active!=r||
           !IsChild(new IntPtr(group),info.Focus)||!IsWindowVisible(info.Focus)||!IsWindowEnabled(info.Focus))return 0;
        var name=new StringBuilder(256);GetClassNameW(info.Focus,name,256);
        // The legacy yyyyMMdd mask is a sibling overlay, not a child of the
        // stable date box. Bounds validate its identity; they never locate a
        // click. Refuse every other edit, including the second date box.
        if(!name.ToString().StartsWith("WindowsForms10.EDIT.",StringComparison.Ordinal)||
           !GetWindowRect(new IntPtr(edit),out target)||!GetWindowRect(info.Focus,out focused)||
           target.Right<=target.Left||target.Bottom<=target.Top||focused.Right<=focused.Left||focused.Bottom<=focused.Top||
           focused.Left<target.Left||focused.Top<target.Top||focused.Right>target.Right||focused.Bottom>target.Bottom)return 0;
        return info.Focus.ToInt64();
    }
    public static long FocusDate(long root,long edit,long group) {
        DateScope(root,edit,group);Activate(root);
        uint pid,owner=GetWindowThreadProcessId(new IntPtr(root),out pid),current=GetCurrentThreadId();
        bool attached=owner!=current;
        // SetFocus is a real OS focus transition with normal WM_SETFOCUS /
        // WM_KILLFOCUS events, not a fabricated focus message. Attach only
        // to the verified owner queue, and detach even when the provider fails.
        if(attached&&!AttachThreadInput(current,owner,true))throw new Exception("Date focus queue unavailable; no input sent");
        try {
            if(!IsActiveOwner(root))throw new Exception("Date owner inactive; no input sent");
            SetFocus(new IntPtr(edit));
        } finally {if(attached)AttachThreadInput(current,owner,false);}
        for(int i=0;i<40;i++) {
            if(!IsActiveOwner(root))throw new Exception("Date owner changed; no keyboard input sent");
            long focused=VerifiedDateFocus(root,edit,group);
            if(focused!=0)return focused;
            System.Threading.Thread.Sleep(50);
        }
        throw new Exception("Exact date did not receive focus; no keyboard input sent");
    }
    public static string[] WriteDateText(long root,long edit,long group,string digits) {
        DateTime parsed;
        if(!DateTime.TryParseExact(digits,"yyyyMMdd",System.Globalization.CultureInfo.InvariantCulture,
                                  System.Globalization.DateTimeStyles.None,out parsed))throw new Exception("Invalid date input");
        long focus=FocusDate(root,edit,group);
        if(VerifiedDateFocus(root,edit,group)==0)throw new Exception("Date focus changed; no keyboard input sent");
        string beforeInput=Text(focus);
        System.Windows.Forms.SendKeys.SendWait("^a{BACKSPACE}");
        string afterClear=beforeInput;
        for(int i=0;i<20;i++) {
            focus=VerifiedDateFocus(root,edit,group);
            if(focus==0)throw new Exception("Date focus changed after clear; keyboard input stopped");
            afterClear=Text(focus);
            if(afterClear!=beforeInput)break;
            System.Threading.Thread.Sleep(50);
        }
        // An already-empty, owner-verified mask legitimately cannot change
        // when cleared. Still require every actual digit and the model's Tab
        // commit to be acknowledged; populated masks retain the strict guard.
        bool wasEmptyMask=beforeInput==""||beforeInput=="________"||beforeInput=="____/__/__";
        if(afterClear==beforeInput&&!wasEmptyMask)throw new Exception("Date clear not acknowledged; no digits or commit sent");
        focus=VerifiedDateFocus(root,edit,group);
        if(focus==0)throw new Exception("Date focus changed before caret selection; no digits sent");
        Message(focus,0xB1,0,0);
        foreach(char c in digits) {
            if(VerifiedDateFocus(root,edit,group)==0)throw new Exception("Focus changed; keyboard input stopped");
            string beforeCharacter=Text(focus);bool acknowledged=false;
            long beforeSelection=Message(focus,0xB0,0,0);
            int beforeCaret=(int)(beforeSelection&65535),beforeEnd=(int)((beforeSelection>>16)&65535);
            System.Windows.Forms.SendKeys.SendWait(c.ToString());
            for(int i=0;i<20;i++) {
                focus=VerifiedDateFocus(root,edit,group);
                if(focus==0)throw new Exception("Focus changed while awaiting character; keyboard input stopped");
                long afterSelection=Message(focus,0xB0,0,0);
                int afterCaret=(int)(afterSelection&65535),afterEnd=(int)((afterSelection>>16)&65535);
                // A mask may retain the same display for an identical digit,
                // while its actual input caret advances. That owned, collapsed
                // caret is positive acknowledgement, not a blind delay. The
                // complete eight digits and committed outer box must still
                // equal the exact requested calendar date below.
                if(Text(focus)!=beforeCharacter||(beforeCaret==beforeEnd&&afterCaret==afterEnd&&
                   beforeCaret>=0&&afterCaret>beforeCaret&&afterCaret<=10)) {acknowledged=true;break;}
                System.Threading.Thread.Sleep(25);
            }
            if(!acknowledged)throw new Exception("Date character not acknowledged; no further digits or commit sent; digit="+
                c+" before="+beforeCharacter+" after="+Text(focus)+" before_selection="+beforeSelection+
                " after_selection="+Message(focus,0xB0,0,0)+" target="+Text(edit));
        }
        focus=VerifiedDateFocus(root,edit,group);
        if(focus==0)throw new Exception("Date focus changed; commit not sent");
        string afterDigits=Text(focus);
        if(afterDigits.Replace("/","").Replace("-","")!=digits)throw new Exception("Date digits readback differs; commit not sent");
        System.Windows.Forms.SendKeys.SendWait("{TAB}");
        for(int i=0;i<40&&Text(edit).Replace("/","").Replace("-","")!=digits;i++)System.Threading.Thread.Sleep(50);
        if(Text(edit).Replace("/","").Replace("-","")!=digits)throw new Exception("Date readback differs; query refused");
        return new string[]{beforeInput,afterClear,afterDigits,Text(edit)};
    }
    public static void DateText(long root,long edit,long group,string digits) {
        // A native display text match is not proof of the legacy internal
        // date model. Always use the normal acknowledged input/Tab commit.
        WriteDateText(root,edit,group,digits);
    }
    public static string[] PreviewHeader(long grid,int maxColumns) {
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(grid),0xFFFFFFFCu,ref iid,out a));
        if(a.accChildCount<1)return new string[0];
        var row=a.get_accChild(1) as Accessibility.IAccessible;
        if(row==null)return new string[0];
        int columns=row.accChildCount;
        if(columns>maxColumns+2)return new string[0];
        var cells=new System.Collections.Generic.List<string>();
        for(int c=1;c<=columns;c++) {
            var cell=row.get_accChild(c) as Accessibility.IAccessible;
            cells.Add(cell!=null?cell.get_accName(0):row.get_accName(c));
        }
        return cells.ToArray();
    }
    public static int[] PreviewShape(long grid) {
        // Metadata only. A wide/different or empty result must not all look
        // like an empty header diagnostic; never read data-cell values here.
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(grid),0xFFFFFFFCu,ref iid,out a));
        int rows=a.accChildCount;if(rows<1)return new int[]{rows,0};
        var header=a.get_accChild(1) as Accessibility.IAccessible;
        return new int[]{rows,header==null?0:header.accChildCount};
    }
    public static string PreviewSignature(long grid) {
        // Constant bounded leading/trailing sample, not SourceRows/full-grid
        // traversal on every poll. Private hash detects a previous result;
        // identical or unproved outcomes stay unknown rather than fabricated.
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(grid),0xFFFFFFFCu,ref iid,out a));
        var text=new StringBuilder();int count=a.accChildCount;text.Append(count).Append(':');
        var indices=new System.Collections.Generic.SortedSet<int>(new int[]{1,2,3,count-2,count-1,count});
        foreach(int i in indices) {
            if(i<1||i>count)continue;
            var row=a.get_accChild(i) as Accessibility.IAccessible;if(row==null)continue;
            try {
                int role=Convert.ToInt32(row.get_accRole(0)),n=row.accChildCount;
                text.Append(i).Append(':').Append(role).Append(':').Append(n).Append(':');
                if(role!=28||n>32)continue;
                for(int c=1;c<=n;c++) {
                    string value;
                    try {value=i==1?row.get_accName(c):row.get_accValue(c);}
                    catch(COMException e) {
                        if(e.ErrorCode!=unchecked((int)0x80020003)&&e.ErrorCode!=unchecked((int)0x80070057))throw;
                        var cell=row.get_accChild(c) as Accessibility.IAccessible;
                        try {value=cell==null?"":i==1?cell.get_accName(0):cell.get_accValue(0);}
                        finally {if(cell!=null&&Marshal.IsComObject(cell))Marshal.ReleaseComObject(cell);}
                    }
                    value=value??"";text.Append(value.Length).Append(':').Append(value.Substring(0,Math.Min(256,value.Length))).Append(';');
                }
            }finally {if(Marshal.IsComObject(row))Marshal.ReleaseComObject(row);}
        }
        if(Marshal.IsComObject(a))Marshal.ReleaseComObject(a);
        using(var sha=System.Security.Cryptography.SHA256.Create())
            return BitConverter.ToString(sha.ComputeHash(Encoding.UTF8.GetBytes(text.ToString()))).Replace("-","").ToLowerInvariant();
    }
    private static Accessibility.IAccessible[] SourceRows(long grid,int maxRows) {
        Guid iid=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(grid),0xFFFFFFFCu,ref iid,out a));
        int children=a.accChildCount;
        if(children<1||children>maxRows+4)throw new Exception("Preview row bound exceeded");
        var rows=new System.Collections.Generic.List<Accessibility.IAccessible>();
        for(int i=1;i<=children;i++) {
            var child=a.get_accChild(i) as Accessibility.IAccessible;
            if(child==null)throw new Exception("Missing accessible grid child");
            int role=Convert.ToInt32(child.get_accRole(0));
            if(role==28)rows.Add(child); // ROLE_SYSTEM_ROW, including the header.
            else if(role!=3)throw new Exception("Unexpected non-row grid child role="+role);
            // ROLE_SYSTEM_SCROLLBAR has five children, not five market cells.
        }
        if(rows.Count>maxRows+2)throw new Exception("Source row bound exceeded");
        return rows.ToArray();
    }
    public static object[] Preview(long grid,int maxRows,int maxColumns) {
        var sourceRows=SourceRows(grid,maxRows);
        var result=new System.Collections.Generic.List<object>();
        // Read header and bounded leading/trailing rows before export clears it.
        for(int i=1;i<=sourceRows.Length;i++) {
            if(i>4&&i<sourceRows.Length-2)continue;
            var row=sourceRows[i-1];
            int columns=row.accChildCount;
            if(columns>maxColumns+2)throw new Exception("Preview column bound exceeded");
            var cells=new System.Collections.Generic.List<string>();
            for(int c=1;c<=columns;c++) {
                Accessibility.IAccessible cell=row.get_accChild(c) as Accessibility.IAccessible;
                cells.Add(i==1?(cell!=null?cell.get_accName(0):row.get_accName(c)):
                    (cell!=null?cell.get_accValue(0):row.get_accValue(c)));
            }
            result.Add(new object[]{i,cells.ToArray()});
        }
        return new object[]{sourceRows.Length,result.ToArray()};
    }
    public static object[][] FullPreview(long grid,int maxRows,int maxColumns,out int objectFallbackCells) {
        objectFallbackCells=0;
        var sourceRows=SourceRows(grid,maxRows);int count=sourceRows.Length;
        if(count<2)throw new Exception("Source Preview has no rows");
        var rows=new System.Collections.Generic.List<object[]>();long totalCharacters=0;
        for(int i=1;i<=count;i++) {
            var row=sourceRows[i-1];
            // accChildCount is a cross-process COM call, not a local property.
            // Read it once per row instead of once per financial cell.
            int columns=row.accChildCount,skip=columns-maxColumns;
            if(skip<0||skip>1)throw new Exception("Unexpected full Preview row schema at index "+i+": columns="+columns);
            var cells=new object[maxColumns];bool blank=true;
            // Header rows have a top-left corner child. Data rows in this
            // vendor can omit their row-header child; prove each row's shape
            // instead of shifting the company key out of the capture.
            for(int c=skip+1;c<=columns;c++) {
                string value;
                if(i==1) {
                    var cell=row.get_accChild(c) as Accessibility.IAccessible;
                    value=cell!=null?cell.get_accName(0):row.get_accName(c);
                } else {
                    try {value=row.get_accValue(c);} // MSAA supports child IDs directly.
                    catch(COMException error) {
                        // Only an unsupported property/child ID uses the
                        // established object path. A transport error stops.
                        if(error.ErrorCode!=unchecked((int)0x80020003)&&error.ErrorCode!=unchecked((int)0x80070057))throw;
                        var cell=row.get_accChild(c) as Accessibility.IAccessible;
                        value=cell!=null?cell.get_accValue(0):row.get_accValue(c);objectFallbackCells++;
                    }
                }
                if(value!=null)totalCharacters+=value.Length;
                if(totalCharacters>64000000)throw new Exception("Source display text exceeds reviewed memory bound; preserve result for scoped recovery");
                cells[c-skip-1]=value;if(!String.IsNullOrEmpty(value))blank=false;
            }
            if(i==count&&blank)continue; // DataGridView's empty new-row affordance.
            rows.Add(cells);
        }
        if(SourceRows(grid,maxRows).Length!=count)throw new Exception("Source grid changed during readback");
        return rows.ToArray();
    }
}
'@ -ReferencedAssemblies @([Accessibility.IAccessible].Assembly.Location,[Windows.Forms.SendKeys].Assembly.Location)

if(Test-Path -LiteralPath $Output){throw 'Refusing to overwrite bridge evidence'}
$requestDoc=Get-Content -LiteralPath $Request -Raw -Encoding UTF8 | ConvertFrom-Json
if($requestDoc.desktop_input_contract -and $requestDoc.desktop_input_contract -cne [TejBridgeNative]::InputContract){throw 'Incompatible desktop input contract'}
if($requestDoc.contract_version -ne 4 -or $requestDoc.action -notin @('inspect','inspect_notices','inspect_date_input','inspect_query_runtime','probe_date_focus','probe_date_input','confirm_metadata_error_cleared','plan','recover_plan','resume_plan_empty_fields','download','resolve_empty','recover_preview','ack_excel_error','ack_source_memory_error','ack_preview_column_limit')){throw 'Unsupported request contract'}
if($ExpectedTitle -cne ('TEJ Smart Wizard (Version 4.1.1.7) -- '+$ExpectedWorkbook)){throw 'Unreviewed wizard version or workbook'}
if($requestDoc.fields.Count -lt 1 -or $requestDoc.fields.Count -gt 2000){throw 'Unreviewed field bound'}
if($requestDoc.max_cells -lt 100 -or $requestDoc.max_cells -gt 400000 -or $requestDoc.max_rows -gt 10000){throw 'Unreviewed output bound'}
$mutex=[Threading.Mutex]::new($false,'Local\StockAgentTEJSmartWizardOwner')
$owns=$false
$querySubmissionPossible=$false
$sourceKeyMode=$null
$excel=$null;$book=$null;$sheet=$null;$range=$null
try {
    $owns=$mutex.WaitOne(0)
    if(-not $owns){throw 'Another StockAgent desktop query owns this session'}
    $root=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$ExpectedWindow)
    function Assert-Scope([long]$h) {
        $owner=[uint32]0
        [void][TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$h,[ref]$owner)
        if($root.Current.Name -cne $ExpectedTitle -or $root.Current.ProcessId -ne $TejProcessId -or
           $owner -ne $TejProcessId -or ($h -ne $ExpectedWindow -and -not [TejBridgeNative]::IsChild([IntPtr]$ExpectedWindow,[IntPtr]$h))){throw 'Query identity or control owner changed'}
    }
    if($root.Current.Name -cne $ExpectedTitle -or $root.Current.ProcessId -ne $TejProcessId){throw 'Wrong query scope'}
    # ActiveWorkbook/Application.Hwnd follow the user's current Excel view,
    # not the independent Wizard's pinned workbook. Read ONLY its exact named
    # book/window; never activate a workbook or select another Excel instance.
    try {
        $excel=[Runtime.InteropServices.Marshal]::GetActiveObject('Excel.Application')
        $book=$excel.Workbooks.Item($ExpectedWorkbook)
        if($null -eq $book -or $book.Name -cne $ExpectedWorkbook -or $book.Windows.Count -ne 1 -or
           $book.Windows.Item(1).Hwnd -ne $ExpectedExcelWindow -or
           -not [TejBridgeNative]::IsWindow([IntPtr]$ExpectedExcelWindow)) {
            throw 'Pinned workbook/window unavailable'
        }
    } catch {throw ('DESKTOP_CONTEXT_BEFORE_QUERY: '+$_.Exception.Message)}
    if($requestDoc.action -eq 'inspect_notices') {
        # Exact-owner, bounded diagnostic only. Text stays in the private
        # acquisition directory; no acknowledgement, source/input or query.
        $notices=@(foreach($handle in [TejBridgeNative]::Dialogs($ExpectedWindow)) {
            $notice=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
            if($notice.Current.ProcessId -ne $TejProcessId){throw 'Foreign notice owner'}
            $nodes=$notice.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
            if($nodes.Count -gt 64){throw 'Notice diagnostic bound exceeded'}
            @{handle=$handle;name=$notice.Current.Name;owner=[TejBridgeNative]::GetWindow([IntPtr]$handle,4).ToInt64();
              controls=@($nodes|ForEach-Object {@{class=$_.Current.ClassName;name=$_.Current.Name;enabled=$_.Current.IsEnabled;handle=$_.Current.NativeWindowHandle}})}
        })
        $processWindows=@(foreach($handle in [TejBridgeNative]::VisibleProcessWindows($ExpectedWindow)) {
            $window=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
            if($window.Current.ProcessId -ne $TejProcessId){throw 'Diagnostic window owner changed'}
            # Names and exact handles remain private, like notice text. No
            # text-box values, credentials or workbook cells are inspected.
            $labels=@();$childControlCount=$null;$childClasses=@();$childControlTypes=@()
            if($handle -ne $ExpectedWindow -and $window.Current.ClassName -ceq '#32770') {
                $nodes=$window.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
                if($nodes.Count -gt 64){throw 'Auxiliary dialog diagnostic bound exceeded'}
                $childControlCount=$nodes.Count
                $childClasses=@($nodes|ForEach-Object {$_.Current.ClassName})
                $childControlTypes=@($nodes|ForEach-Object {$_.Current.ControlType.ProgrammaticName})
                # Legacy connector controls can be exposed as unnamed Pane
                # elements rather than UIA Text/Button. Read only native
                # Static/Button captions, never Edit controls or credentials.
                $labels=@(foreach($class in @('Static','Button')) {
                    foreach($child in [TejBridgeNative]::Children($handle,$class)) {
                        $captionOwner=[uint32]0
                        [void][TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$child,[ref]$captionOwner)
                        if($captionOwner -ne $TejProcessId -or -not [TejBridgeNative]::IsChild([IntPtr]$handle,[IntPtr]$child)){throw 'Auxiliary caption owner changed'}
                        @{class=$class;native_caption=[TejBridgeNative]::Text($child);
                          enabled=[TejBridgeNative]::IsWindowEnabled([IntPtr]$child);handle=$child}
                    }
                })
                if($labels.Count -gt 64){throw 'Auxiliary native caption bound exceeded'}
            }
            @{handle=$handle;name=$window.Current.Name;class=$window.Current.ClassName;
              owner=[TejBridgeNative]::GetWindow([IntPtr]$handle,4).ToInt64();
              enabled=[TejBridgeNative]::IsWindowEnabled([IntPtr]$handle);labels=$labels;
              child_control_count=$childControlCount;child_classes=$childClasses;child_control_types=$childControlTypes}
        })
        $process=Get-Process -Id $TejProcessId
        $owner=[uint32]0;$thread=[TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$ExpectedWindow,[ref]$owner)
        if($owner -ne $TejProcessId){throw 'Diagnostic query owner changed'}
        $threadState=@($process.Threads|Where-Object {$_.Id -eq $thread}|ForEach-Object {
            @{state=[string]$_.ThreadState;reason=$(if($_.ThreadState -eq 'Wait'){[string]$_.WaitReason}else{$null})}})
        $payload=@{contract_version=4;action='inspect_notices';task_id=$requestDoc.task_id;notices=$notices;
                   root_enabled=[TejBridgeNative]::IsWindowEnabled([IntPtr]$ExpectedWindow);
                   root_visible=[TejBridgeNative]::IsWindowVisible([IntPtr]$ExpectedWindow);
                   root_automation_enabled=$root.Current.IsEnabled;process_windows=$processWindows;
                   normal_message_completed=[TejBridgeNative]::WindowResponds($ExpectedWindow);
                   hung_window=[TejBridgeNative]::IsHungAppWindow([IntPtr]$ExpectedWindow);
                   process_name=$process.ProcessName;process_private_bytes=$process.PrivateMemorySize64;
                   process_working_set_bytes=$process.WorkingSet64;query_thread_state=$threadState;
                   data_query_repeated=$false;notice_acknowledged=$false;credentials_read=$false}
        [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
        Write-Output 'Owned notices inspected; nothing acknowledged';exit
    }
    if($requestDoc.action -eq 'ack_preview_column_limit') {
        $h=[long]$requestDoc.error_window
        # Key=3/event formats can exceed 30 even with 28 selected features.
        # The exact owned E8033 is the rejection proof, not guessed key width.
        if($h -le 0 -or $h -notin [TejBridgeNative]::Dialogs($ExpectedWindow)){throw 'Exact rejected Preview and inspected HWND required'}
        $notice=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$h)
        if($notice.Current.ProcessId -ne $TejProcessId -or [TejBridgeNative]::GetWindow([IntPtr]$h,4).ToInt64() -ne $ExpectedWindow){throw 'Wrong Preview limit notice owner'}
        $nodes=$notice.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
        $texts=@($nodes|Where-Object {$_.Current.ClassName -ceq 'Static' -and -not [string]::IsNullOrEmpty($_.Current.Name)})
        $buttons=@($nodes|Where-Object {$_.Current.ClassName -ceq 'Button'})
        if($texts.Count -ne 1 -or $texts[0].Current.Name -cne '(E8033) Too many columns,  the system only provides 30-column preview !!' -or
           $buttons.Count -ne 1 -or $buttons[0].Current.Name -cne 'OK' -or -not $buttons[0].Current.IsEnabled -or
           -not [TejBridgeNative]::IsChild([IntPtr]$h,[IntPtr]$buttons[0].Current.NativeWindowHandle)){throw 'Unreviewed Preview limit layout; nothing acknowledged'}
        [TejBridgeNative]::Activate($h)
        [void][TejBridgeNative]::Message($buttons[0].Current.NativeWindowHandle,0xF5,0,0)
        $deadline=[DateTime]::UtcNow.AddSeconds(5)
        while([TejBridgeNative]::IsWindow([IntPtr]$h) -and [DateTime]::UtcNow -lt $deadline){Start-Sleep -Milliseconds 100}
        if([TejBridgeNative]::IsWindow([IntPtr]$h)){throw 'Exact Preview limit acknowledgement did not close; no repeat'}
        $payload=@{contract_version=4;provider='tej_smart_wizard';action='ack_preview_column_limit';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;observed_at_utc=[DateTime]::UtcNow.ToString('o');
            error_code='vendor_preview_30_column_limit';source_query_rejected=$true;data_query_repeated=$false;credentials_read=$false}
        [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
        Write-Output 'Exact Preview column-limit rejection acknowledged; no new query';exit
    }
    if($requestDoc.action -eq 'ack_source_memory_error') {
        # Explicit operator-only recovery of an independently inspected
        # runtime allocation failure. Not a quota/auth/permission bypass.
        $h=[long]$requestDoc.error_window
        if($h -le 0 -or $h -notin [TejBridgeNative]::Dialogs($ExpectedWindow)){throw 'Exact inspected memory-error HWND required'}
        $notice=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$h)
        if($notice.Current.ProcessId -ne $TejProcessId -or
           [TejBridgeNative]::GetWindow([IntPtr]$h,4).ToInt64() -ne $ExpectedWindow){throw 'Wrong memory-error owner'}
        $nodes=$notice.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
        $texts=@($nodes|Where-Object {$_.Current.ClassName -ceq 'Static' -and -not [string]::IsNullOrEmpty($_.Current.Name)})
        $buttons=@($nodes|Where-Object {$_.Current.ClassName -ceq 'Button'})
        $knownMessage=@{'vendor_mysql_allocation_failed'='MySql:malloc';'vendor_source_table_unresolved'='Cannot find table 0.'}[[string]$requestDoc.error_code]
        if([string]::IsNullOrEmpty($knownMessage) -or $texts.Count -ne 1 -or $texts[0].Current.Name -cne $knownMessage -or
           $buttons.Count -ne 1 -or $buttons[0].Current.Name -cne 'OK' -or -not $buttons[0].Current.IsEnabled){throw 'Unreviewed memory-error layout; nothing acknowledged'}
        if(-not [TejBridgeNative]::IsChild([IntPtr]$h,[IntPtr]$buttons[0].Current.NativeWindowHandle)){throw 'Wrong memory-error acknowledgement owner'}
        [TejBridgeNative]::Activate($h)
        [void][TejBridgeNative]::Message($buttons[0].Current.NativeWindowHandle,0xF5,0,0)
        $deadline=[DateTime]::UtcNow.AddSeconds(5)
        while([TejBridgeNative]::IsWindow([IntPtr]$h) -and [DateTime]::UtcNow -lt $deadline){Start-Sleep -Milliseconds 100}
        if([TejBridgeNative]::IsWindow([IntPtr]$h)){throw 'Exact memory-error notice did not close; no repeat'}
        $payload=@{contract_version=4;provider='tej_smart_wizard';action='ack_source_memory_error';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            observed_at_utc=[DateTime]::UtcNow.ToString('o');data_query_repeated=$false;
            error_code=$requestDoc.error_code;source_axes_require_revalidation=$true;credentials_read=$false}
        [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
        Write-Output 'Exact source allocation error acknowledged; no new data query';exit
    }
    if($requestDoc.action -eq 'ack_excel_error') {
        # Explicit operator recovery ONLY. Never called by the download loop.
        # Acknowledge the known runtime failure, not an entitlement/security
        # prompt. No retry, Office policy modification or COM repair is made.
        if([long]$requestDoc.error_window -le 0 -or [long]$requestDoc.error_window -eq $ExpectedWindow){throw 'Operator recovery requires an exact independently inspected error HWND'}
        $windows=[Windows.Automation.AutomationElement]::RootElement.FindAll([Windows.Automation.TreeScope]::Children,
            [Windows.Automation.PropertyCondition]::new([Windows.Automation.AutomationElement]::ProcessIdProperty,$TejProcessId))
        $found=[Collections.Generic.List[object]]::new()
        foreach($window in $windows) {
            # The vendor's error form is owned by a hidden add-in form, not
            # the query HWND. Require an explicit inspected exact error HWND
            # in addition to the process, version and exact failure text.
            if($window.Current.NativeWindowHandle -ne [long]$requestDoc.error_window){continue}
            $children=$window.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
            $texts=@($children|Where-Object {$_.Current.ClassName -ceq 'Static' -and
                $_.Current.Name.StartsWith("Export To Excel`r`nCannot create ActiveX component.`r`n",[StringComparison]::Ordinal)})
            $version=@($children|Where-Object {$_.Current.Name -ceq 'Version 4.1.1.7'})
            if($texts.Count -eq 1 -and $version.Count -eq 1){$found.Add($window)}
        }
        if($found.Count -ne 1){throw 'No unique owned known Excel runtime failure; nothing acknowledged'}
        $dialogs=@($found[0].FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)|Where-Object {
            $_.Current.ClassName -ceq '#32770' -and $_.Current.Name -ceq 'TEJAddin' -and $_.Current.IsEnabled})
        if($dialogs.Count -ne 1){throw 'Wrong known failure acknowledgement layout'}
        $ok=@($dialogs[0].FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)|Where-Object {
            $_.Current.ClassName -ceq 'Button' -and $_.Current.Name -ceq 'OK' -and $_.Current.IsEnabled})
        if($ok.Count -ne 1 -or -not [TejBridgeNative]::IsChild([IntPtr]$dialogs[0].Current.NativeWindowHandle,[IntPtr]$ok[0].Current.NativeWindowHandle)){throw 'Wrong failure acknowledgement owner'}
        # Standard BM_CLICK can fail on an inactive dialog. Activate only this
        # exact inspected vendor dialog, then verify it actually went away.
        [TejBridgeNative]::Activate($dialogs[0].Current.NativeWindowHandle)
        [void][TejBridgeNative]::Message($ok[0].Current.NativeWindowHandle,0xF5,0,0)
        $deadline=[DateTime]::UtcNow.AddSeconds(5)
        while([TejBridgeNative]::IsWindow([IntPtr]$dialogs[0].Current.NativeWindowHandle) -and [DateTime]::UtcNow -lt $deadline){Start-Sleep -Milliseconds 100}
        if([TejBridgeNative]::IsWindow([IntPtr]$dialogs[0].Current.NativeWindowHandle)){throw 'Known failure acknowledgement did not close the exact dialog'}
        $payload=@{contract_version=4;provider='tej_smart_wizard';action='ack_excel_error';task_id=$requestDoc.task_id;
            observed_at_utc=[DateTime]::UtcNow.ToString('o');data_query_repeated=$false;error_code='vendor_excel_com_initialization_failed';credentials_read=$false}
        [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
        Write-Output 'Known Excel runtime failure acknowledged; no new data query';exit
    }
    function Find-QueryGroup([string]$name) {
        $found=@(foreach($handle in [TejBridgeNative]::Children($ExpectedWindow,'')) {
            if(-not [TejBridgeNative]::IsWindowVisible([IntPtr]$handle)){continue}
            $node=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
            if($node.Current.Name -ceq $name){Assert-Scope $handle;$node}
        })
        if($found.Count -ne 1){throw ('Missing/ambiguous query group: '+$name)};return $found[0]
    }
    function Verify-EditableScope([switch]$BindingReadback) {
        if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0){throw 'Vendor notice blocks source preparation; nothing acknowledged'}
        $companyGroup=Find-QueryGroup 'Company Setting';$dateGroup=Find-QueryGroup 'Date Setting'
        if(-not $root.Current.IsEnabled -or -not $companyGroup.Current.IsEnabled -or
           ($sourceKeyMode -ne 1 -and -not $dateGroup.Current.IsEnabled)){throw 'Source company/date groups disabled; no cached axes or input adopted'}
        if($BindingReadback) {
            foreach($pair in @(@('Type',$requestDoc.type),@('SmartID',$requestDoc.smart_id),@('Data',$requestDoc.table))) {
                $h=Control $source $pair[0] '*COMBOBOX*';$labels=Items $h $true
                if($labels[(Message $h 0x147)] -cne $pair[1]){throw 'Final source binding changed; no axes/query adoption'}
            }
            $selection=Lists $source
            if($selection.Count -ne 2 -or ((Items $selection[1].Current.NativeWindowHandle) -join "`t") -cne (@($requestDoc.fields) -join "`t")){throw 'Final selected schema changed'}
        }
        $proof=@{contract='editable_source_scope_v1';company_group_enabled=$true;date_group_enabled=[bool]$dateGroup.Current.IsEnabled;vendor_notices_absent=$true;binding_readback_verified=[bool]$BindingReadback}
        if($sourceKeyMode -eq 1){$proof.source_key_mode=1;$proof.date_axis_not_used=$true}
        return $proof
    }
    function Verify-SourceSelectors {
        if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0 -or -not $root.Current.IsEnabled){throw 'Vendor notice/disabled root blocks source selection'}
        foreach($name in @('Type','SmartID','Data')) {
            $h=Control $source $name '*COMBOBOX*'
            if(-not [TejBridgeNative]::IsWindowEnabled([IntPtr]$h)){throw 'Source selector disabled; no binding change'}
        }
    }
    # Native tab enumeration never expands a previous result's virtual cells.
    $tabs=[TejBridgeNative]::Children($ExpectedWindow,'WindowsForms10.SysTabControl32.')
    if($tabs.Count -ne 1){throw 'Unexpected query tab layout'};Assert-Scope $tabs[0]
    [TejBridgeNative]::SelectTab($ExpectedWindow,$tabs[0],0)
    $source=Find-QueryGroup 'Data Source'
    function Native-Controls($group,[string]$prefix) {
        # Only enumerate real widgets, not virtual company/date list items or
        # the previous Preview's cells. Names/layout/state still come from UIA.
        $parent=$group.Current.NativeWindowHandle;Assert-Scope $parent
        foreach($handle in [TejBridgeNative]::Children($parent,$prefix)) {
            Assert-Scope $handle
            if(-not [TejBridgeNative]::IsWindowVisible([IntPtr]$handle)){continue}
            $node=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
            if($node.Current.NativeWindowHandle -ne $handle){throw 'Native widget identity differs'}
            $node
        }
    }
    function Control($group,[string]$name,[string]$class) {
        $prefix=@{'*BUTTON*'='WindowsForms10.BUTTON.';'*COMBOBOX*'='WindowsForms10.COMBOBOX.'}[$class]
        if(-not $prefix){throw 'Unreviewed native control class'}
        $matches=@(Native-Controls $group $prefix|Where-Object {([string]$_.Current.Name).Trim() -ceq $name})
        if($matches.Count -ne 1){throw ('Ambiguous control: '+$name)}
        Assert-Scope $matches[0].Current.NativeWindowHandle;return [long]$matches[0].Current.NativeWindowHandle
    }
    function Message([long]$h,[uint32]$m,[long]$w=0,[long]$l=0) {Assert-Scope $h;return [TejBridgeNative]::Message($h,$m,$w,$l)}
    function Items([long]$h,[bool]$combo=$false) {Assert-Scope $h;return ,[TejBridgeNative]::Items($h,$combo)}
    function Select-Combo([long]$h,[string]$name,[bool]$notifyEvenIfSelected=$false) {
        if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0 -or
           -not ([Windows.Automation.AutomationElement]::FromHandle([IntPtr]$h)).Current.IsEnabled){throw 'Notice/disabled source selector; no binding change'}
        $deadline=[DateTime]::UtcNow.AddSeconds(150);$i=-1
        do {
            try{$labels=Items $h $true;$i=[Array]::IndexOf($labels,$name)}catch{if([DateTime]::UtcNow -ge $deadline){throw}}
            if($i -ge 0){break};Start-Sleep -Milliseconds 250
        }while([DateTime]::UtcNow -lt $deadline)
        if($i -lt 0){throw 'Catalog binding did not become available; selection not sent'}
        if((Message $h 0x147) -eq $i -and -not $notifyEvenIfSelected){return}
        [void](Message $h 0x14E $i);$p=[TejBridgeNative]::GetParent([IntPtr]$h).ToInt64()
        $notify=(([TejBridgeNative]::GetDlgCtrlID([IntPtr]$h) -band 65535) -bor 65536)
        [TejBridgeNative]::NotifyBinding($p,$h,$notify)
        do {
            Start-Sleep -Milliseconds 250
            try{if((Message $h 0x147) -eq $i){return}}catch{if([DateTime]::UtcNow -ge $deadline){throw}}
        }while([DateTime]::UtcNow -lt $deadline)
        throw 'Catalog selection did not resolve; notification never repeated'
    }
    function Click-Button($group,[string]$name) {
        $h=Control $group $name '*BUTTON*'
        if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0 -or -not [TejBridgeNative]::IsWindowEnabled([IntPtr]$h)){throw 'Notice/disabled button; no action sent'}
        [void](Message $h 0xF5)
    }
    function Select-FieldList([string]$name,[long]$target,[int]$expectedCount) {
        $h=Control $source $name '*BUTTON*'
        if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0 -or -not [TejBridgeNative]::IsWindowEnabled([IntPtr]$h)){throw 'Notice/disabled schema selector; no action sent'}
        # Large schema selection can take longer than SendMessage's normal
        # read timeout. Submit ONCE, then only read count until it completes.
        if(-not [TejBridgeNative]::PostMessageW([IntPtr]$h,0xF5,[IntPtr]::Zero,[IntPtr]::Zero)){throw 'Field selection action not queued'}
        $deadline=[DateTime]::UtcNow.AddSeconds(150)
        do {
            Start-Sleep -Milliseconds 250
            try{$actual=Message $target 0x18B}catch{if([DateTime]::UtcNow -ge $deadline){throw};continue}
            if($actual -eq $expectedCount){return}
        }while([DateTime]::UtcNow -lt $deadline)
        throw 'Field selection readback deadline exceeded; action never repeated'
    }
    function Set-Checkbox($group,[string]$name,[bool]$checked) {
        $matches=@(Native-Controls $group 'WindowsForms10.BUTTON.'|Where-Object {
            ([string]$_.Current.Name).Trim() -ceq $name -and
            ([TejBridgeNative]::AccessibleState($_.Current.NativeWindowHandle))[0] -eq 44
        })
        if($matches.Count -ne 1){throw ('Missing/ambiguous checkbox: '+$name)}
        $h=$matches[0].Current.NativeWindowHandle;Assert-Scope $h
        $state=([TejBridgeNative]::AccessibleState($h))[1]
        if(($state -band 33) -ne 0){throw 'Checkbox is disabled or indeterminate'}
        if((($state -band 16) -ne 0) -ne $checked){[void](Message $h 0xF5)}
        $state=([TejBridgeNative]::AccessibleState($h))[1]
        if((($state -band 16) -ne 0) -ne $checked){throw ('Checkbox readback differs: '+$name)}
        return $h
    }
    function Select-Company {
        # Company Setting has TWO "Select" buttons: text-search selection and
        # the list-to-selected-list action. Use the one below the actual list.
        $bottom=$companyLists[2].Current.BoundingRectangle.Bottom
        $buttons=@(Native-Controls $company 'WindowsForms10.BUTTON.'|Where-Object {
            $_.Current.Name -ceq 'Select' -and $_.Current.BoundingRectangle.Top -ge $bottom
        })
        if($buttons.Count -ne 1){throw 'Ambiguous company-list selection action'}
        [void](Message $buttons[0].Current.NativeWindowHandle 0xF5)
    }
    function Lists($group) {
        return @(Native-Controls $group 'WindowsForms10.LISTBOX.'|Sort-Object {$_.Current.BoundingRectangle.Y},{$_.Current.BoundingRectangle.X})
    }
    function Read-SourceKeyMode {
        $labels=@(Native-Controls $source 'WindowsForms10.STATIC.'|Where-Object {$_.Current.Name -cmatch '^Key=([123])\s*$'})
        if($labels.Count -ne 1){throw 'Unique source Key=1/2/3 format label required'}
        [void]($labels[0].Current.Name -cmatch '^Key=([123])\s*$')
        return [int]$Matches[1]
    }
    if($requestDoc.action -in @('confirm_metadata_error_cleared','inspect_date_input','inspect_query_runtime','probe_date_focus','probe_date_input')) {
        # The settlement/inspection actions are read-only. Explicit focus /
        # same-value input probes below do not submit or adopt source queries.
        Verify-SourceSelectors
        $actualBinding=@{};$bindingSame=$true
        foreach($pair in @(@('Type',$requestDoc.type),@('SmartID',$requestDoc.smart_id),@('Data',$requestDoc.table))) {
            $h=Control $source $pair[0] '*COMBOBOX*';$labels=Items $h $true
            $index=Message $h 0x147
            if($index -lt 0 -or $index -ge $labels.Count){throw 'Source selector readback unavailable; no settlement'}
            $actualBinding[$pair[0]]=$labels[$index]
            if($labels[$index] -cne $pair[1]){$bindingSame=$false}
        }
        # A second read-only pass must be stable before another table uses
        # the shared interface. No selection notification is repeated.
        Start-Sleep -Milliseconds 300
        Verify-SourceSelectors
        foreach($name in @('Type','SmartID','Data')) {
            $h=Control $source $name '*COMBOBOX*';$labels=Items $h $true;$index=Message $h 0x147
            if($index -lt 0 -or $index -ge $labels.Count -or $labels[$index] -cne $actualBinding[$name]){throw 'Source binding still changing; no metadata isolation'}
        }
        $payload=@{contract_version=4;provider='tej_smart_wizard';action=$requestDoc.action;task_id=$requestDoc.task_id;
            desktop_input_contract=[TejBridgeNative]::InputContract;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            observed_at_utc=[DateTime]::UtcNow.ToString('o');vendor_notices_absent=$true;
            actual_binding=$actualBinding;binding_matches_failed_plan=$bindingSame;source_axes_adopted=$false;source_binding_stable=$true;
            current_field_lists=@((Lists $source)|ForEach-Object {@{items=(Items $_.Current.NativeWindowHandle);style=[TejBridgeNative]::GetWindowLong([IntPtr]$_.Current.NativeWindowHandle,-16)}});
            source_selectors_enabled=$true;company_group_enabled=(Find-QueryGroup 'Company Setting').Current.IsEnabled;
            date_group_enabled=(Find-QueryGroup 'Date Setting').Current.IsEnabled;market_data_query_submitted=$false;credentials_read=$false}
        if($requestDoc.action -in @('inspect_date_input','inspect_query_runtime','probe_date_focus','probe_date_input')) {
            $payload.date_input_controls=@(Native-Controls (Find-QueryGroup 'Date Setting') 'WindowsForms10.EDIT.'|ForEach-Object {
                $value=$null;$supportsValue=$_.TryGetCurrentPattern([Windows.Automation.ValuePattern]::Pattern,[ref]$value)
                @{handle=$_.Current.NativeWindowHandle;native_text=[TejBridgeNative]::Text($_.Current.NativeWindowHandle);
                  enabled=$_.Current.IsEnabled;keyboard_focusable=$_.Current.IsKeyboardFocusable;offscreen=$_.Current.IsOffscreen;
                  supports_value_pattern=$supportsValue;value_readonly=$(if($supportsValue){$value.Current.IsReadOnly}else{$null});
                  native_style=[TejBridgeNative]::GetWindowLong([IntPtr]$_.Current.NativeWindowHandle,-16);
                  supported_patterns=@($_.GetSupportedPatterns()|ForEach-Object {$_.ProgrammaticName})}
            })
        }
        if($requestDoc.action -eq 'inspect_query_runtime') {
            # Capability/readback only: no default action is performed, no
            # source event/input/query is sent and no queue state is changed.
            $h=Control $root 'Preview' '*BUTTON*'
            $node=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$h)
            $invoke=$null;$supportsInvoke=$node.TryGetCurrentPattern([Windows.Automation.InvokePattern]::Pattern,[ref]$invoke)
            $payload.preview_button=@{handle=$h;enabled=$node.Current.IsEnabled;visible=[TejBridgeNative]::IsWindowVisible([IntPtr]$h);
                accessible_role_state=@([TejBridgeNative]::AccessibleState($h));default_action=[TejBridgeNative]::AccessibleDefaultAction($h);
                supports_invoke_pattern=$supportsInvoke;supported_patterns=@($node.GetSupportedPatterns()|ForEach-Object {$_.ProgrammaticName})}
            $payload.current_query_axes=@(foreach($g in @((Find-QueryGroup 'Company Setting'),(Find-QueryGroup 'Date Setting'))){
                foreach($list in (Lists $g)){@{handle=$list.Current.NativeWindowHandle;items=(Items $list.Current.NativeWindowHandle)}}
            })
            $payload.source_binding_unchanged=$true;$payload.date_text_input_sent=$false;
            $payload.query_button_invoked=$false;$payload.source_rows_adopted=$false
            $payload.source_key_labels=@(Native-Controls $source 'WindowsForms10.STATIC.'|ForEach-Object {
                @{name=$_.Current.Name;handle=$_.Current.NativeWindowHandle}
            })
            $payload.source_key_mode=Read-SourceKeyMode
        }
        if($requestDoc.action -eq 'probe_date_focus') {
            if(-not $bindingSame){throw 'Focus probe requires exact current source; no focus change'}
            $dateGroup=Find-QueryGroup 'Date Setting'
            $outer=@($payload.date_input_controls|Where-Object {$_.native_text -cmatch '^\d{4}/\d{2}/\d{2}$'}|Sort-Object {([Windows.Automation.AutomationElement]::FromHandle([IntPtr]$_.handle)).Current.BoundingRectangle.X})
            if($outer.Count -ne 2){throw 'Focus probe requires two stable date boxes'}
            $payload.date_focus_results=@(foreach($edit in $outer) {
                $focus=[TejBridgeNative]::FocusDate($ExpectedWindow,$edit.handle,$dateGroup.Current.NativeWindowHandle)
                @{target_handle=$edit.handle;focus_handle=$focus;target_text_after=[TejBridgeNative]::Text($edit.handle);
                  focus_text=[TejBridgeNative]::Text($focus);verified_exact_box=$true}
            })
            $payload.date_text_input_sent=$false
        }
        if($requestDoc.action -eq 'probe_date_input') {
            # Operator-only input acceptance, never called by the scheduler.
            # Re-enter only the SAME two dates, with no Search/Preview or
            # source changes. Do not accept the production no-op fast path as
            # evidence that a real keyboard write has worked.
            if(-not $bindingSame){throw 'Input probe requires exact current source; no date change'}
            $dateGroup=Find-QueryGroup 'Date Setting';$companyGroup=Find-QueryGroup 'Company Setting'
            $outer=@($payload.date_input_controls|Where-Object {$_.native_text -cmatch '^\d{4}/\d{2}/\d{2}$'}|Sort-Object {([Windows.Automation.AutomationElement]::FromHandle([IntPtr]$_.handle)).Current.BoundingRectangle.X})
            if($outer.Count -ne 2 -or $outer[0].native_text.Replace('/','-') -cne $requestDoc.start -or
               $outer[1].native_text.Replace('/','-') -cne $requestDoc.end -or
               ($payload.current_field_lists[-1].items -join "`t") -cne (@($requestDoc.fields) -join "`t")){
                throw 'Input probe requires original dates/schema; no date change'
            }
            $axes=@(foreach($g in @($companyGroup,$dateGroup)){foreach($list in (Lists $g)){
                @{handle=$list.Current.NativeWindowHandle;items=(Items $list.Current.NativeWindowHandle)}
            }})
            $payload.date_input_results=@(foreach($edit in $outer) {
                $trace=[TejBridgeNative]::WriteDateText($ExpectedWindow,$edit.handle,$dateGroup.Current.NativeWindowHandle,$edit.native_text.Replace('/',''))
                $actual=[TejBridgeNative]::Text($edit.handle)
                if($actual -cne $edit.native_text){throw 'Input probe did not preserve exact original date format; no query'}
                @{same_value_write=$true;target_text_before=$edit.native_text;target_text_after=$actual;verified_exact_box=$true;
                  input_trace=@($trace);intermediate_edit_observed=($trace[0] -cne $trace[1])}
            })
            foreach($axis in $axes){
                $actualItems=Items $axis.handle
                if(($actualItems -join "`t") -cne ($axis.items -join "`t")){throw 'Input probe changed cached axes; no query'}
            }
            foreach($name in @('Type','SmartID','Data')) {
                $h=Control $source $name '*COMBOBOX*';$labels=Items $h $true
                if($labels[(Message $h 0x147)] -cne $actualBinding[$name]){throw 'Input probe source changed; no query'}
            }
            $finalLists=Lists $source;$finalFields=Items $finalLists[-1].Current.NativeWindowHandle
            if(($finalFields -join "`t") -cne (@($requestDoc.fields) -join "`t")){throw 'Input probe schema changed; no query'}
            if(@($payload.date_input_results|Where-Object {-not $_.intermediate_edit_observed}).Count -ne 0){throw 'Input probe did not observe a real date edit; no query'}
            $payload.date_text_input_sent=$true;$payload.query_axes_unchanged=$true;$payload.source_binding_unchanged=$true;
            $payload.field_selection_unchanged=$true;$payload.early_noop_path_used=$false
        }
        [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 6 -Compress),[Text.UTF8Encoding]::new($false))
        Write-Output 'Failed metadata context settled; no new query or scope adoption';exit
    }
    function Sector-Control($group) {
        # Bond layouts mislabel a numeric Unit/Thousand/Million dropdown as
        # "Sector" too. Identify the actual universe filter by its ALL menu.
        $visible=@(Native-Controls $group 'WindowsForms10.COMBOBOX.')
        if($visible.Count -eq 0){return [long]0} # Bond series hide the inapplicable sector filter.
        $matches=@($visible|Where-Object {
            ([string]$_.Current.Name).Trim() -ceq 'Sector' -and 'ALL' -cin (Items $_.Current.NativeWindowHandle $true)
        })
        if($matches.Count -ne 1){throw 'No unique ALL-sector universe filter'}
        return [long]$matches[0].Current.NativeWindowHandle
    }
    function NumericScale-Control {
        $matches=@(Native-Controls $root 'WindowsForms10.COMBOBOX.'|Where-Object {
            ((Items $_.Current.NativeWindowHandle $true) -join "`t") -ceq "-`tUnit`tThousand`tMillion`tBillion"
        })
        if($matches.Count -gt 1){throw 'Ambiguous vendor numeric-scale selector'}
        if($matches.Count -eq 0){return [long]0}
        return [long]$matches[0].Current.NativeWindowHandle
    }
    function Read-NumericScale {
        $h=NumericScale-Control
        if($h -eq 0){return 'no_visible_scale_selector_unverified'}
        $labels=Items $h $true
        $i=Message $h 0x147
        if($i -lt 0 -or $i -ge $labels.Count){throw 'Vendor numeric scale selection unavailable'}
        if($labels[$i] -ceq '-'){return 'vendor_default_dash_not_all_units_verified'}
        return 'vendor_scale_'+$labels[$i]+'_units_require_source_review'
    }
    function Set-NumericScaleDefault {
        $h=NumericScale-Control
        if($h -ne 0){Select-Combo $h '-'}
        $selection=Read-NumericScale
        if($h -ne 0 -and $selection -cne 'vendor_default_dash_not_all_units_verified'){throw 'Vendor default numeric scale did not apply'}
        return $selection
    }
    function Test-NativeFeatureHeaders([string[]]$actual,[string[]]$expected) {
        if($actual.Count -ne $expected.Count){return $false}
        if(($actual -join "`t") -ceq ($expected -join "`t")){return $true}
        $mapped=[Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $native=[Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        for($i=0;$i -lt $expected.Count;$i++) {
            $caption=$expected[$i].Replace(',',' ')
            if(-not $mapped.Add($caption) -or -not $native.Add($actual[$i]) -or
               ($actual[$i] -cne $expected[$i] -and $actual[$i] -cne $caption)){return $false}
        }
        return $true
    }
    function Matching-Preview {
        # The vendor has two controls named DataGridView: data and notes.
        # Identify the data grid by its actual source schema, not screen order.
        $expected=@($requestDoc.fields)
        $keysCount=$(if($requestDoc.source_key_mode -eq 1 -and $requestDoc.key_layout_contract -ceq 'native_company_observed_snapshot_key1_v1'){1}elseif($requestDoc.source_key_mode -eq 3 -and $requestDoc.key_layout_contract -ceq 'native_company_period_record_key3_v1'){3}else{2})
        $found=[Collections.Generic.List[object]]::new()
        $diagnostic=[Collections.Generic.List[object]]::new()
        # UIA IsOffscreen can be false for the vendor's natively hidden
        # controls (reproduced on the stale sector menu). A previous table's
        # same-schema grid must not be adopted while Main Page is still active.
        if((Message $tabs[0] 0x130B) -ne 3){return @()}
        # Enumerate real HWND containers, not every virtual data cell. A
        # TrueCondition UIA traversal here materialized the entire result grid
        # on each poll, duplicating the full cell read and inflating CPU/RAM.
        foreach($handle in [TejBridgeNative]::Children($ExpectedWindow,'WindowsForms10.Window.')) {
            if(-not [TejBridgeNative]::IsWindowVisible([IntPtr]$handle)){continue}
            $candidate=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
            if($candidate.Current.Name -cne 'DataGridView'){continue}
            Assert-Scope $candidate.Current.NativeWindowHandle
            $header=[TejBridgeNative]::PreviewHeader($candidate.Current.NativeWindowHandle,$expected.Count+$keysCount)
            if($header.Count -eq $expected.Count+$keysCount+1 -and $header[0] -cin @('','Top Left Header Cell')){$header=$header[1..($header.Count-1)]}
            $diagnostic.Add(@{handle=$candidate.Current.NativeWindowHandle;offscreen=$candidate.Current.IsOffscreen;
                header=@($header);expected=@($expected);expected_count=$expected.Count;
                native_shape=[TejBridgeNative]::PreviewShape($candidate.Current.NativeWindowHandle)})
            if($candidate.Current.IsOffscreen){continue}
            if($header.Count -ne ($expected.Count+$keysCount) -or [string]::IsNullOrEmpty($header[0]) -or [string]::IsNullOrEmpty($header[1])){continue}
            if(-not (Test-NativeFeatureHeaders $header[$keysCount..($header.Count-1)] $expected)){continue}
            $found.Add($candidate)
        }
        if($found.Count -ne 1) {
            [IO.File]::WriteAllText(($Output+'.preview-scope.json'),($diagnostic.ToArray()|ConvertTo-Json -Depth 4 -Compress),[Text.UTF8Encoding]::new($false))
        }
        return $found.ToArray()
    }
    function Show-ExistingPreview {
        $tabs=[TejBridgeNative]::Children($ExpectedWindow,'WindowsForms10.SysTabControl32.')
        if($tabs.Count -ne 1){throw 'Unexpected existing Preview tab layout'}
        $h=$tabs[0];Assert-Scope $h
        # Verified v4.1.1.7 tabs: Main Page, Transformation, Style Format,
        # Preview. The initial source readback navigated to Main Page, hiding
        # the preserved result controls; recovery returns to Preview (3).
        # Standard TCM_SETCURFOCUS sends the normal selection notification.
        # Unlike the Preview BUTTON this changes only the visible tab; it
        # does not send a data query or alter the verified query selections.
        [TejBridgeNative]::SelectTab($ExpectedWindow,$h,3)
        Start-Sleep -Milliseconds 250
    }
    function Capture-Preview($grid) {
        Assert-Scope $grid.Current.NativeWindowHandle
        $keysCount=$(if($requestDoc.source_key_mode -eq 1 -and $requestDoc.key_layout_contract -ceq 'native_company_observed_snapshot_key1_v1'){1}elseif($requestDoc.source_key_mode -eq 3 -and $requestDoc.key_layout_contract -ceq 'native_company_period_record_key3_v1'){3}else{2})
        $columns=$requestDoc.fields.Count+$keysCount
        $sample=[TejBridgeNative]::Preview($grid.Current.NativeWindowHandle,$requestDoc.max_rows,$columns)
        $fallbackCells=0
        $rows=[TejBridgeNative]::FullPreview($grid.Current.NativeWindowHandle,$requestDoc.max_rows,$columns,[ref]$fallbackCells)
        if($rows.Count -lt 2 -or $rows.Count*$columns -gt $requestDoc.max_cells){throw 'Source grid cell bound exceeded'}
        if(-not $selectedFieldOrderVerified -or
           -not (Test-NativeFeatureHeaders $rows[0][$keysCount..($columns-1)] @($requestDoc.fields))){throw 'Native header/source field order correspondence not verified'}
        return @{contract_version=4;provider='tej_smart_wizard';action='download';observed_at_utc=[DateTime]::UtcNow.ToString('o');
            query_attempt_id=$requestDoc.query_attempt_id;preview_submission_contract=$(if($requestDoc.action -eq 'download' -or $requestDoc.query_attempt_id){[TejBridgeNative]::SubmissionContract}else{$null});
            fresh_preview_transition_verified=[bool]$freshPreviewTransitionVerified;
            task_id=$requestDoc.task_id;type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;fields=@($requestDoc.fields);
            source_key_headers=@($rows[0][0..($keysCount-1)]);source_key_mode=$requestDoc.source_key_mode;
            key_layout_contract=$requestDoc.key_layout_contract;cells=$rows;preview=$sample;
            preview_header_mapping_contract='injective_literal_comma_to_space_preview_headers_v1';
            selected_field_order_readback_verified=$true;
            universe_scope='exact_type_smart_id_all_sectors';calendar_date_mode=$(if($keysCount -eq 1){$null}else{$false});
            checkbox_verification_method='msaa_role44_state_flags';
            capture_method='native_msaa_preview_full';source_value_representation='vendor_display_strings_not_underlying_excel_values';
            msaa_value_read_path='row_child_id_with_bounded_object_path_parity';msaa_object_fallback_cells=$fallbackCells;
            vendor_numeric_scale_selection=$numericScale;
            vendor_numeric_scale_readback_basis=$(if($requestDoc.action -eq 'recover_preview'){'current_ui_only_original_query_setting_unverified'}else{'prepreview_readback_for_this_query'});
            sector_filter_applicable=$sectorFilterApplicable;
            source_scope_proof=$sourceScopeProof;
            source_grid_rows=$rows.Count-1;source_grid_columns=$columns;
            date_axis=$(if($keysCount -eq 1){'observed_source_snapshot_no_historical_date_claim'}else{'requested_smart_wizard_grid_not_native_observation_dates'});provider_http_requests=$null;credentials_read=$false;query_comments_read=$false}
    }
    function Empty-Response {
        $dialogs=@(foreach($handle in [TejBridgeNative]::Dialogs($ExpectedWindow)) {
            [Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
        })
        if($dialogs.Count -eq 0){return $null}
        if($dialogs.Count -ne 1){throw 'Multiple vendor dialogs; no acknowledgement or query retry'}
        $dialog=$dialogs[0];$h=$dialog.Current.NativeWindowHandle
        $owner=[TejBridgeNative]::GetWindow([IntPtr]$h,4).ToInt64()
        if($dialog.Current.ProcessId -ne $TejProcessId -or ($owner -ne $ExpectedWindow -and -not [TejBridgeNative]::IsChild([IntPtr]$ExpectedWindow,[IntPtr]$h))){throw 'Dialog owner differs'}
        $children=$dialog.FindAll([Windows.Automation.TreeScope]::Descendants,[Windows.Automation.Condition]::TrueCondition)
        if($children.Count -gt 64){throw 'Owned dialog diagnostic bound exceeded; no acknowledgement'}
        $texts=@($children|Where-Object {$_.Current.ClassName -ceq 'Static' -and $_.Current.Name -cmatch '^ERROR1:No data !!\([a-zA-Z0-9_]{1,32}\)\s*$'})
        if($texts.Count -ne 1) {
            # A transient notice may disappear before an operator can inspect
            # it. Save only exact-owner Static/Button captions, not Edit values,
            # before stopping. This is evidence, not an acknowledgement or a
            # licence to treat progress/authentication/quota errors as empty.
            $captions=@(foreach($class in @('Static','Button')) {
                foreach($child in [TejBridgeNative]::Children($h,$class)) {
                    $captionOwner=[uint32]0
                    [void][TejBridgeNative]::GetWindowThreadProcessId([IntPtr]$child,[ref]$captionOwner)
                    if($captionOwner -ne $TejProcessId -or -not [TejBridgeNative]::IsChild([IntPtr]$h,[IntPtr]$child)){throw 'Unknown notice caption owner changed'}
                    $caption=[TejBridgeNative]::Text($child)
                    if($caption.Length -gt 4096){throw 'Unknown notice caption bound exceeded'}
                    @{class=$class;text=$caption;enabled=[TejBridgeNative]::IsWindowEnabled([IntPtr]$child)}
                }
            })
            if($captions.Count -gt 64){throw 'Unknown notice control bound exceeded'}
            $noticeProof=@{contract='exact_owned_unknown_notice_no_acknowledgement_v1';
                task_id=$requestDoc.task_id;query_attempt_id=$requestDoc.query_attempt_id;
                observed_at_utc=[DateTime]::UtcNow.ToString('o');handle=$h;owner=$owner;
                class=$dialog.Current.ClassName;controls=$captions;
                notice_acknowledged=$false;data_query_repeated=$false;credentials_read=$false}
            [IO.File]::WriteAllText(($Output+'.notice.json'),($noticeProof|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
            throw 'Unrecognized vendor dialog; permission/quota/errors are never dismissed automatically'
        }
        $ok=@($children|Where-Object {$_.Current.ClassName -ceq 'Button' -and $_.Current.Name -ceq 'OK'})
        if($ok.Count -ne 1 -or -not $ok[0].Current.IsEnabled){throw 'No unique normal empty-response acknowledgement'}
        $messageText=$texts[0].Current.Name.Trim()
        $ack=$ok[0].Current.NativeWindowHandle
        if(-not [TejBridgeNative]::IsChild([IntPtr]$h,[IntPtr]$ack)){throw 'Wrong empty-response acknowledgement owner'}
        # This acknowledges a normal, exact empty query response. It NEVER
        # dismisses a permission, quota, login, server-error or unknown notice.
        [TejBridgeNative]::Activate($h)
        [void][TejBridgeNative]::Message($ack,0xF5,0,0)
        $closedBy=[DateTime]::UtcNow.AddSeconds(5)
        while([TejBridgeNative]::IsWindow([IntPtr]$h) -and [DateTime]::UtcNow -lt $closedBy){Start-Sleep -Milliseconds 100}
        if([TejBridgeNative]::IsWindow([IntPtr]$h)){throw 'Exact normal empty acknowledgement did not close; no repeat'}
        return @{contract_version=4;provider='tej_smart_wizard';action='download';task_id=$requestDoc.task_id;
                 source_key_mode=$requestDoc.source_key_mode;key_layout_contract=$requestDoc.key_layout_contract;
                 query_attempt_id=$requestDoc.query_attempt_id;preview_submission_contract=$(if($requestDoc.action -eq 'download' -or $requestDoc.query_attempt_id){[TejBridgeNative]::SubmissionContract}else{$null});
                 observed_at_utc=[DateTime]::UtcNow.ToString('o');type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
                 fields=@($requestDoc.fields);universe_scope='exact_type_smart_id_all_sectors';calendar_date_mode=$(if($requestDoc.source_key_mode -eq 1){$null}else{$false});
                 checkbox_verification_method='msaa_role44_state_flags';date_axis=$(if($requestDoc.source_key_mode -eq 1){'observed_source_snapshot_no_historical_date_claim'}else{'requested_smart_wizard_grid_not_native_observation_dates'});
                 source_outcome='explicit_empty_scope';source_message=$messageText;capture_method='owned_vendor_empty_dialog';
                 vendor_numeric_scale_selection=$numericScale;sector_filter_applicable=$sectorFilterApplicable;
                 source_scope_proof=$sourceScopeProof;
                 vendor_numeric_scale_readback_basis=$(if($requestDoc.action -eq 'resolve_empty'){'current_ui_only_original_query_setting_unverified'}else{'prepreview_readback_for_this_query'});
                 provider_http_requests=$null;credentials_read=$false;query_comments_read=$false}
    }
    if($requestDoc.action -in @('resolve_empty','recover_preview')) {
        # Readback reconciles the ONE pending preview's actual selections; no
        # new Preview or source request is sent to discover its outcome.
        foreach($pair in @(@('Type',$requestDoc.type),@('SmartID',$requestDoc.smart_id),@('Data',$requestDoc.table))) {
            $h=Control $source $pair[0] '*COMBOBOX*';$labels=Items $h $true
            if($labels[(Message $h 0x147)] -cne $pair[1]){throw 'Empty-response source binding changed'}
        }
        $fieldLists=Lists $source;$company=Find-QueryGroup 'Company Setting';$dates=Find-QueryGroup 'Date Setting'
        $companyLists=Lists $company;$dateLists=Lists $dates
        if($fieldLists.Count -ne 2 -or $companyLists.Count -ne 4 -or $dateLists.Count -ne 2){throw 'Response context layout differs'}
        $sector=Sector-Control $company;$sectorFilterApplicable=$sector -ne 0
        if($sectorFilterApplicable) {
            $sectorNames=Items $sector $true
            if($sectorNames[(Message $sector 0x147)] -cne 'ALL'){throw 'Empty-response company universe changed'}
        }
        # Existing results MUST retain their actual scale; read it without
        # changing a selector or claiming older results used today's default.
        $numericScale=Read-NumericScale
        if($requestDoc.source_key_mode -ne 1) {
            $calendar=Control $dates 'Calendar Date' '*BUTTON*'
            $calendarState=[TejBridgeNative]::AccessibleState($calendar)
            if($calendarState[0] -ne 44 -or ($calendarState[1] -band 48) -ne 0){throw 'Empty-response calendar mode changed'}
        }
        $selectionChecks=@(@($fieldLists[1].Current.NativeWindowHandle,$requestDoc.fields),@($companyLists[3].Current.NativeWindowHandle,$requestDoc.company_labels))
        if($requestDoc.source_key_mode -ne 1){$selectionChecks+=,@($dateLists[1].Current.NativeWindowHandle,$requestDoc.date_labels)}
        foreach($check in $selectionChecks) {
            if(-not [TejBridgeNative]::SameItems([string[]](Items $check[0]),[string[]]$check[1])){throw 'Empty-response selections changed'}
        }
        $selectedFieldOrderVerified=((Items $fieldLists[1].Current.NativeWindowHandle) -join "`t") -ceq (@($requestDoc.fields) -join "`t")
        if(-not $selectedFieldOrderVerified){throw 'Existing-response selected field order changed; no adoption'}
        # Modal No-data replies may temporarily disable the current UI. Adopt
        # only the ONE original pre-Preview scope attestation, not today's mode.
        $sourceScopeProof=$requestDoc.original_query_scope_proof
        if($null -eq $sourceScopeProof -or $sourceScopeProof.contract -cne 'editable_source_scope_v1' -or
           $sourceScopeProof.company_group_enabled -ne $true -or
           ($requestDoc.source_key_mode -ne 1 -and $sourceScopeProof.date_group_enabled -ne $true) -or
           $sourceScopeProof.vendor_notices_absent -ne $true -or $sourceScopeProof.binding_readback_verified -ne $true){throw 'Exact original query scope proof missing; no new query'}
        if($requestDoc.action -eq 'resolve_empty') {
            $payload=Empty-Response
            if($null -eq $payload){throw 'No exact empty response to reconcile'}
        } else {
            if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0){throw 'Vendor notice blocks existing-result recovery'}
            Show-ExistingPreview
            $grids=@(Matching-Preview)
            if($grids.Count -ne 1){throw 'No unique schema-matched existing Preview; no new query'}
            if($requestDoc.query_attempt_id) {
                $original=$requestDoc.original_query_stage
                $signature=[TejBridgeNative]::PreviewSignature($grids[0].Current.NativeWindowHandle)
                if($original.query_attempt_id -cne $requestDoc.query_attempt_id -or
                   $original.preview_submission_contract -cne [TejBridgeNative]::SubmissionContract -or
                   $null -eq $original.before_preview_signatures -or $signature -cin $original.before_preview_signatures){
                    throw 'Existing result has no proved transition from its original attempt; no adoption'
                }
                $freshPreviewTransitionVerified=$true
            }
            $payload=Capture-Preview $grids[0]
        }
        [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 8 -Compress),[Text.UTF8Encoding]::new($false))
        Write-Output 'Exact existing response reconciled; no new data query';exit
    }
    # A failed previous table can disable its company/date widgets while the
    # normal Type/SmartID/Data selectors remain usable. Prepare the new source
    # normally; certify its query dimensions only AFTER that binding resolves.
    if($requestDoc.action -ne 'inspect'){Verify-SourceSelectors}
    # Normal acquisition uses the supported editable query mode. The vendor
    # disables company/date controls when New Table is unchecked, while its
    # stale lists remain readable; those lists cannot authorize date input.
    # Existing-response recovery above must preserve its original UI state.
    $queryEditModeChanged=$false
    if($requestDoc.action -in @('plan','download')) {
        $modeHandle=Control $root 'New Table' '*BUTTON*'
        $modeWasSelected=(([TejBridgeNative]::AccessibleState($modeHandle))[1] -band 16) -ne 0
        [void](Set-Checkbox $root 'New Table' $true)
        $queryEditModeChanged=-not $modeWasSelected
    }
    $bindingAlreadyMatches=$true
    foreach($pair in @(@('Type',$requestDoc.type),@('SmartID',$requestDoc.smart_id),@('Data',$requestDoc.table))) {
        $h=Control $source $pair[0] '*COMBOBOX*';$labels=Items $h $true
        if($labels[(Message $h 0x147)] -cne $pair[1]){$bindingAlreadyMatches=$false}
    }
    foreach($pair in @(@('Type',$requestDoc.type),@('SmartID',$requestDoc.smart_id),@('Data',$requestDoc.table))) {
        $h=Control $source $pair[0] '*COMBOBOX*'
        if($requestDoc.action -in @('recover_plan','resume_plan_empty_fields')) {
            $labels=Items $h $true
            if($labels[(Message $h 0x147)] -cne $pair[1]){throw 'Plan recovery source binding changed; no repeated selection'}
        } else {Select-Combo $h $pair[1]}
    }
    $sourceKeyMode=Read-SourceKeyMode
    if($requestDoc.action -eq 'download') {
        $plannedKeyMode=$(if($requestDoc.source_key_mode){[int]$requestDoc.source_key_mode}else{2})
        if($sourceKeyMode -ne $plannedKeyMode -or ($sourceKeyMode -eq 1 -and
           ($requestDoc.key_layout_contract -cne 'native_company_observed_snapshot_key1_v1' -or $requestDoc.fields.Count+1 -gt 30 -or $requestDoc.date_labels.Count -ne 0)) -or ($sourceKeyMode -eq 3 -and
           ($requestDoc.key_layout_contract -cne 'native_company_period_record_key3_v1' -or $requestDoc.fields.Count+3 -gt 30))){
            throw 'SOURCE_KEY_LAYOUT_BEFORE_QUERY: actual source format differs; no Preview sent'
        }
    }
    if($requestDoc.action -ne 'inspect'){[void](Verify-EditableScope)}
    $fieldLists=Lists $source
    if($fieldLists.Count -ne 2){throw 'Wrong field list layout'}
    $schemaDeadline=[DateTime]::UtcNow.AddSeconds(150)
    do {
        try{$fieldMenu=Items $fieldLists[0].Current.NativeWindowHandle}catch{if([DateTime]::UtcNow -ge $schemaDeadline){throw};continue}
        if(($fieldMenu -join "`t") -ceq (@($requestDoc.catalog_fields) -join "`t")){break}
        Start-Sleep -Milliseconds 250
    }while([DateTime]::UtcNow -lt $schemaDeadline)
    if(($fieldMenu -join "`t") -cne (@($requestDoc.catalog_fields) -join "`t")){throw 'Source schema did not resolve to catalog; no query'}
    $selectionAlreadyMatches=$bindingAlreadyMatches -and
        ((Items $fieldLists[1].Current.NativeWindowHandle) -join "`t") -ceq (@($requestDoc.fields) -join "`t")
    if($requestDoc.action -eq 'resume_plan_empty_fields') {
        if((Items $fieldLists[1].Current.NativeWindowHandle).Count -ne 0){throw 'Operator empty-field recovery requires an actually empty selection'}
        if([TejBridgeNative]::Dialogs($ExpectedWindow).Count -ne 0){throw 'Vendor notice blocks empty-field recovery'}
        Select-FieldList 'Select All' $fieldLists[1].Current.NativeWindowHandle $fieldMenu.Count
    } elseif($requestDoc.action -ne 'recover_plan' -and -not $selectionAlreadyMatches) {
        Select-FieldList 'Clear All' $fieldLists[1].Current.NativeWindowHandle 0
        if($requestDoc.fields.Count -eq $fieldMenu.Count -and (@($requestDoc.fields) -join "`t") -ceq ($fieldMenu -join "`t")) {
            Select-FieldList 'Select All' $fieldLists[1].Current.NativeWindowHandle $fieldMenu.Count
        } else {
            $selectedCount=0
            foreach($field in $requestDoc.fields) {
                $index=[Array]::IndexOf($fieldMenu,$field);if($index -lt 0){throw 'Field disappeared'}
                [TejBridgeNative]::SelectListItem($ExpectedWindow,$fieldLists[0].Current.NativeWindowHandle,$index,$field)
                Click-Button $source 'Select'
                $selectedCount++
                [TejBridgeNative]::WaitSelectedList($ExpectedWindow,$fieldLists[1].Current.NativeWindowHandle,$selectedCount,$field)
            }
        }
    }
    $selectedFields=Items $fieldLists[1].Current.NativeWindowHandle
    if(($selectedFields -join "`t") -cne (@($requestDoc.fields) -join "`t")){throw 'LOCAL_SELECTION_BEFORE_QUERY: selected fields differ; no query'}
    $selectedFieldOrderVerified=$true
    $company=Find-QueryGroup 'Company Setting';$companyLists=Lists $company
    if($companyLists.Count -ne 4){throw 'Unreviewed company list layout'}
    $sectorCombo=Sector-Control $company
    $sectorFilterApplicable=$sectorCombo -ne 0
    $sectorLabels=@();if($sectorFilterApplicable){$sectorLabels=Items $sectorCombo $true}
    if($requestDoc.action -eq 'inspect') {
        $availableCompanies=Items $companyLists[2].Current.NativeWindowHandle
        $universeRefresh=$false
        if($requestDoc.refresh_company_universe_once -eq $true) {
            if(-not $sectorFilterApplicable -or 'ALL' -cnotin $sectorLabels){throw 'No source-bound ALL selector for requested metadata refresh'}
            [void](Set-Checkbox $company 'Set Industry' $false)
            Select-Combo $sectorCombo 'ALL' $true
            $universeRefresh=$true
            # The queued selection notification may still be refreshing the
            # company menu. This is one metadata action, never a query retry.
            Start-Sleep -Milliseconds 500
            $sectorLabels=Items $sectorCombo $true
            $availableCompanies=Items $companyLists[2].Current.NativeWindowHandle
        }
        $beforeGeometry=[TejBridgeNative]::Geometry($ExpectedWindow)
        [TejBridgeNative]::Activate($ExpectedWindow)
        $payload=@{contract_version=4;action='inspect';company_sectors=@($sectorLabels);company_count=$availableCompanies.Count;
            company_universe_refresh_performed=$universeRefresh;
            selected_company_count=(Items $companyLists[3].Current.NativeWindowHandle).Count;
            company_lists=@($companyLists|ForEach-Object {
                $labels=Items $_.Current.NativeWindowHandle
                @{count=$labels.Count;first=@($labels|Select-Object -First 3);enabled=$_.Current.IsEnabled}
            });
            company_group_enabled=$company.Current.IsEnabled;
            date_group_enabled=(Find-QueryGroup 'Date Setting').Current.IsEnabled;
            date_buttons=@(Native-Controls (Find-QueryGroup 'Date Setting') 'WindowsForms10.BUTTON.'|ForEach-Object {
                @{name=$_.Current.Name;enabled=$_.Current.IsEnabled}
            });
            before_activation_geometry=$beforeGeometry;
            root_geometry=[TejBridgeNative]::Geometry($ExpectedWindow);
            date_edits=@(Native-Controls (Find-QueryGroup 'Date Setting') 'WindowsForms10.EDIT.'|ForEach-Object {
                @{handle=$_.Current.NativeWindowHandle;name=$_.Current.Name;geometry=[TejBridgeNative]::Geometry($_.Current.NativeWindowHandle);
                  enabled=$_.Current.IsEnabled;offscreen=$_.Current.IsOffscreen;
                  native_style=[TejBridgeNative]::GetWindowLong([IntPtr]$_.Current.NativeWindowHandle,-16)}
            });date_lists=@((Lists (Find-QueryGroup 'Date Setting'))|ForEach-Object {
                $labels=Items $_.Current.NativeWindowHandle
                @{handle=$_.Current.NativeWindowHandle;count=$labels.Count;first=@($labels|Select-Object -First 3);last=@($labels|Select-Object -Last 3)}
            });advanced_controls=@(Native-Controls $root 'WindowsForms10.BUTTON.'|Where-Object {
                $_.Current.Name -like '*Date Option*'
            }|ForEach-Object {
                @{name=$_.Current.Name;handle=$_.Current.NativeWindowHandle;
                  style=([TejBridgeNative]::GetWindowLong([IntPtr]$_.Current.NativeWindowHandle,-16) -band 15);
                  control_type=$_.Current.ControlType.ProgrammaticName;
                  accessible_state=[TejBridgeNative]::AccessibleState($_.Current.NativeWindowHandle);
                  check=(Message $_.Current.NativeWindowHandle 0xF0)}
            });checkboxes=@(Native-Controls $root 'WindowsForms10.BUTTON.'|Where-Object {
                ([TejBridgeNative]::AccessibleState($_.Current.NativeWindowHandle))[0] -eq 44
            }|ForEach-Object {
                @{name=$_.Current.Name;handle=$_.Current.NativeWindowHandle;
                  accessible_state=[TejBridgeNative]::AccessibleState($_.Current.NativeWindowHandle)}
            });credentials_read=$false}
        [IO.File]::WriteAllText($Output,($payload|ConvertTo-Json -Depth 4 -Compress),[Text.UTF8Encoding]::new($false))
        Write-Output 'Scoped controls inspected; no market-data request';exit
    }
    if($sectorFilterApplicable) {
        [void](Set-Checkbox $company 'Set Industry' $false)
        if('ALL' -cnotin $sectorLabels){throw 'All-sector company universe unavailable; no partial-market attribution'}
        # Source Type/schema can update while the companies still belong to
        # the previous table. The visible ALL text alone is not a fresh menu.
        # Reapply its existing metadata notification once after a binding change.
        $refreshUniverse=$queryEditModeChanged -or (-not $bindingAlreadyMatches) -or
            $requestDoc.refresh_company_universe_once -eq $true
        Select-Combo $sectorCombo 'ALL' $refreshUniverse
        if($refreshUniverse){Start-Sleep -Milliseconds 500}
    }
    $numericScale=Set-NumericScaleDefault
    [void](Verify-EditableScope)
    $availableCompanyCount=Message $companyLists[2].Current.NativeWindowHandle 0x18B
    $companyIndices=@{}
    if($requestDoc.action -eq 'download') {
        $availableCompanies=@()
        foreach($label in $requestDoc.company_labels) {
            if($companyIndices.ContainsKey($label)){throw 'LOCAL_SELECTION_BEFORE_QUERY: duplicate requested company'}
            $companyIndices[$label]=[TejBridgeNative]::ExactListIndex($ExpectedWindow,$companyLists[2].Current.NativeWindowHandle,$label)
        }
    } else {$availableCompanies=Items $companyLists[2].Current.NativeWindowHandle}
    $availableDates=@();$selectedDates=@();$dateLists=@()
    if($sourceKeyMode -ne 1) {
    $dates=Find-QueryGroup 'Date Setting'
    if(-not $company.Current.IsEnabled -or -not $dates.Current.IsEnabled) {
        throw 'DATE_INPUT_BEFORE_QUERY: supported query edit mode has not enabled the company/date controls'
    }
    $edits=@(Native-Controls $dates 'WindowsForms10.EDIT.'|Sort-Object {$_.Current.BoundingRectangle.X})
    if($edits.Count -notin @(2,3)){throw 'DATE_INPUT_BEFORE_QUERY: unreviewed date edit layout'}
    # The third EDIT is a movable yyyyMMdd mask shared by both boxes. Exclude
    # it by the stable outer date format BEFORE either input changes its HWND
    # geometry. Choosing the last X coordinate can otherwise edit start twice.
    $edits=@($edits|Where-Object {[TejBridgeNative]::Text($_.Current.NativeWindowHandle) -cmatch '^(\d{4}/\d{2}/\d{2}|____/__/__)$'})
    if($edits.Count -ne 2){throw 'DATE_INPUT_BEFORE_QUERY: stable outer date boxes unavailable; no date input'}
    $startEdit=$edits[0].Current.NativeWindowHandle;$endEdit=$edits[-1].Current.NativeWindowHandle
    try {
        [TejBridgeNative]::DateText($ExpectedWindow,$startEdit,$dates.Current.NativeWindowHandle,($requestDoc.start.Replace('-','')))
        [TejBridgeNative]::DateText($ExpectedWindow,$endEdit,$dates.Current.NativeWindowHandle,($requestDoc.end.Replace('-','')))
    } catch {
        # All failures in these two guarded edits occur before Preview. In
        # particular Activate can refuse foreground ownership before the
        # narrower hit-test error. Do not bypass focus or type into another
        # application; attest the pre-query boundary for bounded local retry.
        throw ('DATE_INPUT_BEFORE_QUERY: '+$_.Exception.Message)
    }
    $frequency=@{daily='Daily';weekly='Weekly';monthly='Monthly';quarterly='Quarterly';yearly='Yearly'}[$requestDoc.frequency]
    if(-not $frequency){throw 'Unknown date frequency'};Click-Button $dates $frequency
    $frequencySelectionReadbackContract=$null
    if($requestDoc.source_period_key_contract -cin @('literal_source_yyyymm_period_key_v1','literal_source_data_yymm_period_key_v1')) {
        if($requestDoc.frequency -cne 'monthly'){throw 'Reviewed month-period keys require the Monthly grid'}
        $frequencyHandle=Control $dates $frequency '*BUTTON*'
        $frequencyState=[TejBridgeNative]::AccessibleState($frequencyHandle)
        if($frequencyState[0] -ne 45 -or ($frequencyState[1] -band 16) -eq 0 -or ($frequencyState[1] -band 33) -ne 0) {
            throw 'Owned Monthly radio selection readback differs; no query submitted'
        }
        $frequencySelectionReadbackContract='owned_checked_frequency_button_v1'
    }
    [void](Set-Checkbox $dates 'Last Period' $false)
    [void](Set-Checkbox $dates 'Calendar Date' $false)
    Click-Button $dates 'Clear All';Click-Button $dates 'Search'
    [void](Verify-EditableScope)
    $dateLists=Lists $dates;if($dateLists.Count -ne 2){throw 'Wrong date list layout'}
    $availableDateCount=Message $dateLists[0].Current.NativeWindowHandle 0x18B
    $dateIndices=@{}
    if($requestDoc.action -eq 'download') {
        foreach($label in $requestDoc.date_labels) {
            if($dateIndices.ContainsKey($label)){throw 'LOCAL_SELECTION_BEFORE_QUERY: duplicate requested date'}
            $dateIndices[$label]=[TejBridgeNative]::ExactListIndex($ExpectedWindow,$dateLists[0].Current.NativeWindowHandle,$label)
        }
    } else {$availableDates=Items $dateLists[0].Current.NativeWindowHandle}
    }
    if($requestDoc.action -in @('plan','recover_plan','resume_plan_empty_fields')) {
        $sourceScopeProof=Verify-EditableScope -BindingReadback
        $payload=@{contract_version=4;provider='tej_smart_wizard';action='plan';observed_at_utc=[DateTime]::UtcNow.ToString('o');
            task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            fields=@($selectedFields);company_labels=@($availableCompanies);date_labels=@($availableDates);
            universe_scope='exact_type_smart_id_all_sectors';calendar_date_mode=$(if($sourceKeyMode -eq 1){$null}else{$false});
            checkbox_verification_method='msaa_role44_state_flags';
            vendor_numeric_scale_selection=$numericScale;
            sector_filter_applicable=$sectorFilterApplicable;
            source_scope_proof=$sourceScopeProof;
            company_universe_reapplied_after_binding_change=($sectorFilterApplicable -and -not $bindingAlreadyMatches);
            date_axis=$(if($sourceKeyMode -eq 1){'observed_source_snapshot_no_historical_date_claim'}else{'requested_smart_wizard_grid_not_native_observation_dates'});provider_http_requests=$null;credentials_read=$false}
        if($sourceKeyMode -eq 1){$payload.source_key_mode=1;$payload.key_layout_contract='native_company_observed_snapshot_key1_v1'}
    } else {
        if($requestDoc.company_labels.Count -lt 1 -or ($sourceKeyMode -ne 1 -and $requestDoc.date_labels.Count -lt 1)){throw 'Empty query grid'}
        $expectedRows=$requestDoc.company_labels.Count*$(if($sourceKeyMode -eq 1){1}else{$requestDoc.date_labels.Count})
        if($expectedRows -gt $requestDoc.max_rows -or ($expectedRows+1)*($requestDoc.fields.Count+$sourceKeyMode) -gt $requestDoc.max_cells){throw 'Query exceeds reviewed memory/cell bound'}
        if($sourceKeyMode -ne 1){[TejBridgeNative]::WaitSelectedList($ExpectedWindow,$dateLists[1].Current.NativeWindowHandle,0,$null)}
        Click-Button $company 'Clear All'
        [TejBridgeNative]::WaitSelectedList($ExpectedWindow,$companyLists[3].Current.NativeWindowHandle,0,$null)
        if($requestDoc.company_labels.Count -eq $availableCompanyCount) {
            Click-Button $company 'Select All'
            [TejBridgeNative]::WaitSelectedList($ExpectedWindow,$companyLists[3].Current.NativeWindowHandle,$availableCompanyCount,$null)
        } else {
            $selectedCount=0
            foreach($label in $requestDoc.company_labels) {
                $index=$companyIndices[$label]
                [TejBridgeNative]::SelectListItem($ExpectedWindow,$companyLists[2].Current.NativeWindowHandle,$index,$label)
                Select-Company
                $selectedCount++
                [TejBridgeNative]::WaitSelectedList($ExpectedWindow,$companyLists[3].Current.NativeWindowHandle,$selectedCount,$label)
            }
        }
        $selectedCompanies=Items $companyLists[3].Current.NativeWindowHandle
        if(-not [TejBridgeNative]::SameItems([string[]]$selectedCompanies,[string[]]$requestDoc.company_labels)){throw 'Company selection mismatch'}
        if($sourceKeyMode -ne 1) {
        if($requestDoc.date_labels.Count -eq $availableDateCount) {
            Click-Button $dates 'Select All'
            [TejBridgeNative]::WaitSelectedList($ExpectedWindow,$dateLists[1].Current.NativeWindowHandle,$availableDateCount,$null)
        } else {
            $selectedCount=0
            foreach($label in $requestDoc.date_labels) {
                $index=$dateIndices[$label]
                [TejBridgeNative]::SelectListItem($ExpectedWindow,$dateLists[0].Current.NativeWindowHandle,$index,$label)
                Click-Button $dates 'Select'
                $selectedCount++
                [TejBridgeNative]::WaitSelectedList($ExpectedWindow,$dateLists[1].Current.NativeWindowHandle,$selectedCount,$label)
            }
        }
        $selectedDates=Items $dateLists[1].Current.NativeWindowHandle
        if(-not [TejBridgeNative]::SameItems([string[]]$selectedDates,[string[]]$requestDoc.date_labels)){throw 'Date selection mismatch'}
        }
        # Avoid the advanced settings/Excel-range modes. Source values only.
        $main=$root
        if($sourceKeyMode -ne 1){[void](Set-Checkbox $main 'Adv. Date Option' $false)}
        # Persist the fully verified selections before the single action.
        $previewButton=Control $main 'Preview' '*BUTTON*'
        try {
            # MSAA invokes this exact owned button normally; foreground is not
            # the target's identity. No global activation or keyboard fallback.
            Assert-Scope $previewButton
            if(-not [TejBridgeNative]::IsWindowEnabled([IntPtr]$previewButton) -or
               -not [TejBridgeNative]::IsWindowVisible([IntPtr]$previewButton)) {throw 'Owned Preview is not usable'}
            [TejBridgeNative]::VerifyPreviewAction($ExpectedWindow,$previewButton)
            $beforePreviewSignatures=@(foreach($handle in [TejBridgeNative]::Children($ExpectedWindow,'WindowsForms10.Window.')) {
                $candidate=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
                if($candidate.Current.Name -ceq 'DataGridView'){Assert-Scope $handle;[TejBridgeNative]::PreviewSignature($handle)}
            })
        } catch {throw ('QUERY_ACTIVATION_BEFORE_QUERY: '+$_.Exception.Message)}
        $sourceScopeProof=Verify-EditableScope -BindingReadback
        $stage=@{contract_version=4;task_id=$requestDoc.task_id;stage='prepreview_verified';
                 observed_at_utc=[DateTime]::UtcNow.ToString('o');fields=@($selectedFields);
                 company_labels=@($selectedCompanies);date_labels=@($selectedDates);type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
                 calendar_date_mode=$(if($sourceKeyMode -eq 1){$null}else{$false});checkbox_verification_method='msaa_role44_state_flags'}
        $stage.vendor_numeric_scale_selection=$numericScale
        $stage.source_scope_proof=$sourceScopeProof
        $stage.preview_submission_contract=[TejBridgeNative]::SubmissionContract
        $stage.query_attempt_id=$requestDoc.query_attempt_id
        $stage.before_preview_signatures=@($beforePreviewSignatures)
        $stage.desktop_input_contract=[TejBridgeNative]::InputContract
        $stage.source_key_mode=$sourceKeyMode
        $stage.key_layout_contract=$requestDoc.key_layout_contract
        $stage.preview_owned_control_verified=$true
        $stage.preview_foreground_required=$false
        $stage.date_input_requires_foreground=($sourceKeyMode -ne 1)
        $stage.global_keyboard_input_sent=($sourceKeyMode -ne 1)
        [IO.File]::WriteAllText(($Output+'.stage.json'),($stage|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
        # Mark BEFORE submission: a crash during the default action remains
        # unknown, never be classified as safe to submit again.
        $querySubmissionPossible=$true
        [TejBridgeNative]::BeginPreviewDefaultAction($ExpectedWindow,$previewButton)
        $startedPolling=[DateTime]::UtcNow;$deadline=$startedPolling.AddSeconds(150);$grids=@();$freshPreviewTransitionVerified=$false
        do {
            $elapsed=([DateTime]::UtcNow-$startedPolling).TotalSeconds
            Start-Sleep -Milliseconds $(if($elapsed -lt 2){250}else{1000})
            if([TejBridgeNative]::PreviewActionState() -eq -1){throw 'Single Preview default action failed; outcome unknown; no fallback'}
            $empty=Empty-Response
            if($null -ne $empty) {
                [IO.File]::WriteAllText($Output,($empty|ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
                Write-Output 'Exact provider empty response saved; no data invented';exit
            }
            $grids=@(Matching-Preview)
            if($grids.Count -eq 1) {
                $signature=[TejBridgeNative]::PreviewSignature($grids[0].Current.NativeWindowHandle)
                $freshPreviewTransitionVerified=$signature -cnotin $beforePreviewSignatures
            }
        } while(($grids.Count -eq 0 -or -not $freshPreviewTransitionVerified) -and [DateTime]::UtcNow -lt $deadline)
        if($grids.Count -ne 1){throw 'No unambiguous vendor Preview; no export'}
        if(-not $freshPreviewTransitionVerified){throw 'No proved fresh result transition; no stale result adopted'}
        $payload=Capture-Preview $grids[0]
    }
    if($requestDoc.action -in @('plan','recover_plan','resume_plan_empty_fields')){$payload.source_key_mode=$sourceKeyMode}
    if($requestDoc.action -in @('plan','recover_plan','resume_plan_empty_fields')) {
        $payload.frequency=$requestDoc.frequency
        $payload.frequency_selection_readback_contract=$frequencySelectionReadbackContract
    }
    if($payload.action -ceq 'download' -and $payload.capture_method -ceq 'native_msaa_preview_full' -and
       $payload.source_key_headers.Count -ge 2 -and $payload.source_key_headers[1] -cin @('YYYYMM','Data YYMM') -and
       @($requestDoc.date_labels|Where-Object {$_ -cnotmatch '^(\d{6}|\d{8}|\d{4}[-/]\d{2}([-/]\d{2})?)$'}).Count -eq 0) {
        $payload.source_period_key_contract=$(if($payload.source_key_headers[1] -ceq 'YYYYMM'){
            'literal_source_yyyymm_period_key_v1'}else{'literal_source_data_yymm_period_key_v1'})
    }
    $json=$payload|ConvertTo-Json -Depth 8 -Compress
    [IO.File]::WriteAllText($Output,$json,[Text.UTF8Encoding]::new($false))
    Write-Output ($payload.action+' succeeded; raw values written only to the private local output')
} catch {
    if($requestDoc.action -eq 'download' -and -not $querySubmissionPossible -and
       $_.Exception.ToString().Contains('SOURCE_KEY_LAYOUT_BEFORE_QUERY:')) {
        $outcome=@{contract_version=4;provider='tej_smart_wizard';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;action='download';
            market_data_query_submission_possible=$false;error_code='source_key_layout_failed_before_preview';
            source_key_mode=$sourceKeyMode;observed_at_utc=[DateTime]::UtcNow.ToString('o')}
        [IO.File]::WriteAllText(($Output+'.outcome.json'),($outcome|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
    }
    if($requestDoc.action -eq 'plan' -and -not $querySubmissionPossible) {
        # A metadata plan has no data Preview/Export path. Its failure can be
        # isolated only after the shared interface is independently verified.
        $outcome=@{contract_version=4;provider='tej_smart_wizard';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            action='plan';market_data_query_submission_possible=$false;
            error_code='metadata_preparation_failed_before_preview';observed_at_utc=[DateTime]::UtcNow.ToString('o')}
        [IO.File]::WriteAllText(($Output+'.outcome.json'),($outcome|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
    }
    if($requestDoc.action -eq 'download' -and -not $querySubmissionPossible -and
       $_.Exception.ToString().Contains('DESKTOP_CONTEXT_BEFORE_QUERY:')) {
        $outcome=@{contract_version=4;provider='tej_smart_wizard';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            action='download';market_data_query_submission_possible=$false;
            error_code='desktop_context_unavailable_before_preview';observed_at_utc=[DateTime]::UtcNow.ToString('o')}
        [IO.File]::WriteAllText(($Output+'.outcome.json'),($outcome|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
    }
    if($requestDoc.action -eq 'download' -and -not $querySubmissionPossible -and
       $_.Exception.ToString().Contains('DATE_INPUT_BEFORE_QUERY:')) {
        $outcome=@{contract_version=4;provider='tej_smart_wizard';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            action='download';market_data_query_submission_possible=$false;
            error_code=$(if($_.Exception.ToString().Contains('Foreground unavailable; no input sent')){'desktop_foreground_unavailable_before_preview'}else{'local_date_input_failed_before_preview'});observed_at_utc=[DateTime]::UtcNow.ToString('o')}
        [IO.File]::WriteAllText(($Output+'.outcome.json'),($outcome|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
    }
    if($requestDoc.action -eq 'download' -and -not $querySubmissionPossible -and
       $_.Exception.ToString().Contains('LOCAL_SELECTION_BEFORE_QUERY:') -and [TejBridgeNative]::Dialogs($ExpectedWindow).Count -eq 0) {
        $outcome=@{contract_version=4;provider='tej_smart_wizard';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            action='download';market_data_query_submission_possible=$false;
            error_code='local_list_selection_failed_before_preview';observed_at_utc=[DateTime]::UtcNow.ToString('o')}
        [IO.File]::WriteAllText(($Output+'.outcome.json'),($outcome|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
    }
    if($requestDoc.action -eq 'download' -and -not $querySubmissionPossible -and
       $_.Exception.ToString().Contains('QUERY_ACTIVATION_BEFORE_QUERY:') -and [TejBridgeNative]::Dialogs($ExpectedWindow).Count -eq 0) {
        $outcome=@{contract_version=4;provider='tej_smart_wizard';task_id=$requestDoc.task_id;
            type=$requestDoc.type;smart_id=$requestDoc.smart_id;table=$requestDoc.table;
            action='download';market_data_query_submission_possible=$false;
            error_code='local_query_activation_failed_before_preview';observed_at_utc=[DateTime]::UtcNow.ToString('o')}
        [IO.File]::WriteAllText(($Output+'.outcome.json'),($outcome|ConvertTo-Json -Depth 3 -Compress),[Text.UTF8Encoding]::new($false))
    }
    throw
} finally {
    foreach($comObject in @($range,$sheet,$book,$excel)) {
        if($null -ne $comObject -and [Runtime.InteropServices.Marshal]::IsComObject($comObject)){[void][Runtime.InteropServices.Marshal]::ReleaseComObject($comObject)}
    }
    if($owns){$mutex.ReleaseMutex()};$mutex.Dispose()
}
