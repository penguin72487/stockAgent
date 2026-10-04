<# Read-only bounded MSAA path parity probe. No query, input, clipboard or raw-value output. #>
param([Parameter(Mandatory=$true)][long]$ExpectedWindow,
      [Parameter(Mandatory=$true)][int]$TejProcessId,
      [Parameter(Mandatory=$true)][string]$ExpectedTitle,
      [ValidateSet('values','controls')][string]$Mode='values')
$ErrorActionPreference='Stop'
Add-Type -AssemblyName Accessibility
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -TypeDefinition @'
using System;
using System.Text;
using System.Diagnostics;
using System.Collections.Generic;
using System.Runtime.InteropServices;
public static class TejMsaaReadProbe {
    public delegate bool Callback(IntPtr h,IntPtr s);
    [DllImport("user32.dll")] private static extern bool EnumChildWindows(IntPtr h,Callback f,IntPtr s);
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] private static extern int GetClassNameW(IntPtr h,StringBuilder b,int n);
    [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr h,out uint pid);
    [DllImport("user32.dll")] public static extern IntPtr GetParent(IntPtr h);
    [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr h);
    [DllImport("user32.dll",CharSet=CharSet.Unicode)] private static extern IntPtr SendMessageTimeoutW(IntPtr h,uint m,IntPtr w,IntPtr l,uint f,uint t,out IntPtr r);
    [DllImport("user32.dll",CharSet=CharSet.Unicode,EntryPoint="SendMessageTimeoutW")] private static extern IntPtr ReadText(IntPtr h,uint m,IntPtr w,StringBuilder l,uint f,uint t,out IntPtr r);
    [DllImport("oleacc.dll")] private static extern int AccessibleObjectFromWindow(IntPtr h,uint id,ref Guid g,[MarshalAs(UnmanagedType.Interface)] out Accessibility.IAccessible a);
    public static long[] Windows(long root,string prefix) {
        var result=new List<long>();bool exceeded=false;
        EnumChildWindows(new IntPtr(root),(h,s)=>{
            var b=new StringBuilder(256);GetClassNameW(h,b,256);
            if(b.ToString().StartsWith(prefix,StringComparison.Ordinal))result.Add(h.ToInt64());
            if(result.Count>512){exceeded=true;return false;}return true;
        },IntPtr.Zero);
        if(exceeded)throw new Exception("Native control bound exceeded");return result.ToArray();
    }
    public static Accessibility.IAccessible Open(long h) {
        Guid g=new Guid("618736E0-3C3D-11CF-810C-00AA00389B71");Accessibility.IAccessible a;
        Marshal.ThrowExceptionForHR(AccessibleObjectFromWindow(new IntPtr(h),0xFFFFFFFCu,ref g,out a));return a;
    }
    private static long Message(long h,uint m,long w) {
        IntPtr r;if(SendMessageTimeoutW(new IntPtr(h),m,new IntPtr(w),IntPtr.Zero,2,30000,out r)==IntPtr.Zero)throw new Exception("Read-only catalog probe timed out");return r.ToInt64();
    }
    public static string[] ComboItems(long h) {
        long n=Message(h,0x146,0);if(n<0||n>1000)throw new Exception("Combo probe bound exceeded");var labels=new string[n];
        for(int i=0;i<n;i++){long len=Message(h,0x149,i);if(len<0||len>4096)throw new Exception("Label bound exceeded");var b=new StringBuilder((int)len+1);IntPtr r;
            if(ReadText(new IntPtr(h),0x148,new IntPtr(i),b,2,10000,out r)==IntPtr.Zero)throw new Exception("Label probe timed out");labels[i]=b.ToString();}
        return labels;
    }
    public static string SelectedCombo(long h) {
        var items=ComboItems(h);long i=Message(h,0x147,0);
        return i<0||i>=items.Length?null:items[i];
    }
    public static long ListCount(long h) {
        long n=Message(h,0x18B,0);
        if(n<0||n>500000)throw new Exception("Read-only list-count bound exceeded");
        return n;
    }
    public static string[] ListSample(long h) {
        long n=ListCount(h);var indexes=new SortedSet<long>();
        for(long i=0;i<Math.Min(n,3);i++){indexes.Add(i);indexes.Add(n-i-1);}
        var result=new List<string>();
        foreach(long i in indexes){long len=Message(h,0x18A,i);
            if(len<0||len>4096)throw new Exception("Read-only list label bound exceeded");
            var b=new StringBuilder((int)len+1);IntPtr r;
            if(ReadText(new IntPtr(h),0x189,new IntPtr(i),b,2,10000,out r)==IntPtr.Zero)throw new Exception("List label probe timed out");
            result.Add(b.ToString());}
        return result.ToArray();
    }
    public static object[] Probe(long h) {
        var a=Open(h);int count=a.accChildCount;
        if(count<3||count>10004)throw new Exception("Unreviewed probe row bound");
        var row=a.get_accChild(2) as Accessibility.IAccessible;
        if(row==null||Convert.ToInt32(row.get_accRole(0))!=28)throw new Exception("No first source row");
        int columns=row.accChildCount;if(columns<4||columns>2002)throw new Exception("Unreviewed column bound");
        int tested=Math.Min(columns,10);var original=new string[tested];var direct=new string[tested];
        var clock=Stopwatch.StartNew();
        for(int c=1;c<=tested;c++) {
            var cell=row.get_accChild(c) as Accessibility.IAccessible;
            original[c-1]=cell!=null?cell.get_accValue(0):row.get_accValue(c);
        }
        double originalMs=clock.Elapsed.TotalMilliseconds;clock.Restart();
        try{for(int c=1;c<=tested;c++)direct[c-1]=row.get_accValue(c);}
        catch{return new object[]{false,"direct_child_value_unsupported",count,columns,tested,originalMs,clock.Elapsed.TotalMilliseconds};}
        int nonempty=0;bool equal=true;
        for(int i=0;i<tested;i++){equal&=String.Equals(original[i],direct[i],StringComparison.Ordinal);if(!String.IsNullOrEmpty(original[i]))nonempty++;}
        return new object[]{equal&&nonempty>=3,"bounded_first_row_path_parity",count,columns,tested,originalMs,clock.Elapsed.TotalMilliseconds};
    }
}
'@ -ReferencedAssemblies @([Accessibility.IAccessible].Assembly.Location)
$root=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$ExpectedWindow)
if($root.Current.ProcessId -ne $TejProcessId -or $root.Current.Name -cne $ExpectedTitle -or
   $ExpectedTitle -cnotmatch '^TEJ Smart Wizard \(Version 4\.1\.1\.7\) -- Book2$'){throw 'Wrong exact source scope'}
if($Mode -eq 'controls') {
    $controls=@(foreach($handle in [TejMsaaReadProbe]::Windows($ExpectedWindow,'WindowsForms10.COMBOBOX.')) {
        $node=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
        if($node.Current.ProcessId -ne $TejProcessId -or $node.Current.IsOffscreen){continue}
        $ancestors=@();$parent=[TejMsaaReadProbe]::GetParent([IntPtr]$handle)
        for($i=0;$i -lt 8 -and $parent.ToInt64() -ne 0;$i++) {
            $ancestor=[Windows.Automation.AutomationElement]::FromHandle($parent)
            if($ancestor.Current.ProcessId -ne $TejProcessId){break}
            $ancestors+=@{handle=$parent.ToInt64();name=$ancestor.Current.Name}
            if($parent.ToInt64() -eq $ExpectedWindow){break}
            $parent=[TejMsaaReadProbe]::GetParent($parent)
        }
        @{name=$node.Current.Name;automation_id=$node.Current.AutomationId;handle=$handle;
          x=$node.Current.BoundingRectangle.X;y=$node.Current.BoundingRectangle.Y;
          ancestors=$ancestors;native_visible=[TejMsaaReadProbe]::IsWindowVisible([IntPtr]$handle);
          selected=[TejMsaaReadProbe]::SelectedCombo($handle);
          sector_labels=$(if($node.Current.Name -ceq 'Sector'){[TejMsaaReadProbe]::ComboItems($handle)}else{$null})}
    })
    $lists=@(foreach($handle in [TejMsaaReadProbe]::Windows($ExpectedWindow,'WindowsForms10.LISTBOX.')) {
        $node=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
        if($node.Current.ProcessId -ne $TejProcessId){throw 'Foreign list owner'}
        $ancestors=@();$parent=[TejMsaaReadProbe]::GetParent([IntPtr]$handle)
        for($i=0;$i -lt 8 -and $parent.ToInt64() -ne 0;$i++) {
            $ancestor=[Windows.Automation.AutomationElement]::FromHandle($parent)
            if($ancestor.Current.ProcessId -ne $TejProcessId){break}
            $ancestors+=@{handle=$parent.ToInt64();name=$ancestor.Current.Name}
            if($parent.ToInt64() -eq $ExpectedWindow){break}
            $parent=[TejMsaaReadProbe]::GetParent($parent)
        }
        $directParent=[TejMsaaReadProbe]::GetParent([IntPtr]$handle)
        $staticNames=@(foreach($labelHandle in [TejMsaaReadProbe]::Windows($directParent.ToInt64(),'WindowsForms10.STATIC.')) {
            $label=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$labelHandle)
            if($label.Current.ProcessId -ne $TejProcessId){throw 'Foreign static owner'}
            $label.Current.Name
        })
        @{handle=$handle;name=$node.Current.Name;count=[TejMsaaReadProbe]::ListCount($handle);
          bounded_catalog_sample=[TejMsaaReadProbe]::ListSample($handle);parent_static_names=$staticNames;
          native_visible=[TejMsaaReadProbe]::IsWindowVisible([IntPtr]$handle);
          x=$node.Current.BoundingRectangle.X;y=$node.Current.BoundingRectangle.Y;
          width=$node.Current.BoundingRectangle.Width;height=$node.Current.BoundingRectangle.Height;
          ancestors=$ancestors}
    })
    @{controls=$controls;lists=$lists;data_query_sent=$false;raw_values_exposed=$false}|ConvertTo-Json -Depth 5 -Compress
    exit
}
$grids=@(foreach($handle in [TejMsaaReadProbe]::Windows($ExpectedWindow,'WindowsForms10.Window.')) {
    if(-not [TejMsaaReadProbe]::IsWindowVisible([IntPtr]$handle)){continue}
    $node=[Windows.Automation.AutomationElement]::FromHandle([IntPtr]$handle)
    if($node.Current.Name -ceq 'DataGridView' -and -not $node.Current.IsOffscreen){$node}
})
$result=@(foreach($grid in $grids) {
    $pidCheck=[uint32]0;[void][TejMsaaReadProbe]::GetWindowThreadProcessId([IntPtr]$grid.Current.NativeWindowHandle,[ref]$pidCheck)
    if($pidCheck -ne $TejProcessId){throw 'Foreign grid owner'}
    try{$p=[TejMsaaReadProbe]::Probe($grid.Current.NativeWindowHandle)}catch{continue}
    @{parity=$p[0];basis=$p[1];native_child_count=$p[2];columns=$p[3];tested_cells=$p[4];
      object_path_ms=$p[5];direct_path_ms=$p[6];raw_values_exposed=$false;data_query_sent=$false}
})
if($result.Count -ne 1){throw 'No unique bounded source grid probe'}
$result[0]|ConvertTo-Json -Compress
