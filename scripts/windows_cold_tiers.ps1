# Exact immutable CAS files only. Input is local reviewed control, never a script.
$ErrorActionPreference='Stop'
[Diagnostics.Process]::GetCurrentProcess().PriorityClass='Idle'
$request=[Console]::In.ReadToEnd() | ConvertFrom-Json
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class ColdTierAllocation {
 [DllImport("Wofutil.dll", CharSet=CharSet.Unicode)]
 private static extern int WofIsExternalFile(string path, [MarshalAs(UnmanagedType.Bool)] out bool external,
   out uint provider, IntPtr info, ref uint length);
 public static string Codec(string path) {
   bool external; uint provider; uint length=64;
   IntPtr info=Marshal.AllocHGlobal((int)length);
   try {
     int status=WofIsExternalFile(path,out external,out provider,info,ref length);
     if(status<0) Marshal.ThrowExceptionForHR(status);
     if(!external) return "none";
     if(provider!=2 || length>64 || length<8) throw new InvalidOperationException("Unknown external backing");
     switch(Marshal.ReadInt32(info)) {
       case 0: return "XPRESS4K";
       case 1: return "LZX";
       case 2: return "XPRESS8K";
       case 3: return "XPRESS16K";
       default: throw new InvalidOperationException("Unknown file compression");
     }
   } finally { Marshal.FreeHGlobal(info); }
 }
 [DllImport("kernel32.dll", CharSet=CharSet.Unicode, SetLastError=true)]
 public static extern uint GetCompressedFileSizeW(string name, out uint high);
 public static long Bytes(string name) {
   uint high; uint low=GetCompressedFileSizeW(name,out high);
   if(low==0xffffffff && Marshal.GetLastWin32Error()!=0) throw new System.ComponentModel.Win32Exception();
   return ((long)high<<32)|low;
 }
}
'@
$results=@()
foreach($row in $request.objects){
  if($row.path -notmatch '^D:\\stockagent-cold-primary\\packed\\(?:\.local-state\\private-history\\)?objects\\blobs\\([0-9a-f]{2})\\([0-9a-f]{64})\.blob$'){
    throw 'Noncanonical cold object'
  }
  if($Matches[2] -ne $row.sha256 -or $Matches[1] -ne $row.sha256.Substring(0,2)){
    throw 'Cold object path identity differs'
  }
  $source=Get-Item -LiteralPath $row.path -Force
  if($source.PSIsContainer -or $source.Length -ne $row.bytes){throw 'Cold object size differs'}
  $mtime=$source.LastWriteTimeUtc.Ticks
  $before=[ColdTierAllocation]::Bytes($row.path)
  $previous=[ColdTierAllocation]::Codec($row.path)
  $sha=(Get-FileHash -LiteralPath $row.path -Algorithm SHA256).Hash.ToLowerInvariant()
  if($sha -ne $row.sha256){throw 'Original CAS SHA differs'}
  $timer=[Diagnostics.Stopwatch]::StartNew()
  if($previous -ne 'LZX'){
    $details=& compact.exe /C /F /A /Q /EXE:LZX $row.path 2>&1
    if($LASTEXITCODE -ne 0){throw 'Transparent compression rejected'}
  }
  $encoded=[ColdTierAllocation]::Bytes($row.path)
  $keep=($previous -eq 'LZX' -or ($before -gt 0 -and ($before-$encoded)/[double]$before -ge $request.minimum_saving_ratio))
  if(-not $keep){
    if($previous -eq 'none'){
      $details=& compact.exe /U /A /Q /EXE $row.path 2>&1
    }else{
      $details=& compact.exe /C /F /A /Q "/EXE:$previous" $row.path 2>&1
    }
    if($LASTEXITCODE -ne 0){throw 'Nonbeneficial compression rollback rejected'}
  }
  $sha=(Get-FileHash -LiteralPath $row.path -Algorithm SHA256).Hash.ToLowerInvariant()
  $after=Get-Item -LiteralPath $row.path -Force
  if($sha -ne $row.sha256 -or $after.Length -ne $row.bytes -or $after.LastWriteTimeUtc.Ticks -ne $mtime){
    throw 'Original bytes or source clock changed'
  }
  $timer.Stop()
  $results+=@{sha256=$row.sha256;bytes=[long]$row.bytes;before_allocated_bytes=$before;
    after_allocated_bytes=[ColdTierAllocation]::Bytes($row.path);codec=[ColdTierAllocation]::Codec($row.path);
    previous_codec=$previous;already_stable=($previous -eq 'LZX');
    native_full_sha_verified=$true;seconds=$timer.Elapsed.TotalSeconds}
}
@{results=$results} | ConvertTo-Json -Compress -Depth 4
