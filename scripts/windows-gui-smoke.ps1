param(
  [Parameter(Mandatory = $true)]
  [string]$Executable,
  [string]$ScreenshotPath = "$env:RUNNER_TEMP\altai-gui-smoke.png"
)

$ErrorActionPreference = "Stop"

Add-Type @"
using System;
using System.Runtime.InteropServices;

public static class AltaiGuiSmokeNative {
  [StructLayout(LayoutKind.Sequential)]
  public struct RECT { public int Left; public int Top; public int Right; public int Bottom; }

  [DllImport("user32.dll")]
  public static extern bool GetWindowRect(IntPtr hWnd, out RECT rect);

  [DllImport("user32.dll")]
  public static extern bool IsWindowVisible(IntPtr hWnd);

  [DllImport("user32.dll", SetLastError = true)]
  public static extern bool PostMessage(IntPtr hWnd, uint message, IntPtr wParam, IntPtr lParam);
}
"@

$env:ALTAI_GUI_SMOKE = "1"
$configDirectory = Join-Path $env:APPDATA "dev.altai.app"
$legacyStatePath = Join-Path $configDirectory ".window-state.json"
$legacyStateBackup = if (Test-Path $legacyStatePath) { Get-Content $legacyStatePath -Raw } else { $null }
New-Item -ItemType Directory -Path $configDirectory -Force | Out-Null
@{
  main = @{
    width = 1280; height = 800; x = 0; y = 0; prev_x = 80; prev_y = 80
    maximized = $true; visible = $true; decorated = $false; fullscreen = $true
  }
} | ConvertTo-Json -Depth 3 | Set-Content -Path $legacyStatePath -Encoding UTF8

$process = $null
try {
  $process = Start-Process -FilePath $Executable -PassThru
  $deadline = [DateTime]::UtcNow.AddSeconds(60)
  $handle = [IntPtr]::Zero
  do {
    Start-Sleep -Milliseconds 250
    $process.Refresh()
    if ($process.HasExited) {
      throw "ALTAI exited before creating a renderer-ready window (exit $($process.ExitCode))."
    }
    $handle = $process.MainWindowHandle
  } while (($handle -eq [IntPtr]::Zero -or $process.MainWindowTitle -notlike "*renderer-ready*") -and [DateTime]::UtcNow -lt $deadline)

  if ($handle -eq [IntPtr]::Zero) {
    throw "ALTAI did not create a main window within 60 seconds."
  }
  if ($process.MainWindowTitle -notlike "*renderer-ready*") {
    throw "ALTAI created a window but the WebView2 renderer never reached the ready checkpoint."
  }
  if (-not [AltaiGuiSmokeNative]::IsWindowVisible($handle)) {
    throw "ALTAI's renderer-ready window is not visible."
  }

  $rect = New-Object AltaiGuiSmokeNative+RECT
  if (-not [AltaiGuiSmokeNative]::GetWindowRect($handle, [ref]$rect)) {
    throw "Could not read the ALTAI window bounds."
  }
  $width = $rect.Right - $rect.Left
  $height = $rect.Bottom - $rect.Top
  if ($width -lt 420 -or $height -lt 280) {
    throw "ALTAI window has invalid bounds: ${width}x${height}."
  }

  Add-Type -AssemblyName System.Drawing
  Start-Sleep -Milliseconds 750
  $bitmap = New-Object System.Drawing.Bitmap($width, $height)
  $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
  try {
    $graphics.CopyFromScreen($rect.Left, $rect.Top, 0, 0, $bitmap.Size)
    $bitmap.Save($ScreenshotPath, [System.Drawing.Imaging.ImageFormat]::Png)
    # Sample the painted shell, including app-owned Windows chrome. A blank
    # WebView2 compositor is essentially one color. The no-project Home empty
    # state is a sparse light canvas under a light OS theme, so skipping the
    # top 72px only sampled that canvas (3 buckets) while tabs/labels sat in
    # the chrome. Keep this grid in sync with `src/lib/guiSmokeColorBuckets.ts`.
    $colors = New-Object 'System.Collections.Generic.HashSet[string]'
    for ($x = 16; $x -lt $width - 16; $x += 16) {
      for ($y = 16; $y -lt $height - 16; $y += 16) {
        $pixel = $bitmap.GetPixel($x, $y)
        $bucket = '{0},{1},{2}' -f ([Math]::Floor($pixel.R / 8)), ([Math]::Floor($pixel.G / 8)), ([Math]::Floor($pixel.B / 8))
        [void]$colors.Add($bucket)
      }
    }
    if ($colors.Count -lt 6) {
      throw "WebView2 did not paint the real ALTAI shell; viewport contained only $($colors.Count) sampled color buckets."
    }
  } finally {
    $graphics.Dispose()
    $bitmap.Dispose()
  }

  # WM_CLOSE exercises the native close path independently of the renderer.
  if (-not [AltaiGuiSmokeNative]::PostMessage($handle, 0x0010, [IntPtr]::Zero, [IntPtr]::Zero)) {
    throw "Failed to post WM_CLOSE to the ALTAI window."
  }
  if (-not $process.WaitForExit(15000)) {
    throw "ALTAI did not exit within 15 seconds after WM_CLOSE."
  }
} finally {
  Remove-Item Env:ALTAI_GUI_SMOKE -ErrorAction SilentlyContinue
  if ($null -ne $process -and -not $process.HasExited) {
    Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
  }
  if ($null -ne $legacyStateBackup) {
    Set-Content -Path $legacyStatePath -Value $legacyStateBackup -Encoding UTF8
  } else {
    Remove-Item $legacyStatePath -Force -ErrorAction SilentlyContinue
  }
}
