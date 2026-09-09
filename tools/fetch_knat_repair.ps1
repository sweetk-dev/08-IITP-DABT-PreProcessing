# 중앙보조기기센터(국립재활원) 전국 보조기기 수리센터 명부 수집기
#   https://www.knat.go.kr/knw/home/knat/knat_repair.php 에 loc_idx(1~17) 로 POST 하면
#   시도별 목록 표가 돌아온다. 사이트 인코딩이 EUC-KR 이라 바이트로 받아 직접 디코딩한다.
#   원천에 주소/좌표/전화/운영시간이 없다 - 이 명부는 "전국 어디에 무엇이 있는가"만 준다.
param(
  [Parameter(Mandatory=$true)][string]$OutDir
)
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
$url = 'https://www.knat.go.kr/knw/home/knat/knat_repair.php'
$euckr = [System.Text.Encoding]::GetEncoding(51949)
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$rows = New-Object System.Collections.ArrayList
for ($i = 1; $i -le 17; $i++) {
  $body = "loc_idx=$i&kind_val="
  $resp = Invoke-WebRequest -Uri $url -Method Post -Body $body -ContentType 'application/x-www-form-urlencoded' -UseBasicParsing -TimeoutSec 90 -Headers @{ 'User-Agent' = $ua; 'Referer' = $url }
  $html = $euckr.GetString($resp.RawContentStream.ToArray())
  $trs = [regex]::Matches($html, '(?s)<tr>(.*?)</tr>')
  $n = 0
  foreach ($tr in $trs) {
    $seg = $tr.Groups[1].Value
    $region = [regex]::Match($seg, '(?s)<td class="region">(.*?)</td>')
    if (-not $region.Success) { continue }
    function Cell($cls) {
      $m = [regex]::Match($seg, '(?s)<td class="' + $cls + '"[^>]*>(.*?)</td>')
      if (-not $m.Success) { return '' }
      $v = $m.Groups[1].Value -replace '(?i)<br\s*/?>', ' / '
      $v = $v -replace '<[^>]+>', ''
      $v = [System.Net.WebUtility]::HtmlDecode($v)
      return ($v -replace '\s+', ' ').Trim()
    }
    $null = $rows.Add([pscustomobject]@{
      loc_idx    = $i
      sido       = Cell 'region'
      sigungu    = Cell 'city'
      oper_type  = Cell 'work'
      name       = Cell 'store'
      support    = Cell 'support'
    })
    $n++
  }
  Write-Output ("LOC {0} rows={1} acc={2}" -f $i, $n, $rows.Count)
  Start-Sleep -Milliseconds 500
}

$sb = New-Object System.Text.StringBuilder
$null = $sb.AppendLine('"시도명","시군구명","운영방식","업체명","지원기준"')
foreach ($r in $rows) {
  $line = @($r.sido, $r.sigungu, $r.oper_type, $r.name, $r.support) | ForEach-Object { '"' + ([string]$_ -replace '"', '""') + '"' }
  $null = $sb.AppendLine($line -join ',')
}
$csv = Join-Path $OutDir 'knat_repair_raw.csv'
[System.IO.File]::WriteAllText($csv, $sb.ToString(), (New-Object System.Text.UTF8Encoding($false)))
$json = Join-Path $OutDir 'knat_repair_raw.json'
[System.IO.File]::WriteAllText($json, ($rows | ConvertTo-Json -Depth 4 -Compress), (New-Object System.Text.UTF8Encoding($false)))
Write-Output ("DONE rows={0} csv={1}" -f $rows.Count, $csv)
