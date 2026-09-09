# 경기데이터드림 "테마 맞춤형 데이터" 시트 수집기
#   포털 화면은 JS 렌더링이지만, 시트 데이터는 아래 JSON 엔드포인트가 그대로 준다.
#     POST /portal/data/sheet/searchSheetData.do?page=N   (_csrf, infId, infSeq, rows)
#   상세 페이지를 먼저 GET 해서 세션 쿠키와 _csrf 토큰을 얻어야 한다.
#   인증키가 필요 없고 로그인도 필요 없다.
param(
  [Parameter(Mandatory=$true)][string]$InfId,
  [Parameter(Mandatory=$true)][string]$OutPrefix,
  [string]$CateId = 'T105',
  [int]$InfSeq = 1,
  [int]$Rows = 1000
)
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
$page = "https://data.gg.go.kr/portal/adjust/selectThemeServicePage.do?infId=$InfId&cateId=$CateId&infSeq=$InfSeq"
$api  = 'https://data.gg.go.kr/portal/data/sheet/searchSheetData.do'

$r = Invoke-WebRequest -Uri $page -SessionVariable S -UseBasicParsing -TimeoutSec 90 -Headers @{ 'User-Agent' = $ua }
$m = [regex]::Match($r.Content, 'name="_csrf"[^>]*value="([^"]+)"')
if (-not $m.Success) { $m = [regex]::Match($r.Content, 'value="([0-9a-f-]{36})"[^>]*name="_csrf"') }
if (-not $m.Success) { throw 'CSRF token not found' }
$csrf = $m.Groups[1].Value

$all = New-Object System.Collections.ArrayList
$pageNo = 1
while ($true) {
  $body = @{ '_csrf' = $csrf; 'CSRFToken' = $csrf; 'infId' = $InfId; 'infSeq' = $InfSeq; 'rows' = $Rows }
  $resp = Invoke-WebRequest -Uri ($api + '?page=' + $pageNo) -Method Post -Body $body -WebSession $S -UseBasicParsing -TimeoutSec 180 -Headers @{ 'User-Agent' = $ua; 'Referer' = $page; 'X-Requested-With' = 'XMLHttpRequest' }
  $j = [Text.Encoding]::UTF8.GetString($resp.RawContentStream.ToArray()) | ConvertFrom-Json
  foreach ($d in $j.data) { $null = $all.Add($d) }
  Write-Output ("PAGE {0}/{1} count={2} acc={3}" -f $j.page, $j.pages, $j.count, $all.Count)
  if ($j.page -ge $j.pages) { break }
  $pageNo++
  Start-Sleep -Milliseconds 400
}

$jsonPath = "$OutPrefix.json"
[System.IO.File]::WriteAllText($jsonPath, ($all | ConvertTo-Json -Depth 5 -Compress), (New-Object System.Text.UTF8Encoding($false)))

$cols = @($all[0].PSObject.Properties.Name)
$sb = New-Object System.Text.StringBuilder
$null = $sb.AppendLine(($cols | ForEach-Object { '"' + $_ + '"' }) -join ',')
foreach ($d in $all) {
  $line = foreach ($c in $cols) { '"' + ([string]$d.$c -replace '"', '""') + '"' }
  $null = $sb.AppendLine($line -join ',')
}
$csvPath = "$OutPrefix.csv"
[System.IO.File]::WriteAllText($csvPath, $sb.ToString(), (New-Object System.Text.UTF8Encoding($false)))
Write-Output ("DONE rows={0} csv={1}" -f $all.Count, $csvPath)
