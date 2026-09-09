# data.go.kr 표준데이터셋 통합본 수집기 (그리드 다운로드 채널)
#   상세페이지의 CSV 다운로드 버튼이 호출하는 두 엔드포인트를 그대로 사용한다.
#   1) /download/columList.json?pk={PK}&ext=CSV            -> 컬럼 목록 + 총건수 + 물리테이블명
#   2) /download/standard.json?publicDataPk={PK}&...&page=N -> 실 데이터 (perPage 최대 10000)
#   로그인/인증키가 필요 없다. 오픈API(활용신청 필요)가 막혔을 때의 1차 채널.
#
# 사용: powershell -File fetch_std_dataset.ps1 -Pk 15034533 -OutDir C:\...\out -Slug wheelchair_charger
param(
  [Parameter(Mandatory=$true)][string]$Pk,
  [Parameter(Mandatory=$true)][string]$OutDir,
  [Parameter(Mandatory=$true)][string]$Slug,
  [int]$PerPage = 10000
)
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
$base = 'https://www.data.go.kr'
$hdr = @{ 'User-Agent' = $ua; 'Referer' = "$base/data/$Pk/standard.do"; 'X-Requested-With' = 'XMLHttpRequest' }

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# 세션 쿠키 확보 - 상세페이지를 먼저 연다
$null = Invoke-WebRequest -Uri "$base/data/$Pk/standard.do" -SessionVariable S -UseBasicParsing -TimeoutSec 90 -Headers @{ 'User-Agent' = $ua }

# 1) 헤더
$hjson = (Invoke-WebRequest -Uri "$base/download/columList.json?pk=$Pk&ext=CSV" -WebSession $S -Headers $hdr -UseBasicParsing -TimeoutSec 90).Content
$h = $hjson | ConvertFrom-Json
$total = [int]$h.totalCount
$table = $h.tableVO.svcTableNm
# 제공기관 코드/명(INSTT_CODE, INSTT_NM)을 colNmList 로 보내면 서버가 빈 응답을 준다.
# 요청에서는 빼고, 응답에는 어차피 함께 실려 오므로 CSV 헤더에는 남긴다.
$reqExclude = @('INSTT_CODE', 'INSTT_NM')
$allCodes = @($h.columList | ForEach-Object { $_.columCode })
$allNames = @($h.columList | ForEach-Object { $_.columNm })
$codes = @($allCodes | Where-Object { $reqExclude -notcontains $_ })
Write-Output ("HEADER pk={0} table={1} total={2} cols={3}" -f $Pk, $table, $total, $codes.Count)
[System.IO.File]::WriteAllText((Join-Path $OutDir "${Slug}_columns.json"), $hjson, (New-Object System.Text.UTF8Encoding($false)))

# 2) 데이터 - 페이지 순회
$qs = ($codes | ForEach-Object { "colNmList=$_" }) -join '&'
$pages = [Math]::Max(1, [Math]::Ceiling($total / $PerPage))
$rows = New-Object System.Collections.ArrayList
for ($p = 1; $p -le $pages; $p++) {
  $u = "$base/download/standard.json?publicDataPk=$Pk&$qs&totalCount=$total&svcTableNm=$table&perPage=$PerPage&page=$p"
  $d = (Invoke-WebRequest -Uri $u -WebSession $S -Headers $hdr -UseBasicParsing -TimeoutSec 300).Content | ConvertFrom-Json
  $chunk = if ($d -is [array]) { $d } elseif ($d.PSObject.Properties.Name -contains 'data') { $d.data } else { $d.records }
  foreach ($r in $chunk) { $null = $rows.Add($r) }
  Write-Output ("PAGE {0}/{1} rows={2} acc={3}" -f $p, $pages, @($chunk).Count, $rows.Count)
  Start-Sleep -Milliseconds 400
}

# 3) 원문 그대로 저장 - JSON(UTF-8 BOM 없음) + CSV(한글 헤더)
$jsonPath = Join-Path $OutDir "${Slug}_raw.json"
[System.IO.File]::WriteAllText($jsonPath, ($rows | ConvertTo-Json -Depth 6 -Compress), (New-Object System.Text.UTF8Encoding($false)))

$sb = New-Object System.Text.StringBuilder
$null = $sb.AppendLine(($allNames | ForEach-Object { '"' + ($_ -replace '"', '""') + '"' }) -join ',')
foreach ($r in $rows) {
  $line = @()
  foreach ($c in $allCodes) {
    $v = $r.$c
    if ($null -eq $v) { $v = '' }
    $line += '"' + ([string]$v -replace '"', '""') + '"'
  }
  $null = $sb.AppendLine($line -join ',')
}
$csvPath = Join-Path $OutDir "${Slug}_raw.csv"
[System.IO.File]::WriteAllText($csvPath, $sb.ToString(), (New-Object System.Text.UTF8Encoding($false)))
Write-Output ("DONE rows={0} json={1} csv={2}" -f $rows.Count, $jsonPath, $csvPath)
