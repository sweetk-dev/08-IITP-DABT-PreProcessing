# 중앙보조기기센터 전국 보조기기센터 명부 수집기
#   https://www.knat.go.kr/knw/home/knat/knat_map.php
#   중앙 1곳 + 시도·권역 32곳 = 표 33개. 표 하나가 센터 하나이고 caption 이 센터명이다.
#   행 구성: 주소 / 전담지역 / 전화번호 / 홈페이지 / 문의(담당업무별 연락처)
#   사이트 인코딩이 EUC-KR 이라 바이트로 받아 직접 디코딩한다.
#
#   ⚠️ 같은 사이트의 knat_repair.php(수리센터 지정업체 명부 244건)와는 다른 페이지다.
#      이쪽은 공공 위탁 거점이라 주소·전화·홈페이지가 있고, 저쪽은 그게 전혀 없다.
#   ⚠️ 두 페이지 모두 운영시간 항목이 없다. 복무규정으로 추정해 채우지 말 것.
#
# 사용:
#   powershell -File fetch_knat_center.ps1 -OutPrefix C:\...\knat_center
param(
  [Parameter(Mandatory=$true)][string]$OutPrefix
)
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
$url = 'https://www.knat.go.kr/knw/home/knat/knat_map.php'
# 949(CP949)는 51949(euc-kr)의 확장 완성형까지 포함한다. 안전한 쪽을 쓴다.
$euckr = [System.Text.Encoding]::GetEncoding(949)

function Strip([string]$s) {
  if (-not $s) { return '' }
  $t = $s -replace '(?s)<[^>]+>', ''
  $t = [System.Net.WebUtility]::HtmlDecode($t)
  return ($t -replace '\s+', ' ').Trim()
}

$resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 90 -Headers @{ 'User-Agent' = $ua }
$html = $euckr.GetString($resp.RawContentStream.ToArray())

$rows = New-Object System.Collections.ArrayList
foreach ($t in [regex]::Matches($html, '(?s)<table[^>]*>(.*?)</table>')) {
  $seg = $t.Groups[1].Value
  $capM = [regex]::Match($seg, '(?s)<caption>(.*?)</caption>')
  if (-not $capM.Success) { continue }
  $center = (Strip $capM.Groups[1].Value) -replace '\s*정보$', ''
  if (-not $center) { continue }

  $field = @{}
  $homepage = ''
  foreach ($tr in [regex]::Matches($seg, '(?s)<tr[^>]*>\s*<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>\s*</tr>')) {
    $label = Strip $tr.Groups[1].Value
    $raw = $tr.Groups[2].Value
    $field[$label] = Strip $raw
    if ($label -eq '홈페이지') {
      # 작은따옴표 href 도 받는다. 못 잡으면 앵커 텍스트("바로가기")가 URL 로 둔갑한다.
      $a = [regex]::Match($raw, '(?i)href\s*=\s*["'']([^"''>]+)["'']')
      if ($a.Success) { $homepage = $a.Groups[1].Value.Trim() }
    }
  }
  if (-not $field.ContainsKey('주소')) { continue }

  # 주소 앞머리의 "(우)01022" 를 우편번호로 분리한다
  $addr = $field['주소']
  $zip = ''
  $zm = [regex]::Match($addr, '^\(우\)\s*(\d{5,6})\s*(.*)$')
  if ($zm.Success) { $zip = $zm.Groups[1].Value; $addr = $zm.Groups[2].Value.Trim() }

  # href 를 못 찾았을 때의 폴백은 도메인처럼 생긴 문자열일 때만 허용한다
  if (-not $homepage -and $field.ContainsKey('홈페이지')) {
    $cand = $field['홈페이지']
    if ($cand -match '[A-Za-z0-9-]+\.[A-Za-z]{2,}') { $homepage = $cand }
  }
  $null = $rows.Add([pscustomobject]@{
    center   = $center
    zip      = $zip
    addr     = $addr
    area     = if ($field.ContainsKey('전담지역')) { $field['전담지역'] } else { '' }
    tel      = if ($field.ContainsKey('전화번호')) { $field['전화번호'] } else { '' }
    homepage = $homepage
  })
}

if ($rows.Count -eq 0) { throw '수집 결과가 0건이다 - 페이지 구조가 바뀌었는지 확인할 것' }

$jsonPath = "$OutPrefix.json"
[System.IO.File]::WriteAllText($jsonPath, ($rows | ConvertTo-Json -Depth 4 -Compress), (New-Object System.Text.UTF8Encoding($false)))

$cols = @('center', 'zip', 'addr', 'area', 'tel', 'homepage')
$hdr  = @('센터명', '우편번호', '주소', '전담지역', '전화번호', '홈페이지')
$sb = New-Object System.Text.StringBuilder
$null = $sb.AppendLine(($hdr | ForEach-Object { '"' + $_ + '"' }) -join ',')
foreach ($r in $rows) {
  $line = foreach ($c in $cols) { '"' + ([string]$r.$c -replace '"', '""') + '"' }
  $null = $sb.AppendLine($line -join ',')
}
$csvPath = "$OutPrefix.csv"
[System.IO.File]::WriteAllText($csvPath, $sb.ToString(), (New-Object System.Text.UTF8Encoding($false)))
Write-Output ("DONE centers={0} csv={1}" -f $rows.Count, $csvPath)
