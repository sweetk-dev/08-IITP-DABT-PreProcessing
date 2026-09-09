# 주소 -> WGS84 좌표 보강 (카카오 로컬 주소검색)
#   knat 수리센터 명부처럼 상호명만 있는 경우와 달리, 여기는 원천이 도로명/지번 주소를
#   준다. 그래서 키워드 검색이 아니라 주소 검색(address.json)을 쓴다 - 동명 체인
#   오결합 위험이 없고 정확도가 훨씬 높다.
#   주소가 통째로 실패하면 뒤쪽 상세(동/호/층)를 잘라 가며 재시도한다.
#
# 사용:
#   powershell -File enrich_address_kakao.ps1 -InCsv in.csv -AddrCol 주소 -OutCsv out.csv -KeyFile ...\api-keys.md
param(
  [Parameter(Mandatory=$true)][string]$InCsv,
  [Parameter(Mandatory=$true)][string]$OutCsv,
  [Parameter(Mandatory=$true)][string]$KeyFile,
  [string]$AddrCol = '주소',
  [int]$DelayMs = 110
)
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$keyText = [System.IO.File]::ReadAllText($KeyFile)
$km = [regex]::Match($keyText, 'KAKAO_REST_API_KEY\s*[:=]\s*`?([0-9a-zA-Z]{20,})`?')
if (-not $km.Success) { throw 'KAKAO REST key not found in key file' }
$hdr = @{ 'Authorization' = ('KakaoAK ' + $km.Groups[1].Value) }

function Get-Coord([string]$q) {
  if (-not $q) { return $null }
  $u = 'https://dapi.kakao.com/v2/local/search/address.json?size=1&query=' + [System.Uri]::EscapeDataString($q)
  try {
    $r = Invoke-RestMethod -Uri $u -Headers $hdr -TimeoutSec 20
    if ($r.documents -and @($r.documents).Count -gt 0) { return @($r.documents)[0] }
  } catch {
    # 키 만료·권한·쿼터 초과는 조용히 넘기면 전 건이 NONE 으로 끝나고도 성공처럼 보인다.
    $code = 0
    try { $code = [int]$_.Exception.Response.StatusCode.value__ } catch { }
    if ($code -eq 401 -or $code -eq 403 -or $code -eq 429) {
      throw ("카카오 API 오류 HTTP {0} - 키/쿼터를 확인할 것: {1}" -f $code, $_.Exception.Message)
    }
  }
  return $null
}

# 카카오는 번지가 안 맞으면 행정구역 중심점(address_type=REGION)을 돌려준다.
# 그 좌표는 구청·동 중심이지 업소 위치가 아니다 - 확정 좌표로 쓰면 안 된다.
function Test-Exact($doc) {
  if (-not $doc) { return $false }
  return ($doc.address_type -eq 'ROAD_ADDR' -or $doc.address_type -eq 'REGION_ADDR')
}

# "세종특별자치시 충현로 90 (조치원읍), 1층 1호" 처럼 뒤에 상세가 붙으면 주소검색이 실패한다.
# 쉼표 뒤 / 괄호 안을 순차로 걷어내며 재시도한다.
function Get-Variants([string]$addr) {
  $v = @($addr)
  if ($addr -match '^(.*?),') { $v += $Matches[1].Trim() }
  $noParen = ($addr -replace '\(.*?\)', ' ') -replace '\s+', ' '
  $v += $noParen.Trim()
  if ($noParen -match '^(.*?),') { $v += $Matches[1].Trim() }
  # 쉼표 없이 공백으로 층·호가 붙는 표기도 있다 ("... 효원로 241 지하1층")
  $noDetail = ($noParen -replace '\s+(지하\s*\d+|\d+)\s*(층|호|동|관)\b.*$', '') -replace '\s+', ' '
  if ($noDetail -and $noDetail -ne $noParen) { $v += $noDetail.Trim() }
  return @($v | Where-Object { $_ } | Select-Object -Unique)
}

# PS 5.1 은 데이터가 1행뿐이면 배열이 아니라 단일 객체를 돌려준다 - @() 로 고정
$rows = @(Import-Csv -Path $InCsv -Encoding UTF8)
$out = New-Object System.Collections.ArrayList
$i = 0; $hit = 0
foreach ($r in $rows) {
  $i++
  $addr = [string]$r.$AddrCol
  $doc = $null; $used = ''; $rank = 0
  foreach ($q in Get-Variants $addr) {
    $rank++
    $cand = Get-Coord $q
    if (Test-Exact $cand) { $doc = $cand; $used = $q; break }
    # REGION(행정구역 중심)만 나온 질의는 채택하지 않고 다음 변형으로 넘어간다
    Start-Sleep -Milliseconds $DelayMs
  }
  $o = [ordered]@{}
  foreach ($p in $r.PSObject.Properties) { $o[$p.Name] = $p.Value }
  $o['addr_road']    = if ($doc -and $doc.road_address) { $doc.road_address.address_name } else { '' }
  $o['addr_jibun']   = if ($doc -and $doc.address) { $doc.address.address_name } else { '' }
  $o['zip_code']     = if ($doc -and $doc.road_address) { $doc.road_address.zone_no } else { '' }
  $o['sgg_name']     = if ($doc -and $doc.road_address) { $doc.road_address.region_2depth_name }
                       elseif ($doc -and $doc.address) { $doc.address.region_2depth_name } else { '' }
  $o['longitude']    = if ($doc) { $doc.x } else { '' }
  $o['latitude']     = if ($doc) { $doc.y } else { '' }
  $o['address_type'] = if ($doc) { $doc.address_type } else { '' }
  $o['geo_grade']    = if ($doc) { 'A' } else { 'NONE' }
  $o['geo_query']    = $used
  $o['geo_attempt']  = $rank
  if ($doc) { $hit++ }
  $null = $out.Add([pscustomobject]$o)
  if ($i % 100 -eq 0) { Write-Output ("  ... {0}/{1} matched={2}" -f $i, $rows.Count, $hit) }
  Start-Sleep -Milliseconds $DelayMs
}

# PS 5.1 의 Export-Csv -Encoding UTF8 은 BOM 을 강제로 붙인다. 다른 수집기 산출물과
# 인코딩을 맞추기 위해 직접 쓴다. JSON 도 같이 남긴다(기존 수집기 패턴).
$jsonPath = [System.IO.Path]::ChangeExtension($OutCsv, '.json')
[System.IO.File]::WriteAllText($jsonPath, ($out | ConvertTo-Json -Depth 4 -Compress), (New-Object System.Text.UTF8Encoding($false)))

$cols = @($out[0].PSObject.Properties.Name)
$sb = New-Object System.Text.StringBuilder
$null = $sb.AppendLine(($cols | ForEach-Object { '"' + $_ + '"' }) -join ',')
foreach ($r in $out) {
  $line = foreach ($c in $cols) { '"' + ([string]$r.$c -replace '"', '""') + '"' }
  $null = $sb.AppendLine($line -join ',')
}
[System.IO.File]::WriteAllText($OutCsv, $sb.ToString(), (New-Object System.Text.UTF8Encoding($false)))
Write-Output ("DONE total={0} exact={1} unresolved={2} -> {3}" -f $out.Count, $hit, ($out.Count - $hit), $OutCsv)
