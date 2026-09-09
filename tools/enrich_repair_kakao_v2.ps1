# 수리센터 명부 주소/좌표 보강 v2 - 카카오 로컬 + 검증 게이트
#
# v1 의 문제: 첫 결과를 그대로 받아써서 "서울 강남구 액티피아"가 동작구 매장으로,
#            "강남구 케어존"이 금천구 "실버케어존"으로 붙었다. 지정업체 명부에는
#            체인 상호가 구마다 반복 등장하므로 첫 결과 채택은 구조적으로 틀린다.
#
# v2 규칙 - 아래 둘을 모두 통과한 후보만 좌표를 채운다.
#   (1) 지역 일치: 카카오 주소에 명부의 시군구명이 들어 있어야 한다
#   (2) 상호 일치: 공백/괄호/법인격 표기를 지운 뒤 한쪽이 다른 쪽을 포함해야 한다
# 통과 못 하면 좌표를 비운다. 추정 좌표를 넣지 않는 것이 이 파이프라인의 원칙이다.
param(
  [Parameter(Mandatory=$true)][string]$InCsv,
  [Parameter(Mandatory=$true)][string]$OutCsv,
  [Parameter(Mandatory=$true)][string]$KeyFile
)
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$keyText = [System.IO.File]::ReadAllText($KeyFile)
$km = [regex]::Match($keyText, 'KAKAO_REST_API_KEY\s*[:=]\s*`?([0-9a-zA-Z]{20,})`?')
if (-not $km.Success) { throw 'KAKAO REST key not found' }
$hdr = @{ 'Authorization' = ('KakaoAK ' + $km.Groups[1].Value) }

function Norm([string]$s) {
  if (-not $s) { return '' }
  $t = $s -replace '\(.*?\)', ''
  $t = $t -replace '주식회사|\(주\)|㈜|사회복지법인|사단법인|재단법인', ''
  $t = $t -replace '[\s\.\-_,·]', ''
  return $t
}

function Search-Kakao([string]$q) {
  $u = 'https://dapi.kakao.com/v2/local/search/keyword.json?size=15&query=' + [System.Uri]::EscapeDataString($q)
  try { return (Invoke-RestMethod -Uri $u -Headers $hdr -TimeoutSec 20).documents } catch { return $null }
}

$rows = Import-Csv -Path $InCsv -Encoding UTF8
$out = New-Object System.Collections.ArrayList
$i = 0
foreach ($r in $rows) {
  $i++
  $sido = $r.'시도명'; $sgg = $r.'시군구명'; $name = $r.'업체명'
  $sggClean = ($sgg -replace '\(.*?\)', '').Trim()
  $isWide = ($sggClean -match '전체|권역|남부|북부|전지역|도내') -or (-not $sggClean)
  $nameKey = Norm $name

  $queries = @()
  if (-not $isWide) { $queries += "$sido $sggClean $name" }
  $queries += "$sido $name"
  $queries += "$name"

  $hit = $null; $usedQ = ''; $grade = 'NONE'; $cands = 0
  foreach ($q in $queries) {
    $docs = @(Search-Kakao $q)
    $cands += $docs.Count
    foreach ($d in $docs) {
      $addr = ($d.road_address_name + ' ' + $d.address_name)
      $regionOk = $isWide -or ($addr -like "*$sggClean*")
      $pk = Norm $d.place_name
      $nameOk = ($pk -and $nameKey) -and (($pk -like "*$nameKey*") -or ($nameKey -like "*$pk*"))
      if ($regionOk -and $nameOk) { $hit = $d; $usedQ = $q; $grade = 'A'; break }
    }
    if ($hit) { break }
    # 지역만 맞고 상호가 다른 후보는 채택하지 않는다 (동명 체인 오결합 방지)
    Start-Sleep -Milliseconds 110
  }

  $null = $out.Add([pscustomobject]@{
    sido        = $sido
    sigungu     = $sgg
    oper_type   = $r.'운영방식'
    name        = $name
    support     = $r.'지원기준'
    kakao_name  = if ($hit) { $hit.place_name } else { '' }
    addr_road   = if ($hit) { $hit.road_address_name } else { '' }
    addr_jibun  = if ($hit) { $hit.address_name } else { '' }
    longitude   = if ($hit) { $hit.x } else { '' }
    latitude    = if ($hit) { $hit.y } else { '' }
    tel         = if ($hit) { $hit.phone } else { '' }
    place_url   = if ($hit) { $hit.place_url } else { '' }
    category    = if ($hit) { $hit.category_name } else { '' }
    geo_grade   = $grade
    match_query = $usedQ
    cand_seen   = $cands
  })
  if ($i % 40 -eq 0) { Write-Output ("  ... {0}/{1}" -f $i, $rows.Count) }
  Start-Sleep -Milliseconds 110
}

$out | Export-Csv -Path $OutCsv -NoTypeInformation -Encoding UTF8
$a = @($out | Where-Object { $_.geo_grade -eq 'A' }).Count
Write-Output ("DONE total={0} verified={1} unresolved={2} -> {3}" -f $out.Count, $a, ($out.Count - $a), $OutCsv)
