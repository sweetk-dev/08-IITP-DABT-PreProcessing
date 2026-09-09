# 국민건강보험공단 장애인보조기기 급여 등록업소 수집기
#   전동보장구를 취급하는 전국 등록업소의 상호·주소·전화번호를 받아온다.
#   법적 근거: 국민건강보험법 시행규칙 - 장애인보조기기 보험급여 기준 등 고시
#
#   오픈API 는 없다. 조회 화면이 POST 폼이고 세션 종속이 없어 서버 사이드로 재현된다.
#     POST /nhis/policy/retrieveAssistingDevicesRegStoreList.do
#     body: gubn / upsoCd / siDoCd / addr / pumMokCd / brchchk / upsoName / pageIndex
#   응답 HTML 은 한 업소가 tr 두 개다 - tr.hBox(번호·업소명·주소·전화) + tr.cBox(취급품목 dl).
#   한 페이지 10건이고, 총 페이지 수는 "마지막페이지" 링크의 인자로만 알 수 있다.
#
# 사용:
#   powershell -File fetch_nhis_assist_store.ps1 -OutPrefix C:\...\nhis_assist_store
#   powershell -File fetch_nhis_assist_store.ps1 -OutPrefix ... -PumMokCd 17051705   # 의료용 스쿠터
#   powershell -File fetch_nhis_assist_store.ps1 -OutPrefix ... -Sido 41,11          # 특정 시도만
#
# 주의: 이 목록은 "급여 구입처로 공단에 등록된 판매·수리 업소"이지 지자체 수리 지원사업
#       지정업체가 아니다. 적재 시 confidence 를 낮게 잡는다(로더 주석 참조).
param(
  [Parameter(Mandatory=$true)][string]$OutPrefix,
  [string]$PumMokCd = '17041704',        # 17041704 전동휠체어(가군,나군) / 17051705 의료용 스쿠터 / 00009999 전체
  [string]$Sido = '',                    # 쉼표 구분 시도코드. 비우면 17개 시도 전부
  [int]$DelayMs = 350
)
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36'
$url = 'https://www.nhis.or.kr/nhis/policy/retrieveAssistingDevicesRegStoreList.do'

# 조회 화면의 시/도 select 값 그대로. 12 는 2026-07 출범한 전남광주통합특별시다.
# ⚠️ PowerShell 변수는 대소문자를 구분하지 않는다. 이 해시테이블을 $SIDO 로 두면
#    파라미터 $Sido 와 같은 변수가 되어 파라미터를 덮어쓴다. 그러면
#    [string]::IsNullOrWhiteSpace($Sido) 가 거짓이 되고 $Sido.Split(",") 이
#    "System.Collections.Specialized.OrderedDictionary" 를 쪼개 엉뚱하게 1회만 돈다.
$SidoMap = [ordered]@{
  '11' = '서울특별시'; '12' = '전남광주통합특별시'; '26' = '부산광역시'; '27' = '대구광역시';
  '28' = '인천광역시'; '30' = '대전광역시'; '31' = '울산광역시'; '36' = '세종특별자치시';
  '41' = '경기도';     '43' = '충청북도';   '44' = '충청남도';   '47' = '경상북도';
  '48' = '경상남도';   '50' = '제주특별자치도'; '51' = '강원특별자치도'; '52' = '전북특별자치도'
}

function Strip([string]$s) {
  if (-not $s) { return '' }
  $t = $s -replace '(?s)<[^>]+>', ''
  $t = [System.Net.WebUtility]::HtmlDecode($t)
  return ($t -replace '\s+', ' ').Trim()
}

function Get-Page([string]$sidoCd, [int]$page) {
  $body = "gubn=&upsoCd=&siDoCd=$sidoCd&addr=&pumMokCd=$PumMokCd&brchchk=&upsoName=&pageIndex=$page"
  $resp = Invoke-WebRequest -Uri $url -Method Post -Body $body -ContentType 'application/x-www-form-urlencoded' -UseBasicParsing -TimeoutSec 90 -Headers @{ 'User-Agent' = $ua; 'Referer' = $url }
  return [Text.Encoding]::UTF8.GetString($resp.RawContentStream.ToArray())
}

# PowerShell 5.1 에서 [ordered]@{} 의 .Keys 는 @() 로 감싸도 열거되지 않고 딕셔너리
# 자체가 한 원소로 들어간다. GetEnumerator() 로 키를 꺼내야 한다.
$targets = if (-not [string]::IsNullOrWhiteSpace($Sido)) {
  @($Sido.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
} else {
  @($SidoMap.Keys)
}
$all = New-Object System.Collections.ArrayList

foreach ($cd in $targets) {
  $sidoNm = $SidoMap[$cd]
  $html = Get-Page $cd 1
  # ⚠️ 총 10페이지 이하인 시도는 "마지막페이지" 버튼이 아예 렌더링되지 않는다.
  #    (실측: 제주 2p / 울산 4p / 인천 7p 는 class="last" 없음, 서울 26p 는 있음)
  #    그 버튼만 보고 판정하면 1페이지만 긁고 조용히 끝난다. 페이지 링크 전체의
  #    최댓값을 쓰고, 첫 행의 번호(= 결과 총건수)로 한 번 더 교차 검증한다.
  $pageNums = [regex]::Matches($html, 'fn_retrieveAssistingDevicesRegStoreList\((\d+)\)') |
              ForEach-Object { [int]$_.Groups[1].Value }
  $lastPage = if ($pageNums) { ($pageNums | Measure-Object -Maximum).Maximum } else { 1 }
  # 첫 행 번호는 그 조회조건의 총건수다(내림차순 부여). 한 페이지 10건.
  $totM = [regex]::Match($html, '(?s)<tr[^>]*class="[^"]*\bhBox\b[^"]*"[^>]*>\s*<td[^>]*>(\d+)</td>')
  $expected = if ($totM.Success) { [Math]::Ceiling([int]$totM.Groups[1].Value / 10.0) } else { 0 }
  if ($expected -gt $lastPage) { $lastPage = $expected }

  $page = 1
  $before = $all.Count
  while ($true) {
    if ($page -gt 1) { $html = Get-Page $cd $page }
    $pairs = [regex]::Matches($html, '(?s)<tr[^>]*class="[^"]*\bhBox\b[^"]*"[^>]*>(.*?)</tr>\s*<tr[^>]*class="[^"]*\bcBox\b[^"]*"[^>]*>(.*?)</tr>')
    if ($pairs.Count -eq 0) {
      Write-Output ("  [경고] SIDO {0} page {1} 에서 업소 행을 못 찾았다 - 구조 변경 의심" -f $cd, $page)
      break
    }
    foreach ($p in $pairs) {
      $tds = [regex]::Matches($p.Groups[1].Value, '(?s)<td[^>]*>(.*?)</td>')
      if ($tds.Count -lt 4) { continue }
      # 취급품목은 dt(품목)/dd(○ 여부) 쌍이다. ○ 표시된 것만 남긴다.
      $items = @()
      foreach ($d in [regex]::Matches($p.Groups[2].Value, '(?s)<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>')) {
        $nm = Strip $d.Groups[1].Value
        $mark = Strip $d.Groups[2].Value
        if ($nm -and $mark -match '[○●◯✓✔]') { $items += $nm }
      }
      $null = $all.Add([pscustomobject]@{
        sido_cd = $cd
        sido_nm = $sidoNm
        seq     = Strip $tds[0].Groups[1].Value
        name    = Strip $tds[1].Groups[1].Value
        addr    = Strip $tds[2].Groups[1].Value
        tel     = Strip $tds[3].Groups[1].Value
        items   = ($items -join ' | ')
      })
    }
    if ($page -ge $lastPage) { break }
    $page++
    Start-Sleep -Milliseconds $DelayMs
  }
  $got = $all.Count - $before
  $tot = if ($totM.Success) { [int]$totM.Groups[1].Value } else { 0 }
  $warn = if ($tot -gt 0 -and $got -lt $tot) { '  <-- 원천 총건수보다 적다' } else { '' }
  Write-Output ("SIDO {0} {1,-16} pages={2} total={3} rows={4} acc={5}{6}" -f $cd, $sidoNm, $lastPage, $tot, $got, $all.Count, $warn)
  Start-Sleep -Milliseconds $DelayMs
}

if ($all.Count -eq 0) { throw '수집 결과가 0건이다 - 폼 파라미터나 응답 구조가 바뀌었는지 확인할 것' }

$jsonPath = "$OutPrefix.json"
[System.IO.File]::WriteAllText($jsonPath, ($all | ConvertTo-Json -Depth 4 -Compress), (New-Object System.Text.UTF8Encoding($false)))

$cols = @('sido_cd', 'sido_nm', 'seq', 'name', 'addr', 'tel', 'items')
$hdr  = @('시도코드', '시도명', '번호', '업소명', '주소', '전화번호', '취급품목')
$sb = New-Object System.Text.StringBuilder
$null = $sb.AppendLine(($hdr | ForEach-Object { '"' + $_ + '"' }) -join ',')
foreach ($r in $all) {
  $line = foreach ($c in $cols) { '"' + ([string]$r.$c -replace '"', '""') + '"' }
  $null = $sb.AppendLine($line -join ',')
}
$csvPath = "$OutPrefix.csv"
[System.IO.File]::WriteAllText($csvPath, $sb.ToString(), (New-Object System.Text.UTF8Encoding($false)))
Write-Output ("DONE pumMok={0} rows={1} csv={2}" -f $PumMokCd, $all.Count, $csvPath)
