"""비정형 데이터 검사 — 하위 폴더별 파일 수로 통계적 다양성(지표 4)과 수집 자동화 비율(지표 6)을 낸다.

구분(기초·고용·이동권 등)마다 폴더 하나를 두고, 그 안의 하위 폴더를 분포 단위로 본다.
2025년 품질 시험과 같은 산식이다.

    분포 비율(%)      = 하위 폴더 파일 수 / 구분 파일 수 × 100
    정상 분포 기준     = 분포 비율이 3% ~ 30% 안에 있음
    통계적 다양성(%)  = 정상 분포 하위 폴더의 파일 수 합 / 구분 파일 수 × 100
    수집 자동화 비율(%) = 자동 수집한 파일 수 / 전체 비정형 파일 수 × 100

설정 파일(JSON) 예시 — 경로는 실행하는 곳 기준으로 적는다.

    {"groups": [
       {"name": "기초 비정형", "path": "data/basic", "auto": true},
       {"name": "고용 비정형", "path": "data/employment", "auto": false},
       {"name": "이동권 비정형", "path": "data/mobility", "auto": true,
        "auto_override": {"도면 및 지도": false}}
     ],
     "low": 3, "high": 30, "dedup": true,
     "ignore": ["Thumbs.db", ".DS_Store", "desktop.ini"]}

dedup 이 참이면 구분 안에서 내용(SHA-256)이 같은 파일은 한 번만 센다 — 같은 사진을 여러 번 받아
쌓인 사본이 분포와 자동화 비율을 부풀리지 않게 하기 위해서다. 세어 본 중복 수는 결과에 따로 남긴다.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional


def _sha256(p: Path, buf: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(buf), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class FolderCount:
    name: str
    files: int
    duplicates: int = 0
    auto: bool = True
    share_pct: float = 0.0
    in_range: bool = False


@dataclass
class GroupResult:
    name: str
    path: str
    total: int
    duplicates: int
    diversity_pct: Optional[float]
    auto_files: int
    folders: List[FolderCount] = field(default_factory=list)

    def as_dict(self) -> dict:
        d = asdict(self)
        return d


def count_group(path: str | Path, auto: bool = True, auto_override: Optional[Dict[str, bool]] = None,
                low: float = 3.0, high: float = 30.0, dedup: bool = True,
                ignore: Optional[List[str]] = None, name: str = "") -> GroupResult:
    """구분 폴더 하나를 센다. 바로 아래 하위 폴더를 분포 단위로 보고, 그 아래는 재귀로 모두 센다.

    구분 폴더 바로 아래에 놓인 파일(하위 폴더 없음)은 '(폴더 없음)' 하나로 묶는다.
    """
    root = Path(path)
    ign = set(ignore or [])
    seen: set = set()
    folders: Dict[str, FolderCount] = {}
    ov = auto_override or {}
    for dirpath, _dirs, fnames in os.walk(root):
        rel = Path(dirpath).relative_to(root)
        top = rel.parts[0] if rel.parts else "(폴더 없음)"
        fc = folders.setdefault(top, FolderCount(top, 0, 0, ov.get(top, auto)))
        for fn in fnames:
            if fn in ign:
                continue
            if dedup:
                h = _sha256(Path(dirpath) / fn)
                if h in seen:
                    fc.duplicates += 1
                    continue
                seen.add(h)
            fc.files += 1
    total = sum(f.files for f in folders.values())
    ok_files = 0
    for f in folders.values():
        f.share_pct = round(100.0 * f.files / total, 2) if total else 0.0
        f.in_range = total > 0 and low <= f.share_pct <= high
        if f.in_range:
            ok_files += f.files
    flist = sorted((f for f in folders.values() if f.files or f.duplicates), key=lambda f: -f.files)
    return GroupResult(name=name or root.name, path=str(root), total=total,
                       duplicates=sum(f.duplicates for f in folders.values()),
                       diversity_pct=(round(100.0 * ok_files / total, 2) if total else None),
                       auto_files=sum(f.files for f in folders.values() if f.auto), folders=flist)


def run(config_path: str | Path) -> dict:
    """설정 파일의 구분을 모두 세고 다양성(구분별)과 자동화 비율(전체)을 돌려준다."""
    cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))
    low, high = float(cfg.get("low", 3)), float(cfg.get("high", 30))
    groups = [count_group(g["path"], g.get("auto", True), g.get("auto_override"), low, high,
                          bool(cfg.get("dedup", True)), cfg.get("ignore"), g.get("name", ""))
              for g in cfg["groups"]]
    total = sum(g.total for g in groups)
    auto = sum(g.auto_files for g in groups)
    return {
        "groups": [g.as_dict() for g in groups],
        "diversity": {g.name: g.diversity_pct for g in groups},
        "automation": {"auto_files": auto, "total_files": total,
                       "pass_rate": round(100.0 * auto / total, 2) if total else None},
        "rule": {"low": low, "high": high, "dedup": bool(cfg.get("dedup", True))},
    }
