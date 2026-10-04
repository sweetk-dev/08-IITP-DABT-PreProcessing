"""DB 통합 테스트의 대상 DB 선택 규칙 테스트 — DB 없이 돈다.

통합 테스트(test_db_processing / test_mobility_db / test_gg_toilet_db)는 대상 DB 에
테스트 테이블을 만들고 지우며 표식 행을 넣고 지운다. 대상은 ITEST_DB_URL 로만 정해야 한다.

config.py / db.py 는 임포트 시점에 .env 를 읽어 DB_URL 을 환경변수로 올린다. 통합 테스트가
DB_URL 을 대신 쓰면, .env 가 있는 배포 폴더에서 테스트를 실행했을 때 배치가 쓰는 DB 에서
통합 테스트가 돈다. 이 파일은 "DB_URL 만 있을 때는 통합 테스트가 건너뛰어진다"를 확인한다.

모듈 전역(ITEST_URL)은 임포트 시점의 환경변수로 정해지므로, 환경변수를 지정한 별도
파이썬 프로세스에서 각 테스트 모듈을 임포트해 값을 읽는다(같은 프로세스에서 다시
임포트하면 이미 수집된 테스트 클래스가 중복 등록된다).
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TESTS_DIR = os.path.join(ROOT, 'tests')

# 대상 DB 에 쓰기를 하는 통합 테스트 모듈과, 그 안의 DB 테스트 클래스.
ITEST_MODULES = {
    'test_db_processing': ('KosisLoadDbTests',),
    'test_mobility_db': ('UpsertFacilitiesDbTests', 'UpsertEmergencySupportDbTests'),
    'test_gg_toilet_db': ('SyncPublicToiletsDbTests',),
}

# 접속되지 않는 주소(포트 1). 테스트는 접속을 시도하지 않지만, 시도하더라도 닿는 DB 가 없게 한다.
# 드라이버를 URL 에 명시한다(+psycopg2): db.py 는 임포트 시점에 DB_URL 로 엔진을 만들고,
# 드라이버를 생략하면 SQLAlchemy 버전에 따라 기본 드라이버가 달라 requirements.txt 에 없는
# 드라이버를 찾다가 임포트가 실패할 수 있다.
UNREACHABLE_URL = 'postgresql+psycopg2://itest_guard:itest_guard@127.0.0.1:1/itest_guard'

# 자식 프로세스에서 실행할 코드: 모듈을 임포트하고, ITEST_URL 과 각 DB 테스트 클래스의
# unittest 건너뜀 표식(__unittest_skip__)을 한 줄로 출력한다.
_PROBE = (
    "import sys, importlib\n"
    "sys.path.insert(0, sys.argv[1]); sys.path.insert(0, sys.argv[2])\n"
    "m = importlib.import_module(sys.argv[3])\n"
    "flags = [str(bool(getattr(getattr(m, c), '__unittest_skip__', False))) for c in sys.argv[4:]]\n"
    "print('PROBE|' + repr(m.ITEST_URL) + '|' + ','.join(flags))\n"
)


def _probe(module, classes, db_url, itest_url):
    """지정한 환경변수로 module 을 임포트한 결과 (ITEST_URL 의 repr, 클래스별 건너뜀 여부) 를 돌려준다.

    ITEST_DB_URL 을 "없음"으로 만들 때는 빈 문자열을 넣는다 — 변수를 아예 지우면 임포트 중
    load_dotenv() 가 주변 .env 의 값을 채울 수 있다(이미 있는 변수는 덮어쓰지 않는다).
    """
    env = dict(os.environ)
    env['DB_URL'] = db_url
    env['ITEST_DB_URL'] = itest_url
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    out = subprocess.run(
        [sys.executable, '-c', _PROBE, ROOT, TESTS_DIR, module] + list(classes),
        env=env, cwd=ROOT, capture_output=True, text=True, timeout=120)
    lines = [line for line in out.stdout.splitlines() if line.startswith('PROBE|')]
    if out.returncode != 0 or not lines:
        raise AssertionError('probe 실패(%s): %s' % (module, out.stderr[-500:]))
    _tag, itest_repr, flags = lines[-1].split('|')
    return itest_repr, [f == 'True' for f in flags.split(',')]


class ItestDbUrlGuardTests(unittest.TestCase):

    def test_db_url_alone_does_not_enable_integration_tests(self):
        """DB_URL 만 있고 ITEST_DB_URL 이 없으면 통합 테스트 클래스는 전부 건너뛴다."""
        for module, classes in ITEST_MODULES.items():
            with self.subTest(module=module):
                itest_repr, skipped = _probe(module, classes, db_url=UNREACHABLE_URL, itest_url='')
                self.assertEqual(itest_repr, 'None')
                self.assertEqual(skipped, [True] * len(classes))

    def test_itest_db_url_enables_integration_tests_and_is_the_target(self):
        """ITEST_DB_URL 이 있으면 실행 대상이 되고, 대상 주소는 DB_URL 이 아니라 ITEST_DB_URL 이다."""
        other = UNREACHABLE_URL.replace('itest_guard@', 'other@')
        for module, classes in ITEST_MODULES.items():
            with self.subTest(module=module):
                itest_repr, skipped = _probe(module, classes, db_url=other, itest_url=UNREACHABLE_URL)
                self.assertEqual(itest_repr, repr(UNREACHABLE_URL))
                self.assertEqual(skipped, [False] * len(classes))


if __name__ == '__main__':
    unittest.main()
