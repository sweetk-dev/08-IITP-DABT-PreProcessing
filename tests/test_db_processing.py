"""db_processing 테스트 — KOSIS 응답 검증(단위) + 원본·통합 테이블 적재(통합).

두 묶음으로 나뉜다.

1. ``ValidateKosisDataResponseTests`` — DB 없이 도는 단위 테스트.
   적재 전 응답 검증 함수(validate_kosis_data_response)의 판정만 본다.

2. ``KosisLoadDbTests`` — 실제 PostgreSQL 이 있을 때만 돈다.
   ITEST_DB_URL 이 설정돼 있지 않으면 건너뛴다(DB_URL 만 있는 환경에서는 실행하지 않는다).
   01-IITP-DABT-Database 의 basic init 스크립트가 적용된 DB 가 필요하다.

   운영 테이블의 기존 행은 건드리지 않는다:
   - 통합 테이블은 테스트 전용 사본(stats_itest_intg_a / _b)을 만들어 쓰고 끝나면 지운다.
   - 외부 API·통계 소스·요약 정보 행은 'ITEST' 표식을 단 전용 행을 넣었다가 지운다.
   - 원본(stats_kosis_origin_data)·메타 테이블은 전용 src_data_id 의 행만 다룬다.
   - 문자형 dt 분기는 테이블 이름(stats_dis_hlth_disease_cost_sub)으로 갈리므로 그 테이블을
     그대로 쓰되, 전용 src_data_id 의 행만 넣고 지운다.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import db_processing  # noqa: E402
from db_processing import KosisDataValidationError, validate_kosis_data_response  # noqa: E402

# 통합 테스트 대상 DB 는 ITEST_DB_URL 로만 정한다(DB_URL 로 대신하지 않는다).
# config.py / db.py 는 임포트 시점에 .env 를 읽어 DB_URL 을 환경변수로 올린다. DB_URL 을
# 대신 쓰면 .env 가 있는 배포 폴더에서 테스트를 돌렸을 때 배치가 쓰는 DB 에서
# 통합 테스트(테스트 테이블 생성·삭제, 표식 행 삽입·삭제)가 실행된다.
# 빈 문자열도 "미설정"으로 본다.
ITEST_URL = os.getenv('ITEST_DB_URL') or None


# ---------------------------------------------------------------------------
# 1. 단위 테스트 — 응답 검증 함수
# ---------------------------------------------------------------------------
class ValidateKosisDataResponseTests(unittest.TestCase):
    """적재 가능 판정: 리스트 + 비어 있지 않음 + 오류 행 없음 + 숫자 PRD_DE 행 1개 이상."""

    def test_normal_list_is_ok(self):
        ok, reason = validate_kosis_data_response([{'PRD_DE': '2023', 'DT': '1'}])
        self.assertTrue(ok)
        self.assertEqual(reason, '')

    def test_error_dict_is_rejected_with_code_and_message(self):
        ok, reason = validate_kosis_data_response({'err': '20', 'errMsg': '필수요청변수값이 누락되었습니다.'})
        self.assertFalse(ok)
        self.assertIn('err=20', reason)
        self.assertIn('필수요청변수값', reason)

    def test_plain_dict_is_rejected(self):
        """오류 키가 없어도 dict 는 행 목록이 아니다(1행짜리 데이터로 취급하지 않는다)."""
        ok, reason = validate_kosis_data_response({'PRD_DE': '2023', 'DT': '1'})
        self.assertFalse(ok)
        self.assertIn('리스트가 아님', reason)

    def test_empty_list_is_rejected(self):
        ok, reason = validate_kosis_data_response([])
        self.assertFalse(ok)
        self.assertIn('빈 리스트', reason)

    def test_none_and_string_are_rejected(self):
        for value in (None, 'error text', 3):
            ok, _ = validate_kosis_data_response(value)
            self.assertFalse(ok, repr(value))

    def test_list_without_numeric_prd_de_is_rejected(self):
        rows = [{'PRD_DE': '', 'DT': '1'}, {'DT': '2'}, {'PRD_DE': '2023Q1', 'DT': '3'}]
        ok, reason = validate_kosis_data_response(rows)
        self.assertFalse(ok)
        self.assertIn('숫자 PRD_DE', reason)

    def test_one_numeric_prd_de_is_enough(self):
        rows = [{'PRD_DE': '', 'DT': '1'}, {'PRD_DE': '2022', 'DT': '2'}]
        ok, _ = validate_kosis_data_response(rows)
        self.assertTrue(ok)

    def test_error_dict_mixed_into_rows_is_rejected(self):
        """기간 분할 수집 결과에 오류 dict 가 섞이면 일부 기간이 빠진 응답이다."""
        rows = [{'PRD_DE': '2022', 'DT': '2'}, {'err': '30', 'errMsg': '데이터가 존재하지 않습니다.'}]
        ok, reason = validate_kosis_data_response(rows)
        self.assertFalse(ok)
        self.assertIn('err=30', reason)

    def test_non_dict_row_is_rejected(self):
        ok, _ = validate_kosis_data_response([{'PRD_DE': '2022'}, 'x'])
        self.assertFalse(ok)


# ---------------------------------------------------------------------------
# 1-1. 단위 테스트 — 갱신일(SendDe) 확인과 meta 오류 본문 판별 (DB 불필요)
# ---------------------------------------------------------------------------
class LatestDateRequiredTests(unittest.TestCase):
    """갱신일을 얻지 못한 통계는 DB 에 쓰기 전에 명시적 사유로 실패한다.

    DB 세션은 MagicMock 으로 대신한다 — 실패 시 session.execute 가 한 번도 호출되지 않아야
    "기존 데이터에 손대지 않았다"가 성립한다.
    """

    VALID_ROWS = [{'PRD_DE': '2023', 'DT': '1', 'TBL_ID': 'DT_X', 'C1': 'A', 'ITM_ID': 'T1'}]
    META_XML = ('<Structures><MetaRow><objId>ITEM</objId><itmId>T1</itmId><objIdSn>1</objIdSn>'
                '</MetaRow></Structures>')

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix='utest_kosis_')

    def tearDown(self):
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, name, body):
        path = os.path.join(self.tmp, name)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(body)
        return path

    def _file_info(self, latest_name, latest_body, meta_body=None):
        return {
            'stat_tbl_id': 'DT_X', 'src_data_id': 1, 'ext_api_id': 1, 'stat_api_id': 1,
            'latest_path': self._write(latest_name, latest_body),
            'data_path': self._write('data.json', json.dumps(self.VALID_ROWS)),
            'meta_path': self._write('meta.xml', meta_body if meta_body is not None else self.META_XML),
        }

    def _assert_rejected_before_any_sql(self, file_info, *expected):
        session = MagicMock()
        with self.assertRaises(KosisDataValidationError) as cm:
            db_processing.process_single_statistic(
                session, file_info, {'ext_api_id': 1}, {'stat_tbl_id': 'DT_X'},
                {'intg_tbl_id': 'stats_x', 'src_data_id': 1})
        session.execute.assert_not_called()
        for text in expected:
            self.assertIn(text, str(cm.exception))

    # --- latest: JSON 형식 ---
    def test_json_latest_without_send_de_is_rejected(self):
        for body in ('[]', '[{"TBL_ID": "DT_X"}]', '{}', '[{"SendDe": ""}]'):
            with self.subTest(body=body):
                self._assert_rejected_before_any_sql(
                    self._file_info('latest.json', body), '갱신일을 확인하지 못함', '기존 데이터 유지')

    def test_json_latest_error_dict_reason_has_code(self):
        self._assert_rejected_before_any_sql(
            self._file_info('latest.json', json.dumps({'err': '20', 'errMsg': '필수요청변수값이 누락되었습니다.'})),
            '갱신일을 확인하지 못함', 'err=20')

    # --- latest: text(XML) 형식 ---
    def test_xml_latest_without_send_de_element_is_rejected(self):
        self._assert_rejected_before_any_sql(
            self._file_info('latest.xml', '<Structures><MetaRow><tblId>DT_X</tblId></MetaRow></Structures>'),
            '갱신일을 확인하지 못함', 'SendDe 없음')

    def test_xml_latest_error_elements_are_named_in_reason(self):
        self._assert_rejected_before_any_sql(
            self._file_info('latest.xml', '<error><err>30</err><errMsg>데이터가 존재하지 않습니다.</errMsg></error>'),
            '갱신일을 확인하지 못함', 'err=30', '데이터가 존재하지 않습니다.')

    def test_xml_latest_holding_json_error_text_is_rejected_with_code(self):
        """형식이 XML 로 설정된 통계에 JSON 오류 본문이 저장된 경우 — XML 해석 예외가 아니라 사유가 남는다."""
        self._assert_rejected_before_any_sql(
            self._file_info('latest.xml', '{"err":"11","errMsg":"유효하지 않은 인증키입니다."}'),
            '갱신일을 확인하지 못함', 'err=11')

    def test_xml_latest_unparsable_body_is_rejected(self):
        self._assert_rejected_before_any_sql(
            self._file_info('latest.xml', '서비스 점검 중입니다'), '갱신일을 확인하지 못함', '해석할 수 없음')

    def test_latest_with_send_de_passes_this_check(self):
        """갱신일이 있으면 이 검사에서 걸리지 않고 적재 단계로 넘어간다(JSON·XML 모두)."""
        cases = (('latest.json', '[{"SendDe": "2025-03-06"}, {"SendDe": "2024-01-01"}]'),
                 ('latest.xml', '<S><MetaRow><SendDe>2025-03-06</SendDe></MetaRow></S>'))
        for name, body in cases:
            with self.subTest(name=name):
                self.assertEqual(
                    db_processing._require_latest_date(self._write(name, body), 'DT_X'), '2025-03-06')

    # --- meta: text 형식에 저장된 오류 본문 ---
    def test_meta_json_error_text_is_rejected_before_any_sql(self):
        self._assert_rejected_before_any_sql(
            self._file_info('latest.json', '[{"SendDe": "2025-03-06"}]',
                            meta_body='{"err":"20","errMsg":"필수요청변수값이 누락되었습니다."}'),
            'meta 응답이 오류 본문', 'err=20')

    def test_describe_error_body_ignores_normal_bodies(self):
        for body in (None, '', self.META_XML, '[{"SendDe": "2025-03-06"}]', '{"SendDe": "2025-03-06"}',
                     'XML 이 아닌 본문'):
            with self.subTest(body=body):
                self.assertIsNone(db_processing._describe_error_body(body))


# ---------------------------------------------------------------------------
# 2. 통합 테스트 — 실제 PostgreSQL
# ---------------------------------------------------------------------------
INTG_A = 'stats_itest_intg_a'
INTG_B = 'stats_itest_intg_b'
INTG_COST = 'stats_dis_hlth_disease_cost_sub'   # 문자형 dt 분기(테이블 이름으로 갈린다)
EXT_SYS = 'ITEST_KOSIS'
IF_NAME = 'ITEST-KOSIS-db-processing'
LATEST = '2025-03-06'

# stat_tbl_id → 통합 테이블
STATS = {
    'ITEST_DT_A': INTG_A,
    'ITEST_DT_B': INTG_B,
    'ITEST_DT_C': INTG_COST,
}

META_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>\n<Structures>'
    '<MetaRow><objId>ITEM</objId><objNm>항목</objNm><itmId>T1</itmId><itmNm>인원</itmNm>'
    '<objIdSn>1</objIdSn><unitNm>명</unitNm></MetaRow>'
    '<MetaRow><objId>A</objId><objNm>구분</objNm><itmId>A1</itmId><itmNm>전체</itmNm>'
    '<objIdSn>2</objIdSn></MetaRow>'
    # 데이터의 분류 코드(A1·A2)가 모두 메타에 있어야 한다 — 적재 끝에 메타 무결성을 확인한다(v1.17.0).
    '<MetaRow><objId>A</objId><objNm>구분</objNm><itmId>A2</itmId><itmNm>세부</itmNm>'
    '<objIdSn>3</objIdSn></MetaRow>'
    '</Structures>'
)


def _rows(stat_tbl_id, years=(2021, 2022, 2023), cats=('A1', 'A2'), dt='10'):
    """KOSIS data 응답 모양의 행 목록. 행 수 = len(years) * len(cats)."""
    out = []
    for year in years:
        for cat in cats:
            out.append({
                'ORG_ID': '117', 'TBL_ID': stat_tbl_id, 'TBL_NM': '시험 통계',
                'C1': cat, 'C1_OBJ_NM': '구분', 'C1_NM': '분류 ' + cat,
                'ITM_ID': 'T1', 'ITM_NM': '인원', 'UNIT_NM': '명',
                'PRD_SE': 'Y', 'PRD_DE': str(year), 'DT': dt, 'LST_CHN_DE': '2025-03-01',
            })
    return out


@unittest.skipUnless(ITEST_URL, 'ITEST_DB_URL 미설정 — DB 통합 테스트 생략')
class KosisLoadDbTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        from sqlalchemy import create_engine, text
        from sqlalchemy.orm import sessionmaker
        cls.text = staticmethod(text)
        cls.engine = create_engine(ITEST_URL)
        # db_processing 은 모듈 전역 Session 으로 접속한다. 테스트 대상 DB 로 바꿔 끼운다.
        cls._orig_session = db_processing.Session
        db_processing.Session = sessionmaker(bind=cls.engine)
        cls.tmp = tempfile.mkdtemp(prefix='itest_kosis_')
        cls._cleanup_db()
        with cls.engine.begin() as conn:
            for tbl in (INTG_A, INTG_B):
                # 인덱스·FK 는 복사하지 않는다(UNIQUE 인덱스가 있는 경우는 별도 테스트에서 건다).
                conn.execute(text(
                    'CREATE TABLE ' + tbl +
                    ' (LIKE stats_dis_reg_natl_by_new INCLUDING DEFAULTS INCLUDING IDENTITY)'))
            cls.ext_api_id = conn.execute(text(
                "INSERT INTO sys_ext_api_info (if_name, ext_sys, ext_url, auth, data_format, created_by)"
                " VALUES (:n, :s, 'http://invalid.example', 'k', 'json', 'ITEST') RETURNING ext_api_id"),
                {'n': IF_NAME, 's': EXT_SYS}).scalar()
            cls.src_data_id = {}
            cls.stat_api_id = {}
            for stat_tbl_id, intg in STATS.items():
                stat_api_id = conn.execute(text(
                    "INSERT INTO sys_stats_src_api_info (ext_api_id, stat_title, stat_tbl_id, use_base_url_yn,"
                    " api_data_url, created_by) VALUES (:e, :t, :s, 'N', '{}', 'ITEST') RETURNING stat_api_id"),
                    {'e': cls.ext_api_id, 't': 'ITEST ' + stat_tbl_id, 's': stat_tbl_id}).scalar()
                src_data_id = conn.execute(text(
                    "INSERT INTO stats_src_data_info (ext_api_id, ext_sys, stat_api_id, intg_tbl_id, stat_title,"
                    " stat_org_id, stat_survey_name, stat_pub_dt, periodicity, collect_start_dt, collect_end_dt,"
                    " stat_tbl_id, stat_tbl_name, stat_latest_chn_dt, created_by)"
                    " VALUES (:e, 'KOSIS', :a, :i, :t, 'ITEST', 'ITEST', '2025', '년', '2021', '2023',"
                    " :s, :t, '2000-01-01', 'ITEST') RETURNING src_data_id"),
                    {'e': cls.ext_api_id, 'a': stat_api_id, 'i': intg,
                     't': 'ITEST ' + stat_tbl_id, 's': stat_tbl_id}).scalar()
                cls.stat_api_id[stat_tbl_id] = stat_api_id
                cls.src_data_id[stat_tbl_id] = src_data_id
            # sys_data_summary_info 는 통합 테이블마다 1행이 있어야 적재가 성공한다.
            # 전용 사본 테이블용 행은 새로 넣고, 실제 테이블(INTG_COST)용 행은 없을 때만 넣는다.
            cls._summary_created = []
            for intg in (INTG_A, INTG_B, INTG_COST):
                exists = conn.execute(text(
                    "SELECT count(*) FROM sys_data_summary_info WHERE sys_tbl_id = :t"), {'t': intg}).scalar()
                if exists:
                    continue
                conn.execute(text(
                    "INSERT INTO sys_data_summary_info (data_type, title, sys_tbl_id, src_org_name,"
                    " src_latest_chn_dt, sys_data_reg_dt, open_api_url)"
                    " VALUES ('basic', :ti, :t, 'ITEST', '2000-01-01', '2000-01-01', '/itest')"),
                    {'ti': 'ITEST ' + intg, 't': intg})
                cls._summary_created.append(intg)
            # 실제 요약 행을 쓰는 경우 테스트가 바꾼 값을 되돌리기 위해 원래 값을 기억해 둔다.
            cls._summary_saved = conn.execute(text(
                "SELECT src_latest_chn_dt, sys_data_ref_dt, updated_at FROM sys_data_summary_info"
                " WHERE sys_tbl_id = :t"), {'t': INTG_COST}).fetchone()

    @classmethod
    def tearDownClass(cls):
        db_processing.Session = cls._orig_session
        text = cls.text
        with cls.engine.begin() as conn:
            if INTG_COST not in cls._summary_created and cls._summary_saved is not None:
                conn.execute(text(
                    "UPDATE sys_data_summary_info SET src_latest_chn_dt = :a, sys_data_ref_dt = :b,"
                    " updated_at = :c WHERE sys_tbl_id = :t"),
                    {'a': cls._summary_saved[0], 'b': cls._summary_saved[1],
                     'c': cls._summary_saved[2], 't': INTG_COST})
            for intg in cls._summary_created:
                conn.execute(text("DELETE FROM sys_data_summary_info WHERE sys_tbl_id = :t"), {'t': intg})
        cls._cleanup_db()
        cls.engine.dispose()

    @classmethod
    def _cleanup_db(cls):
        """이 테스트가 만든 행·테이블만 지운다(표식: if_name / ext_sys / 테이블명)."""
        text = cls.text
        with cls.engine.begin() as conn:
            ids = [r[0] for r in conn.execute(text(
                "SELECT d.src_data_id FROM stats_src_data_info d JOIN sys_ext_api_info e"
                " ON e.ext_api_id = d.ext_api_id WHERE e.if_name = :n"), {'n': IF_NAME})]
            for sid in ids:
                conn.execute(text("DELETE FROM stats_kosis_origin_data WHERE src_data_id = :s"), {'s': sid})
                conn.execute(text("DELETE FROM stats_kosis_metadata_code WHERE src_data_id = :s"), {'s': sid})
                conn.execute(text("DELETE FROM " + INTG_COST + " WHERE src_data_id = :s"), {'s': sid})
            conn.execute(text(
                "DELETE FROM stats_src_data_info WHERE ext_api_id IN"
                " (SELECT ext_api_id FROM sys_ext_api_info WHERE if_name = :n)"), {'n': IF_NAME})
            conn.execute(text(
                "DELETE FROM sys_stats_src_api_info WHERE ext_api_id IN"
                " (SELECT ext_api_id FROM sys_ext_api_info WHERE if_name = :n)"), {'n': IF_NAME})
            conn.execute(text("DELETE FROM sys_ext_api_info WHERE if_name = :n"), {'n': IF_NAME})
            for tbl in (INTG_A, INTG_B):
                conn.execute(text('DROP TABLE IF EXISTS ' + tbl))

    def setUp(self):
        """테스트마다 전용 행을 비우고 동기화 시각을 NULL 로 되돌린다."""
        text = self.text
        with self.engine.begin() as conn:
            for stat_tbl_id, intg in STATS.items():
                sid = self.src_data_id[stat_tbl_id]
                conn.execute(text("DELETE FROM " + intg + " WHERE src_data_id = :s"), {'s': sid})
                conn.execute(text("DELETE FROM stats_kosis_origin_data WHERE src_data_id = :s"), {'s': sid})
                conn.execute(text("DELETE FROM stats_kosis_metadata_code WHERE src_data_id = :s"), {'s': sid})
            conn.execute(text("DROP INDEX IF EXISTS uidx_itest_intg_a_key"))
            conn.execute(text(
                "UPDATE sys_stats_src_api_info SET latest_sync_time = NULL WHERE ext_api_id = :e"),
                {'e': self.ext_api_id})
            conn.execute(text(
                "UPDATE sys_ext_api_info SET latest_sync_time = NULL WHERE ext_api_id = :e"),
                {'e': self.ext_api_id})

    # --- 도우미 -------------------------------------------------------------
    def _file_info(self, stat_tbl_id, data, latest=LATEST, meta=META_XML):
        """수집 단계가 남기는 파일 3종(data/latest/meta)을 임시 폴더에 만들고 file_info 를 돌려준다."""
        base = os.path.join(self.tmp, stat_tbl_id + '_' + str(len(os.listdir(self.tmp))))
        paths = {'data': base + '_data.json', 'latest': base + '_latest.json', 'meta': base + '_meta.xml'}
        with open(paths['data'], 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        with open(paths['latest'], 'w', encoding='utf-8') as f:
            json.dump([{'SendDe': latest}] if latest else [], f)
        with open(paths['meta'], 'w', encoding='utf-8') as f:
            f.write(meta)
        return {
            'stat_tbl_id': stat_tbl_id,
            'meta_path': paths['meta'], 'latest_path': paths['latest'], 'data_path': paths['data'],
            'ext_api_id': self.ext_api_id, 'stat_api_id': self.stat_api_id[stat_tbl_id],
            'src_data_id': self.src_data_id[stat_tbl_id], 'ext_sys': 'KOSIS',
        }

    def _run(self, responses, collect_failed=None, col_maps=None, **file_kwargs):
        """responses: {stat_tbl_id: data 응답}. process_db_insertion 을 실제로 실행한다.

        col_maps: {stat_tbl_id: intg_col_map(JSON 문자열/dict)} — 통합 테이블 칸 배치 규칙(v1.17.0).
        """
        saved = [self._file_info(s, d, **file_kwargs) for s, d in responses.items()]
        api_info = {'ext_api_id': self.ext_api_id}
        stats_src_list = [{'stat_tbl_id': s, 'ext_api_id': self.ext_api_id,
                           'stat_api_id': self.stat_api_id[s],
                           'intg_col_map': (col_maps or {}).get(s)} for s in STATS]
        info = {s: {'intg_tbl_id': intg, 'src_data_id': self.src_data_id[s]} for s, intg in STATS.items()}
        return db_processing.process_db_insertion(saved, api_info, stats_src_list, info,
                                                  collect_failed=collect_failed)

    def _count(self, table, stat_tbl_id):
        with self.engine.begin() as conn:
            return conn.execute(self.text(
                "SELECT count(*) FROM " + table + " WHERE src_data_id = :s"),
                {'s': self.src_data_id[stat_tbl_id]}).scalar()

    def _sync_time(self, stat_tbl_id=None):
        with self.engine.begin() as conn:
            if stat_tbl_id is None:
                return conn.execute(self.text(
                    "SELECT latest_sync_time FROM sys_ext_api_info WHERE ext_api_id = :e"),
                    {'e': self.ext_api_id}).scalar()
            return conn.execute(self.text(
                "SELECT latest_sync_time FROM sys_stats_src_api_info WHERE stat_api_id = :a"),
                {'a': self.stat_api_id[stat_tbl_id]}).scalar()

    def _age_rows(self, stat_tbl_id, days=3):
        """방금 적재한 행을 며칠 전에 적재된 것처럼 만든다(다른 날 재실행 상황)."""
        sid = self.src_data_id[stat_tbl_id]
        with self.engine.begin() as conn:
            for tbl in (STATS[stat_tbl_id], 'stats_kosis_origin_data', 'stats_kosis_metadata_code'):
                conn.execute(self.text(
                    "UPDATE " + tbl + " SET created_at = created_at - make_interval(days => :d)"
                    " WHERE src_data_id = :s"), {'d': days, 's': sid})


    # --- (v1.17.0) KOSIS 구조 변경 대응 — 칸 배치 규칙·모양 가드·메타 무결성 -----------
    # KOSIS 가 예전 "항목"(성별)을 "분류"로 옮긴 상황을 재현한다.
    #   예전 응답: C1=구분(A1·A2), ITM_ID=성별(T001 계·T002 남자)
    #   새 응답  : C1=구분, C2=성별(T001·T002), ITM_ID=T1(인원) 1개
    # 규칙 {"c2": null, "itm_id": "C2", "item_obj": "D"} 를 쓰면 통합 테이블은 예전과 같은 모양이어야 한다.
    PIVOT_RULE = {'c2': None, 'itm_id': 'C2', 'item_obj': 'D'}
    PIVOT_META = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<Structures>'
        '<MetaRow><objId>ITEM</objId><objNm>항목</objNm><itmId>T1</itmId><itmNm>인원</itmNm></MetaRow>'
        '<MetaRow><objId>A</objId><objNm>구분</objNm><itmId>A1</itmId><itmNm>전체</itmNm></MetaRow>'
        '<MetaRow><objId>A</objId><objNm>구분</objNm><itmId>A2</itmId><itmNm>세부</itmNm></MetaRow>'
        '<MetaRow><objId>D</objId><objNm>성별</objNm><itmId>T001</itmId><itmNm>계</itmNm></MetaRow>'
        '<MetaRow><objId>D</objId><objNm>성별</objNm><itmId>T002</itmId><itmNm>남자</itmNm></MetaRow>'
        '</Structures>')
    OLD_META = (
        '<?xml version="1.0" encoding="UTF-8"?>\n<Structures>'
        '<MetaRow><objId>ITEM</objId><objNm>항목</objNm><itmId>T001</itmId><itmNm>계</itmNm></MetaRow>'
        '<MetaRow><objId>ITEM</objId><objNm>항목</objNm><itmId>T002</itmId><itmNm>남자</itmNm></MetaRow>'
        '<MetaRow><objId>A</objId><objNm>구분</objNm><itmId>A1</itmId><itmNm>전체</itmNm></MetaRow>'
        '<MetaRow><objId>A</objId><objNm>구분</objNm><itmId>A2</itmId><itmNm>세부</itmNm></MetaRow>'
        '</Structures>')

    @staticmethod
    def _old_shape(years=(2021, 2022, 2023)):
        return [{'TBL_ID': 'ITEST_DT_A', 'PRD_DE': str(y), 'DT': '7', 'C1': c, 'ITM_ID': g, 'UNIT_NM': '명'}
                for y in years for c in ('A1', 'A2') for g in ('T001', 'T002')]

    @staticmethod
    def _new_shape(years=(2021, 2022, 2023, 2024), dt='7'):
        return [{'TBL_ID': 'ITEST_DT_A', 'PRD_DE': str(y), 'DT': dt, 'C1': c, 'C1_OBJ_NM': '구분',
                 'C2': g, 'C2_OBJ_NM': '성별', 'ITM_ID': 'T1', 'UNIT_NM': '명'}
                for y in years for c in ('A1', 'A2') for g in ('T001', 'T002')]

    def _intg_keys(self):
        with self.engine.begin() as conn:
            return sorted(tuple(r) for r in conn.execute(self.text(
                "SELECT prd_de, c1, COALESCE(c2,''), itm_id FROM " + INTG_A + " WHERE src_data_id = :s"),
                {'s': self.src_data_id['ITEST_DT_A']}))

    def test_pivot_rule_keeps_legacy_shape_and_remaps_meta(self):
        self._run({'ITEST_DT_A': self._old_shape()}, meta=self.OLD_META)
        before = self._intg_keys()
        result = self._run({'ITEST_DT_A': self._new_shape()}, meta=self.PIVOT_META,
                           col_maps={'ITEST_DT_A': self.PIVOT_RULE}, latest='2026-08-24')
        self.assertEqual(result['failed'], [])
        after = self._intg_keys()
        # 예전 연도 키는 그대로, 새 연도(2024)만 같은 모양으로 늘었다
        self.assertEqual([k for k in after if k[0] != 2024], before)
        self.assertEqual(sorted({k[3] for k in after}), ['T001', 'T002'])
        self.assertEqual({k[2] for k in after}, {''})
        with self.engine.begin() as conn:
            meta = conn.execute(self.text(
                "SELECT obj_id, obj_nm, itm_id, itm_nm FROM stats_kosis_metadata_code"
                " WHERE src_data_id = :s AND obj_nm = '항목' ORDER BY itm_id"),
                {'s': self.src_data_id['ITEST_DT_A']}).all()
            info = conn.execute(self.text(
                "SELECT collect_end_dt, avail_cat_cols FROM stats_src_data_info WHERE src_data_id = :s"),
                {'s': self.src_data_id['ITEST_DT_A']}).one()
        self.assertEqual([tuple(m) for m in meta], [('ITEM', '항목', 'T001', '계'), ('ITEM', '항목', 'T002', '남자')])
        self.assertEqual(info[0], '2024')               # 수집 종료연도가 받은 마지막 연도로 앞당겨졌다
        self.assertEqual(json.loads(info[1]), ['c1'])  # 통합 테이블에 값이 들어간 분류 칸만

    def test_new_structure_without_rule_is_blocked_and_preserved(self):
        """규칙 없이 새 구조를 받으면(주소만 고친 상황) 키가 맞지 않아 멈추고 기존 행을 지킨다."""
        self._run({'ITEST_DT_A': self._old_shape()}, meta=self.OLD_META)
        before = self._intg_keys()
        result = self._run({'ITEST_DT_A': self._new_shape()}, meta=self.PIVOT_META)
        self.assertEqual([f[0] for f in result['failed']], ['ITEST_DT_A'])
        self.assertIn('키가 일치', result['failed'][0][1])
        self.assertEqual(self._intg_keys(), before)

    def test_accept_reshape_env_allows_one_off_reload(self):
        self._run({'ITEST_DT_A': self._old_shape()}, meta=self.OLD_META)
        with patch.dict(os.environ, {'KOSIS_ACCEPT_RESHAPE': 'ITEST_DT_A'}):
            result = self._run({'ITEST_DT_A': self._new_shape()}, meta=self.PIVOT_META)
        self.assertEqual(result['failed'], [])
        self.assertEqual({k[3] for k in self._intg_keys()}, {'T1'})

    def test_missing_existing_year_is_blocked(self):
        self._run({'ITEST_DT_A': self._old_shape()}, meta=self.OLD_META)
        result = self._run({'ITEST_DT_A': self._new_shape(years=(2022, 2023, 2024))}, meta=self.PIVOT_META,
                           col_maps={'ITEST_DT_A': self.PIVOT_RULE})
        self.assertIn('기존 연도 2021', result['failed'][0][1])
        self.assertEqual(len(self._intg_keys()), 12)

    def test_code_missing_in_meta_rolls_back(self):
        """메타에 이름이 없는 코드가 생기면(공개 API 오류 원인) 적재 전체를 되돌린다."""
        self._run({'ITEST_DT_A': self._old_shape()}, meta=self.OLD_META)
        bad_meta = self.PIVOT_META.replace(
            '<MetaRow><objId>D</objId><objNm>성별</objNm><itmId>T002</itmId><itmNm>남자</itmNm></MetaRow>', '')
        result = self._run({'ITEST_DT_A': self._new_shape()}, meta=bad_meta,
                           col_maps={'ITEST_DT_A': self.PIVOT_RULE})
        self.assertIn('항목 코드 1건(T002)', result['failed'][0][1])
        self.assertEqual(len(self._intg_keys()), 12)

    def test_bad_rule_fails_only_that_statistic(self):
        result = self._run({'ITEST_DT_A': self._new_shape(), 'ITEST_DT_B': _rows('ITEST_DT_B')},
                           meta=self.PIVOT_META, col_maps={'ITEST_DT_A': {'itm_id': 'C2'}})
        self.assertEqual([f[0] for f in result['failed']], ['ITEST_DT_A'])
        self.assertIn('item_obj 를 지정해야 함', result['failed'][0][1])
        self.assertEqual(result['succeeded'], ['ITEST_DT_B'])

    def test_partial_scope_does_not_touch_system_sync_time(self):
        saved = [self._file_info('ITEST_DT_A', _rows('ITEST_DT_A'))]
        stats_src_list = [{'stat_tbl_id': s, 'ext_api_id': self.ext_api_id,
                           'stat_api_id': self.stat_api_id[s]} for s in STATS]
        info = {s: {'intg_tbl_id': i, 'src_data_id': self.src_data_id[s]} for s, i in STATS.items()}
        result = db_processing.process_db_insertion(saved, {'ext_api_id': self.ext_api_id}, stats_src_list,
                                                    info, update_system_sync=False)
        self.assertEqual(result['failed'], [])
        self.assertIsNotNone(self._sync_time('ITEST_DT_A'))
        self.assertIsNone(self._sync_time())

    # --- (1) 정상 응답 2회 연속 ---------------------------------------------
    def test_same_day_rerun_keeps_single_copy(self):
        rows = _rows('ITEST_DT_A')
        for _ in range(2):
            result = self._run({'ITEST_DT_A': rows})
            self.assertEqual(result['failed'], [])
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(rows))
        self.assertEqual(self._count('stats_kosis_origin_data', 'ITEST_DT_A'), len(rows))
        self.assertEqual(self._count('stats_kosis_metadata_code', 'ITEST_DT_A'), 3)  # 메타 행 3개(항목 1 + 분류 2)

    def test_next_day_rerun_with_same_update_date_keeps_single_copy(self):
        """갱신일이 그대로인 통계를 다른 날 다시 적재해도 통합 테이블은 응답 행 수와 같다."""
        rows = _rows('ITEST_DT_A')
        self._run({'ITEST_DT_A': rows})
        self._age_rows('ITEST_DT_A')
        result = self._run({'ITEST_DT_A': rows})
        self.assertEqual(result['failed'], [])
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(rows))
        self.assertEqual(self._count('stats_kosis_origin_data', 'ITEST_DT_A'), len(rows))

    def test_new_update_date_replaces_previous_version(self):
        """갱신일이 바뀌면 이전 갱신일의 행은 남지 않는다(통계별 최신 1벌)."""
        self._run({'ITEST_DT_A': _rows('ITEST_DT_A')})
        self._age_rows('ITEST_DT_A')
        newer = _rows('ITEST_DT_A', years=(2021, 2022, 2023, 2024))
        result = self._run({'ITEST_DT_A': newer}, latest='2025-09-01')
        self.assertEqual(result['failed'], [])
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(newer))
        self.assertEqual(self._count('stats_kosis_metadata_code', 'ITEST_DT_A'), 3)  # 메타 행 3개(항목 1 + 분류 2)
        with self.engine.begin() as conn:
            dates = conn.execute(self.text(
                "SELECT DISTINCT src_latest_chn_dt::text FROM " + INTG_A + " WHERE src_data_id = :s"),
                {'s': self.src_data_id['ITEST_DT_A']}).scalars().all()
        self.assertEqual(dates, ['2025-09-01'])

    def test_success_updates_sync_times(self):
        result = self._run({s: _rows(s) for s in ('ITEST_DT_A', 'ITEST_DT_B')})
        self.assertEqual(sorted(result['succeeded']), ['ITEST_DT_A', 'ITEST_DT_B'])
        self.assertIsNotNone(self._sync_time('ITEST_DT_A'))
        self.assertIsNotNone(self._sync_time())

    # --- (2)(3) 오류·빈 응답 → 기존 행 보존 + 실패 집계 ----------------------
    def _assert_rejected_and_preserved(self, bad_response, expect_in_reason, **file_kwargs):
        """bad_response: ITEST_DT_A 의 data 응답. file_kwargs: latest/meta 파일 내용 지정(_file_info 참고).

        file_kwargs 는 이 실행의 두 통계(A·B) 모두에 적용된다. 그래서 file_kwargs 를 준 경우에는
        "다른 통계는 정상 적재" 확인을 하지 않고, A 의 실패 사유·기존 행 보존만 본다.
        """
        rows = _rows('ITEST_DT_A')
        self._run({'ITEST_DT_A': rows})
        self._age_rows('ITEST_DT_A')
        with self.engine.begin() as conn:
            conn.execute(self.text(
                "UPDATE sys_stats_src_api_info SET latest_sync_time = NULL WHERE ext_api_id = :e"),
                {'e': self.ext_api_id})
            conn.execute(self.text(
                "UPDATE sys_ext_api_info SET latest_sync_time = NULL WHERE ext_api_id = :e"),
                {'e': self.ext_api_id})

        if file_kwargs:
            result = self._run({'ITEST_DT_A': bad_response}, **file_kwargs)
        else:
            result = self._run({'ITEST_DT_A': bad_response, 'ITEST_DT_B': _rows('ITEST_DT_B')})

        # 실패 집계 + 사유
        self.assertEqual([f[0] for f in result['failed']], ['ITEST_DT_A'])
        self.assertIn(expect_in_reason, result['failed'][0][1])
        if not file_kwargs:
            self.assertEqual(result['succeeded'], ['ITEST_DT_B'])
        # 기존 행 보존 — 통합·원본 모두 그대로, 오류 응답은 원본에도 들어가지 않는다
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(rows))
        self.assertEqual(self._count('stats_kosis_origin_data', 'ITEST_DT_A'), len(rows))
        with self.engine.begin() as conn:
            blanks = conn.execute(self.text(
                "SELECT count(*) FROM stats_kosis_origin_data WHERE src_data_id = :s AND prd_de = ''"),
                {'s': self.src_data_id['ITEST_DT_A']}).scalar()
        self.assertEqual(blanks, 0)
        # 성공 표시 미갱신 — 실패한 통계의 동기화 시각, 시스템 전체 동기화 시각
        self.assertIsNone(self._sync_time('ITEST_DT_A'))
        self.assertIsNone(self._sync_time())
        if file_kwargs:
            return
        # 다른 통계는 정상 적재
        self.assertEqual(self._count(INTG_B, 'ITEST_DT_B'), len(_rows('ITEST_DT_B')))
        self.assertIsNotNone(self._sync_time('ITEST_DT_B'))

    def test_error_dict_response_preserves_existing_rows(self):
        self._assert_rejected_and_preserved(
            {'err': '20', 'errMsg': '필수요청변수값이 누락되었습니다.'}, 'err=20')

    def test_empty_list_response_preserves_existing_rows(self):
        self._assert_rejected_and_preserved([], '빈 리스트')

    def test_response_without_numeric_period_preserves_existing_rows(self):
        self._assert_rejected_and_preserved([{'PRD_DE': '', 'DT': '1', 'C1': 'A'}], '숫자 PRD_DE')

    def test_missing_update_date_is_rejected_with_explicit_reason(self):
        """latest 응답에 SendDe 가 없으면 DB 제약 위반 문구가 아니라 명시적 사유로 실패한다."""
        self._assert_rejected_and_preserved(_rows('ITEST_DT_A'), '갱신일을 확인하지 못함', latest=None)

    def test_failure_after_load_rolls_back_whole_statistic(self):
        """원본 교체·통합 교체 뒤 단계(메타 적재)가 실패하면 그 통계의 변경은 전부 되돌려진다."""
        rows = _rows('ITEST_DT_A')
        self._run({'ITEST_DT_A': rows})
        self._age_rows('ITEST_DT_A')
        bigger = _rows('ITEST_DT_A', years=(2019, 2020, 2021, 2022, 2023))
        result = self._run({'ITEST_DT_A': bigger}, meta='XML 이 아닌 본문')
        self.assertEqual([f[0] for f in result['failed']], ['ITEST_DT_A'])
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(rows))
        self.assertEqual(self._count('stats_kosis_origin_data', 'ITEST_DT_A'), len(rows))

    # --- (4) 이미 2벌인 상태 → 1벌로 수렴 -----------------------------------
    def test_already_duplicated_rows_converge_to_single_copy(self):
        rows = _rows('ITEST_DT_A')
        self._run({'ITEST_DT_A': rows})
        self._age_rows('ITEST_DT_A', days=10)
        sid = self.src_data_id['ITEST_DT_A']
        with self.engine.begin() as conn:
            # 통합·원본 테이블 모두 같은 내용을 한 벌 더 넣어 2벌 상태를 만든다.
            conn.execute(self.text(
                "INSERT INTO " + INTG_A + " (src_data_id, prd_de, c1, c2, c3, itm_id, unit_nm, dt,"
                " lst_chn_de, src_latest_chn_dt, created_at, created_by)"
                " SELECT src_data_id, prd_de, c1, c2, c3, itm_id, unit_nm, dt, lst_chn_de,"
                " src_latest_chn_dt, created_at, created_by FROM " + INTG_A + " WHERE src_data_id = :s"),
                {'s': sid})
            conn.execute(self.text(
                "INSERT INTO stats_kosis_origin_data (src_data_id, org_id, tbl_id, tbl_nm, c1, c1_obj_nm,"
                " c1_nm, itm_id, itm_nm, prd_se, prd_de, dt, stat_latest_chn_dt, created_at, created_by)"
                " SELECT src_data_id, org_id, tbl_id, tbl_nm, c1, c1_obj_nm, c1_nm, itm_id, itm_nm,"
                " prd_se, prd_de, dt, stat_latest_chn_dt, created_at, created_by"
                " FROM stats_kosis_origin_data WHERE src_data_id = :s"), {'s': sid})
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), 2 * len(rows))

        result = self._run({'ITEST_DT_A': rows})

        self.assertEqual(result['failed'], [])
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(rows))
        self.assertEqual(self._count('stats_kosis_origin_data', 'ITEST_DT_A'), len(rows))

    # --- (5) 통계 1건 수집 예외 → 나머지는 적재 ------------------------------
    def test_collect_exception_in_one_statistic_does_not_stop_others(self):
        import main as main_module

        existing = _rows('ITEST_DT_A')
        self._run({'ITEST_DT_A': existing})
        with self.engine.begin() as conn:
            conn.execute(self.text(
                "UPDATE sys_ext_api_info SET latest_sync_time = NULL WHERE ext_api_id = :e"),
                {'e': self.ext_api_id})

        def fake_save_single_file(args):
            _api_info, stats_src, _dirs, _data_info = args
            stat_tbl_id = stats_src['stat_tbl_id']
            if stat_tbl_id == 'ITEST_DT_A':
                raise RuntimeError('[ITEST_DT_A] save_single_file - 파일 저장 실패')
            return self._file_info(stat_tbl_id, _rows(stat_tbl_id))

        stats_src_list = [{'stat_tbl_id': s} for s in ('ITEST_DT_A', 'ITEST_DT_B')]
        collect_failed = []
        with patch.object(main_module, 'save_single_file', side_effect=fake_save_single_file):
            saved = main_module.save_all_files({}, stats_src_list, {}, {}, failed_out=collect_failed)

        self.assertEqual([f[0] for f in collect_failed], ['ITEST_DT_A'])
        self.assertEqual([s['stat_tbl_id'] for s in saved], ['ITEST_DT_B'])

        info = {s: {'intg_tbl_id': intg, 'src_data_id': self.src_data_id[s]} for s, intg in STATS.items()}
        result = db_processing.process_db_insertion(
            saved, {'ext_api_id': self.ext_api_id}, stats_src_list, info, collect_failed=collect_failed)

        self.assertEqual(result['succeeded'], ['ITEST_DT_B'])
        self.assertEqual(self._count(INTG_B, 'ITEST_DT_B'), len(_rows('ITEST_DT_B')))
        # 수집에 실패한 통계의 기존 행은 그대로, 시스템 전체 동기화 시각은 갱신되지 않는다
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(existing))
        self.assertIsNone(self._sync_time())

    # --- 문자형 dt 분기 ------------------------------------------------------
    def test_text_dt_table_branch_keeps_single_copy_and_raw_value(self):
        rows = _rows('ITEST_DT_C', dt='-')
        for _ in range(2):
            result = self._run({'ITEST_DT_C': rows})
            self.assertEqual(result['failed'], [])
        self.assertEqual(self._count(INTG_COST, 'ITEST_DT_C'), len(rows))
        with self.engine.begin() as conn:
            values = conn.execute(self.text(
                "SELECT DISTINCT dt FROM " + INTG_COST + " WHERE src_data_id = :s"),
                {'s': self.src_data_id['ITEST_DT_C']}).scalars().all()
        self.assertEqual(values, ['-'])

    # --- 통합 테이블에 UNIQUE 인덱스가 있는 경우 -----------------------------
    def test_rerun_works_with_unique_index_on_integration_table(self):
        """01 의 통합 테이블 UNIQUE 인덱스(자연키)와 같은 정의를 건 상태에서도 재실행이 된다."""
        with self.engine.begin() as conn:
            conn.execute(self.text(
                "CREATE UNIQUE INDEX uidx_itest_intg_a_key ON " + INTG_A + " USING btree"
                " (src_data_id, prd_de, c1, COALESCE(c2, ''), COALESCE(c3, ''), itm_id)"))
        rows = _rows('ITEST_DT_A')
        for _ in range(2):
            result = self._run({'ITEST_DT_A': rows})
            self.assertEqual(result['failed'], [])
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(rows))
        # 키에 자료갱신일이 없으므로, 갱신일이 바뀐 재수집도 "전부 지우고 다시 넣기"로 통과해야 한다
        # (기존 행을 남긴 채 새 갱신일 행을 넣는 방식이면 여기서 UNIQUE 위반이 난다).
        result = self._run({'ITEST_DT_A': rows}, latest='2025-09-01')
        self.assertEqual(result['failed'], [])
        self.assertEqual(self._count(INTG_A, 'ITEST_DT_A'), len(rows))


if __name__ == '__main__':
    unittest.main()
