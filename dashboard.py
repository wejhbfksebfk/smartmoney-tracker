"""
┌─────────────────────────────────────────────────────────────┐
│          스마트 머니 트래커 v2.0                             │
│  한국투자증권 KIS API 기반 실시간 수급 포착 시스템           │
│                                                             │
│  [핵심 필터]                                                 │
│  ① 매수강도 급등    직전 스냅샷 대비 순매수 증가율 포착      │
│  ② 동반매수 가중치  외국인 + 프로그램 동시 유입 탐지         │
│  ③ 바닥권 반등      52주 저점 근접 + 변동성 안정 + 거래량    │
│  ④ 스마트머니 점수  위 3가지 조합 종합 점수                  │
└─────────────────────────────────────────────────────────────┘

[사용법]
  pip install requests
  python smart_money_tracker.py

[인증]
  최초 실행 시 APP KEY / APP SECRET 입력 → config.json 자동 저장
  이후 실행부터 자동 로드 (삭제하면 재입력)
"""

import requests
import json
import time
import csv
import sys
import math
import collections
from datetime import datetime
from pathlib import Path


# ══════════════════════════════════════════════════════════════
#  ▶ 상수 / 튜닝 파라미터
# ══════════════════════════════════════════════════════════════

CONFIG_FILE = Path("config.json")
TOKEN_FILE  = Path("token_cache.json")

BASE_URL_REAL  = "https://openapi.koreainvestment.com:9443"
BASE_URL_PAPER = "https://openapivts.koreainvestment.com:29443"

# ── API 호출 제한 ────────────────────────────────────────────
API_CALL_INTERVAL = 0.08     # 초당 ~12건 (한투 제한 20건의 60% 수준)
TOP_N             = 30       # 기본 순매수 상위 N개

# ── 스마트머니 필터 임계값 (필요 시 직접 수정) ───────────────
MOMENTUM_SURGE_RATIO  = 1.5  # 직전 스냅샷 대비 순매수 증가율 ≥ 150%
DUAL_BUY_BONUS        = 2.0  # 외국인+프로그램 동시 유입 시 가중치 점수
BOTTOM_MAX_PRDY_CTRT  = 3.0  # 당일 등락률 절댓값 상한 (%) → 이미 급등 제외
BOTTOM_FROM_52W_LOW   = 30.0 # 52주 저점 대비 현재가 상승폭 상한 (%) → 바닥권
BOTTOM_MIN_VOL_RATIO  = 1.5  # 당일거래량 / 평균거래량 최소 배수 → 수급 유입 확인
SMART_SCORE_THRESHOLD = 5.0  # 이 점수 미만은 스마트머니 목록에서 제외


# ══════════════════════════════════════════════════════════════
#  ▶ 1. 인증 / 토큰
# ══════════════════════════════════════════════════════════════

def load_config() -> dict:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_config(cfg: dict):
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    print(f"  ✅ 설정 저장 완료 → {CONFIG_FILE}\n")


def get_credentials() -> tuple[str, str, bool]:
    """config.json 에서 로드, 없으면 대화식 입력 후 저장"""
    cfg = load_config()
    if cfg.get("app_key") and cfg.get("app_secret"):
        mode = "실전" if cfg.get("real", True) else "모의"
        print(f"  🔑 저장된 인증 정보 로드 ({mode}투자)\n")
        return cfg["app_key"], cfg["app_secret"], cfg.get("real", True)

    print("=" * 58)
    print("  한국투자증권 KIS API 인증 정보 설정")
    print("  발급처 : https://apiportal.koreainvestment.com")
    print("=" * 58)
    app_key    = input("  APP KEY    : ").strip()
    app_secret = input("  APP SECRET : ").strip()
    real_yn    = input("  실전투자?  [Y/n] : ").strip().lower()
    is_real    = (real_yn != "n")

    save_config({"app_key": app_key, "app_secret": app_secret, "real": is_real})
    return app_key, app_secret, is_real


def get_access_token(app_key: str, app_secret: str, base_url: str) -> str:
    """OAuth2 토큰 발급 (token_cache.json 에 23.9시간 캐시)"""
    if TOKEN_FILE.exists():
        cache = json.loads(TOKEN_FILE.read_text())
        if time.time() - cache.get("issued_at", 0) < 86000:
            print("  🔄 캐시 토큰 사용\n")
            return cache["access_token"]

    print("  🔐 액세스 토큰 발급 중...")
    resp = requests.post(
        f"{base_url}/oauth2/tokenP",
        json={"grant_type": "client_credentials",
              "appkey": app_key, "appsecret": app_secret},
        timeout=10,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError(f"토큰 발급 실패: {resp.json()}")

    TOKEN_FILE.write_text(
        json.dumps({"access_token": token, "issued_at": time.time()})
    )
    print("  ✅ 토큰 발급 완료\n")
    return token


# ══════════════════════════════════════════════════════════════
#  ▶ 2. 공통 HTTP 래퍼
# ══════════════════════════════════════════════════════════════

def make_headers(token: str, app_key: str, app_secret: str, tr_id: str) -> dict:
    return {
        "Content-Type":  "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey":         app_key,
        "appsecret":      app_secret,
        "tr_id":          tr_id,
        "custtype":       "P",
    }


def safe_get(url: str, headers: dict, params: dict,
             max_retry: int = 3) -> dict | None:
    """
    GET 래퍼
    - API_CALL_INTERVAL 호출 간격 보장
    - 429 / 5xx → 지수 백오프 재시도 (최대 max_retry 회)
    """
    for attempt in range(1, max_retry + 1):
        time.sleep(API_CALL_INTERVAL)
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)

            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"    ⚠️  429 호출한도 초과 → {wait}s 대기 [{attempt}/{max_retry}]")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()

            if data.get("rt_cd") != "0":
                print(f"    ⚠️  API 오류 rt_cd={data.get('rt_cd')} "
                      f"msg={data.get('msg1','')}")
                return None

            return data

        except requests.exceptions.Timeout:
            print(f"    ⚠️  Timeout → 재시도 [{attempt}/{max_retry}]")
            time.sleep(1)
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️  요청 오류: {e}")
            time.sleep(1)

    print("    ❌ 최대 재시도 초과 → None 반환")
    return None


# ══════════════════════════════════════════════════════════════
#  ▶ 3. KIS API 데이터 수집
# ══════════════════════════════════════════════════════════════

def fetch_investor_top(token, app_key, app_secret, base_url,
                       investor: str = "FRG") -> list[dict]:
    """
    외국인/기관 순매수 상위 (FHPST02060000)
    KOSPI(J) + KOSDAQ(Q) 각 50개 수집 후 합산
    investor : 'FRG'=외국인  'ORG'=기관합계
    """
    url      = (f"{base_url}/uapi/domestic-stock/v1"
                "/quotations/foreign-institution-total")
    tr_id    = "FHPST02060000"
    inv_code = "1" if investor == "FRG" else "2"
    results  = []

    for mkt in ["J", "Q"]:
        params = {
            "fid_cond_mrkt_div_code": mkt,
            "fid_cond_scr_div_code":  "16448",
            "fid_input_iscd":         "0000",
            "fid_div_cls_code":       "1",      # 순매수 상위
            "fid_rank_sort_cls_code": "0",      # 상위순
            "fid_input_cnt_1":        "50",
            "fid_etc_cls_code":       inv_code,
        }
        data = safe_get(url,
                        make_headers(token, app_key, app_secret, tr_id),
                        params)
        if data and data.get("output"):
            for row in data["output"]:
                row["_market"]   = "KOSPI" if mkt == "J" else "KOSDAQ"
                row["_investor"] = "외국인" if investor == "FRG" else "기관"
                results.append(row)
        time.sleep(0.15)

    return results


def fetch_program_top(token, app_key, app_secret, base_url) -> list[dict]:
    """
    프로그램 매매 순매수 상위 (FHPST01710000)
    차익 + 비차익 합산 기준
    """
    url   = (f"{base_url}/uapi/domestic-stock/v1"
             "/quotations/program-trade-by-stock")
    tr_id = "FHPST01710000"
    results = []

    for mkt in ["J", "Q"]:
        params = {
            "fid_cond_mrkt_div_code": mkt,
            "fid_cond_scr_div_code":  "20171",
            "fid_input_iscd":         "0000",
            "fid_div_cls_code":       "0",
            "fid_rank_sort_cls_code": "0",
            "fid_input_cnt_1":        "50",
        }
        data = safe_get(url,
                        make_headers(token, app_key, app_secret, tr_id),
                        params)
        if data and data.get("output"):
            for row in data["output"]:
                row["_market"] = "KOSPI" if mkt == "J" else "KOSDAQ"
                results.append(row)
        time.sleep(0.15)

    return results


def fetch_stock_detail(token, app_key, app_secret, base_url,
                       iscd: str) -> dict | None:
    """
    종목 기본 시세 (FHKST01010100)
    → 52주 고/저가, 당일 누적거래량, 평균거래량 포함
    바닥권 필터에서 사용
    """
    url   = (f"{base_url}/uapi/domestic-stock/v1"
             "/quotations/inquire-price")
    tr_id = "FHKST01010100"
    params = {
        "fid_cond_mrkt_div_code": "J",
        "fid_input_iscd":         iscd,
    }
    data = safe_get(url,
                    make_headers(token, app_key, app_secret, tr_id),
                    params)
    if data and data.get("output"):
        return data["output"]
    return None


# ══════════════════════════════════════════════════════════════
#  ▶ 4. 매수강도 히스토리 (스냅샷 간 증가율 계산)
# ══════════════════════════════════════════════════════════════
#
#  실행할 때마다 종목별 순매수수량을 deque 에 누적.
#  2회차부터 "직전 스냅샷 대비 증가율" 계산 가능.
#  갱신 주기를 10분으로 설정하면 "10분 대비" 강도 측정이 됨.
#
_ntby_history: dict[str, collections.deque] = {}
_HISTORY_LEN = 3   # 최근 3스냅샷 보관


def update_history(iscd: str, ntby_qty: int) -> None:
    if iscd not in _ntby_history:
        _ntby_history[iscd] = collections.deque(maxlen=_HISTORY_LEN)
    _ntby_history[iscd].append(ntby_qty)


def get_surge_ratio(iscd: str) -> float:
    """
    직전 스냅샷 대비 현재 순매수 증가율
      = 현재값 / max(|이전값|, 1)
    데이터 부족 시 1.0 (중립) 반환
    """
    hist = _ntby_history.get(iscd)
    if not hist or len(hist) < 2:
        return 1.0
    prev, curr = hist[-2], hist[-1]
    return curr / max(abs(prev), 1)


# ══════════════════════════════════════════════════════════════
#  ▶ 5. 유틸리티
# ══════════════════════════════════════════════════════════════

def safe_float(val, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return default


def safe_int(val, default: int = 0) -> int:
    try:
        return int(str(val).replace(",", ""))
    except (TypeError, ValueError):
        return default


# ══════════════════════════════════════════════════════════════
#  ▶ 6. 스마트머니 점수 계산
# ══════════════════════════════════════════════════════════════

def compute_smart_score(
    iscd:        str,
    ntby_qty:    int,
    prdy_ctrt:   float,      # 당일 등락률 (%)
    detail:      dict | None,
    is_dual_buy: bool,       # 외국인 + 프로그램 동시 순매수 여부
) -> tuple[float, dict]:
    """
    스마트머니 종합 점수 계산  (최대 약 20점)

    ┌──────────────────────────────────────────────────┐
    │ A. 매수강도 급등   0~6점  직전 스냅샷 대비 증가율 │
    │ B. 동반매수 가중   0~2점  외국인+프로그램 동시     │
    │ C. 바닥권 반등     0~6점  52주저점 근접 + 거래량   │
    │ D. 수급 규모       0~4점  절대 순매수수량           │
    └──────────────────────────────────────────────────┘

    반환: (total_score, breakdown_dict)
    필터 탈락 시 score=0, breakdown["필터통과"]=False
    """
    bd = {
        "매수강도급등": 0.0,
        "동반매수가중": 0.0,
        "바닥권반등":   0.0,
        "수급규모":     0.0,
        "필터통과":     True,
        "필터사유":     "",
    }

    # ── A. 매수강도 급등 점수 ────────────────────────────────
    surge = get_surge_ratio(iscd)
    if surge >= MOMENTUM_SURGE_RATIO:
        # surge 1.5→2.4pt  2.0→4.0pt  3.0→6.0pt (log2 스케일)
        score_a = min(6.0, math.log2(max(surge, 1.001)) * 4.0)
    else:
        score_a = 0.0
    bd["매수강도급등"] = round(score_a, 2)

    # ── B. 동반매수 가중 ─────────────────────────────────────
    score_b = DUAL_BUY_BONUS if is_dual_buy else 0.0
    bd["동반매수가중"] = score_b

    # ── C. 바닥권 반등 필터 + 점수 ───────────────────────────
    score_c = 0.0
    if detail:
        w52_hgpr  = safe_float(detail.get("w52_hgpr",  0))
        w52_lwpr  = safe_float(detail.get("w52_lwpr",  0))
        stck_prpr = safe_float(detail.get("stck_prpr", 0))
        acml_vol  = safe_int(detail.get("acml_vol",    0))   # 당일 누적거래량
        avrg_vol  = safe_int(detail.get("avrg_vol",    0))   # 평균 거래량

        # [필터 1] 당일 급등·급락 종목 제외
        if abs(prdy_ctrt) > BOTTOM_MAX_PRDY_CTRT:
            bd["필터통과"] = False
            bd["필터사유"] = (
                f"당일 등락률 {prdy_ctrt:+.1f}% "
                f"(허용 ±{BOTTOM_MAX_PRDY_CTRT}%)"
            )
            return 0.0, bd

        # [필터 2] 52주 저점 대비 상승폭 → 바닥권 여부
        if w52_lwpr > 0 and stck_prpr > 0:
            from_low_pct = (stck_prpr - w52_lwpr) / w52_lwpr * 100
            if from_low_pct > BOTTOM_FROM_52W_LOW:
                bd["필터통과"] = False
                bd["필터사유"] = (
                    f"52주저점 대비 +{from_low_pct:.1f}% "
                    f"(허용 {BOTTOM_FROM_52W_LOW}% 이내)"
                )
                return 0.0, bd

            # 바닥 근접도 점수: 저점에 가까울수록 높음 (최대 3점)
            proximity = 1.0 - (from_low_pct / BOTTOM_FROM_52W_LOW)
            score_c  += proximity * 3.0

        # [필터 3] 거래량 폭발 확인
        if avrg_vol > 0:
            vol_ratio = acml_vol / avrg_vol
            if vol_ratio < BOTTOM_MIN_VOL_RATIO:
                bd["필터통과"] = False
                bd["필터사유"] = (
                    f"거래량 비율 {vol_ratio:.2f}x "
                    f"(최소 {BOTTOM_MIN_VOL_RATIO}x)"
                )
                return 0.0, bd
            # 1.5x→1pt  3.0x→3pt (선형 캡)
            score_c += min(3.0, (vol_ratio - 1.0) * 1.5)

    bd["바닥권반등"] = round(score_c, 2)

    # ── D. 수급 규모 점수 ────────────────────────────────────
    # 10만주 이상부터 점수, 100만주에서 ~4점
    if ntby_qty >= 100_000:
        score_d = min(4.0, math.log10(ntby_qty / 100_000 + 1) * 4.0)
    else:
        score_d = 0.0
    bd["수급규모"] = round(score_d, 2)

    total = score_a + score_b + score_c + score_d
    return round(total, 2), bd


# ══════════════════════════════════════════════════════════════
#  ▶ 7. 메인 파이프라인
# ══════════════════════════════════════════════════════════════

def run_pipeline(token: str, app_key: str, app_secret: str,
                 base_url: str) -> tuple[list[dict], list[dict]]:
    """
    전체 데이터 수집 → 정제 → 필터 적용
    반환: (top30_list, smart_money_list)
    """

    # ── 7-1. 수급 데이터 수집 ─────────────────────────────────
    print("  📡 [1/3] 외국인 순매수 수집 중...")
    frg_raw = fetch_investor_top(token, app_key, app_secret, base_url, "FRG")

    print("  📡 [2/3] 기관  순매수 수집 중...")
    org_raw = fetch_investor_top(token, app_key, app_secret, base_url, "ORG")

    print("  📡 [3/3] 프로그램 매매 수집 중...")
    prg_raw = fetch_program_top(token, app_key, app_secret, base_url)

    # ── 7-2. 프로그램 순매수 종목 집합 ───────────────────────
    prg_buy_set: set[str] = set()
    for row in prg_raw:
        amt = safe_int(row.get("pgm_ntby_tr_pbmn",
                      row.get("ntby_tr_pbmn", 0)))
        if amt > 0:
            iscd = row.get("mksc_shrn_iscd", "").strip()
            if iscd:
                prg_buy_set.add(iscd)

    # ── 7-3. 외국인/기관 raw → 정규화 딕셔너리 ──────────────
    def normalize(raw: list[dict], label: str) -> list[dict]:
        out = []
        for row in raw:
            iscd = row.get("mksc_shrn_iscd", "").strip()
            if not iscd:
                continue
            ntby_qty = safe_int(row.get("frgn_ntby_qty",
                                row.get("orgn_ntby_qty", 0)))
            ntby_amt = safe_int(row.get("frgn_ntby_tr_pbmn",
                                row.get("orgn_ntby_tr_pbmn", 0)))
            out.append({
                "종목코드":         iscd,
                "종목명":           row.get("hts_kor_isnm", "").strip(),
                "시장":             row.get("_market", ""),
                "투자자":           label,
                "현재가":           safe_int(row.get("stck_prpr", 0)),
                "등락률":           safe_float(row.get("prdy_ctrt", 0)),
                "순매수수량":       ntby_qty,
                "순매수금액(백만)": round(ntby_amt / 1_000_000, 1),
            })
            update_history(iscd, ntby_qty)   # 강도 히스토리 업데이트
        return out

    frg_list = normalize(frg_raw, "외국인")
    org_list = normalize(org_raw, "기관")

    # ── 7-4. 상위 30 추출 (외국인+기관 합산, 중복 제거) ──────
    combined = frg_list + org_list
    combined.sort(key=lambda x: x["순매수수량"], reverse=True)

    seen: set[str] = set()
    top30: list[dict] = []
    for row in combined:
        if row["종목코드"] not in seen:
            seen.add(row["종목코드"])
            top30.append(row)
        if len(top30) >= TOP_N:
            break

    # ── 7-5. 스마트머니 필터 ─────────────────────────────────
    print(f"\n  🔬 스마트머니 필터 적용 ({len(top30)}종목)...")
    smart: list[dict] = []

    for i, row in enumerate(top30, 1):
        iscd     = row["종목코드"]
        is_dual  = iscd in prg_buy_set
        prdr     = row["등락률"]
        ntby_qty = row["순매수수량"]
        tag      = f"[{i:02d}/{len(top30)}]"

        # 종목 상세 조회 (52주 고/저, 거래량)
        detail = fetch_stock_detail(token, app_key, app_secret, base_url, iscd)
        time.sleep(0.05)

        score, bd = compute_smart_score(iscd, ntby_qty, prdr, detail, is_dual)

        # 필터 탈락
        if not bd["필터통과"]:
            print(f"    {tag} ✗ {row['종목명']:<12}  {bd['필터사유']}")
            continue

        # 점수 미달
        if score < SMART_SCORE_THRESHOLD:
            print(f"    {tag} △ {row['종목명']:<12}  "
                  f"점수 {score:.1f}pt (기준 {SMART_SCORE_THRESHOLD}pt 미달)")
            continue

        # ★ 포착
        dual_tag = "  🔥동반매수" if is_dual else ""
        print(f"    {tag} ★ {row['종목명']:<12}  "
              f"★{score:.1f}pt  "
              f"급등:{bd['매수강도급등']:.1f} "
              f"동반:{bd['동반매수가중']:.1f} "
              f"바닥:{bd['바닥권반등']:.1f} "
              f"규모:{bd['수급규모']:.1f}"
              f"{dual_tag}")

        smart.append({**row,
                      "프로그램동반":  "O" if is_dual else "-",
                      "스마트점수":    score,
                      "급등점수":      bd["매수강도급등"],
                      "동반점수":      bd["동반매수가중"],
                      "바닥점수":      bd["바닥권반등"],
                      "규모점수":      bd["수급규모"]})

    smart.sort(key=lambda x: x["스마트점수"], reverse=True)
    return top30, smart


# ══════════════════════════════════════════════════════════════
#  ▶ 8. 출력
# ══════════════════════════════════════════════════════════════

W = 82   # 테이블 너비


def _divider(char="═"):
    print(char * W)


def _title(text: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _divider()
    print(f"  {text}   [{now}]")
    _divider()


def print_top30(data: list[dict]):
    _title("📊 외국인/기관 순매수 상위 30")
    print(f"  {'#':>3}  {'종목명':<14}{'코드':>7}  {'시장':>6}  "
          f"{'현재가':>9}  {'등락':>7}  {'순매수(주)':>11}  {'금액(백만)':>10}")
    _divider("─")
    for i, r in enumerate(data, 1):
        chg = f"{r['등락률']:+.2f}%"
        print(f"  {i:>3}  {r['종목명']:<14}{r['종목코드']:>7}  {r['시장']:>6}  "
              f"{r['현재가']:>9,}  {chg:>7}  {r['순매수수량']:>11,}  "
              f"{r['순매수금액(백만)']:>10,.1f}")
    _divider()


def print_smart(data: list[dict]):
    _title(f"🤖 스마트머니 포착 종목  ({len(data)}개)")

    if not data:
        print("  현재 장 조건에 부합하는 스마트머니 종목이 없습니다.")
        _divider()
        return

    print(f"  {'#':>3}  {'종목명':<14}{'코드':>7}  {'시장':>6}  "
          f"{'현재가':>9}  {'등락':>7}  "
          f"{'급등':>5}  {'동반':>5}  {'바닥':>5}  {'규모':>5}  "
          f"{'★점수':>6}  {'동반매수':>5}")
    _divider("─")
    for i, r in enumerate(data, 1):
        chg  = f"{r['등락률']:+.2f}%"
        dual = "🔥" if r["프로그램동반"] == "O" else "  -"
        print(f"  {i:>3}  {r['종목명']:<14}{r['종목코드']:>7}  {r['시장']:>6}  "
              f"{r['현재가']:>9,}  {chg:>7}  "
              f"{r['급등점수']:>5.1f}  {r['동반점수']:>5.1f}  "
              f"{r['바닥점수']:>5.1f}  {r['규모점수']:>5.1f}  "
              f"{r['스마트점수']:>6.1f}  {dual:>5}")
    _divider()
    print()
    print("  [점수 가이드]")
    print(f"   급등({MOMENTUM_SURGE_RATIO}배 이상 증가 시 발동)  "
          f"동반(외국인+프로그램 동시 유입)  "
          f"바닥(52주저점 {BOTTOM_FROM_52W_LOW}% 이내 + 거래량 {BOTTOM_MIN_VOL_RATIO}x 이상)")
    print(f"   ★점수 = 급등+동반+바닥+규모 합산  │  "
          f"포착 기준 ≥ {SMART_SCORE_THRESHOLD}점")
    print()


# ══════════════════════════════════════════════════════════════
#  ▶ 9. CSV 저장
# ══════════════════════════════════════════════════════════════

def save_csv(data: list[dict], label: str):
    if not data:
        print(f"  ℹ️  {label} 데이터 없음 → 저장 생략")
        return
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    path = f"smart_money_{label}_{ts}.csv"
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  💾 저장 완료 → {path}")


# ══════════════════════════════════════════════════════════════
#  ▶ 10. 진입점
# ══════════════════════════════════════════════════════════════

def main():
    print()
    print("  " + "★" * 24)
    print("    스마트 머니 트래커 v2.0")
    print("    외국인 / 기관 / 프로그램 수급 포착")
    print("  " + "★" * 24)
    print()

    # 인증
    app_key, app_secret, is_real = get_credentials()
    base_url = BASE_URL_REAL if is_real else BASE_URL_PAPER

    # 토큰
    try:
        token = get_access_token(app_key, app_secret, base_url)
    except Exception as e:
        print(f"  ❌ 토큰 발급 실패: {e}")
        print("     APP KEY / SECRET 확인 후 config.json 삭제 → 재실행")
        sys.exit(1)

    # 갱신 주기
    raw_iv   = input("  자동 갱신 주기 (초, 0=단발 실행) [기본: 0] : ").strip()
    interval = int(raw_iv) if raw_iv.isdigit() else 0

    if interval > 0:
        print(f"\n  ℹ️  매수강도 급등 점수는 2회차부터 의미 있습니다.")
        print(f"     (직전 스냅샷과 비교 → 갱신 주기를 10분으로 설정하면 '10분 강도' 측정)")

    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"\n  {'─'*76}")
            print(f"  🔍 [{iteration}회차] 수집 시작  "
                  f"({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
            print(f"  {'─'*76}")

            top30, smart = run_pipeline(token, app_key, app_secret, base_url)

            print()
            print_top30(top30)
            print()
            print_smart(smart)

            save_yn = input("  CSV 저장? [y/N] : ").strip().lower()
            if save_yn == "y":
                save_csv(top30, "top30")
                save_csv(smart, "smart_money")

            if interval == 0:
                break

            print(f"\n  ⏳ {interval}초 후 재조회...  (Ctrl+C 로 종료)")
            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n  👋 종료합니다.")


if __name__ == "__main__":
    main()
