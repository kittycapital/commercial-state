#!/usr/bin/env python3
"""
네이버 부동산 상업용 매물 수집기 (로컬 실행용)
상가(SG) · 사무실(SMS) · 건물(DDDGG) · 공장/창고(JWJT) · 토지(LND)

사용법:
  python collect_commercial.py                          # 전체 수집
  python collect_commercial.py --sido 서울               # 서울만
  python collect_commercial.py --sido 서울 --gu 강남구    # 서울 강남구만
  python collect_commercial.py --trade B2                # 월세만
  python collect_commercial.py --push                    # 수집 후 자동 git push

※ 네이버 부동산 API는 한국 IP에서만 접근 가능 → 로컬(PC)에서 직접 실행
"""

import requests
import json
import time
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────

# 매물유형: SG(상가), SMS(사무실), DDDGG(건물), JWJT(공장/창고), LND(토지)
DEFAULT_TYPES = "SG:SMS:DDDGG:JWJT:LND"

# 거래유형: A1(매매), B1(전세), B2(월세)
DEFAULT_TRADE = "A1"

# API 요청 간격 (초) — 429 방지
REQUEST_DELAY = 3.0

# 429 에러 시 대기 시간 (초)
RETRY_DELAY = 30

# 최대 재시도 횟수
MAX_RETRIES = 3

# ──────────────────────────────────────────────
# 세션 생성 (쿠키 유지 = 차단 방지)
# ──────────────────────────────────────────────

def create_session():
    """네이버 부동산 페이지 방문 → 쿠키 획득 → 세션 리턴"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ko-KR,ko;q=0.9",
    })

    # 먼저 메인 페이지 방문해서 쿠키 받기
    print("🌐 네이버 부동산 접속 중 (쿠키 획득)...", end=" ")
    try:
        res = session.get("https://m.land.naver.com/", timeout=10)
        print(f"✅ 상태: {res.status_code}")
    except Exception as e:
        print(f"⚠ 접속 실패: {e}")
        print("   → 인터넷 연결 또는 한국 IP 확인 필요")
        return None

    # API 호출용 헤더 추가
    session.headers.update({
        "Referer": "https://m.land.naver.com/",
        "Accept": "*/*",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    })

    return session


# ──────────────────────────────────────────────
# 지역 좌표
# ──────────────────────────────────────────────

REGIONS = {
    "서울": {
        "강남구": {"lat": 37.5172, "lon": 127.0473, "btm": 37.4954, "top": 37.5390, "lft": 127.0170, "rgt": 127.0776},
        "강동구": {"lat": 37.5301, "lon": 127.1238, "btm": 37.5100, "top": 37.5502, "lft": 127.1036, "rgt": 127.1440},
        "강북구": {"lat": 37.6396, "lon": 127.0255, "btm": 37.6195, "top": 37.6597, "lft": 127.0053, "rgt": 127.0457},
        "강서구": {"lat": 37.5510, "lon": 126.8495, "btm": 37.5309, "top": 37.5711, "lft": 126.8200, "rgt": 126.8790},
        "관악구": {"lat": 37.4784, "lon": 126.9516, "btm": 37.4583, "top": 37.4985, "lft": 126.9315, "rgt": 126.9717},
        "광진구": {"lat": 37.5385, "lon": 127.0823, "btm": 37.5184, "top": 37.5586, "lft": 127.0622, "rgt": 127.1024},
        "구로구": {"lat": 37.4954, "lon": 126.8874, "btm": 37.4753, "top": 37.5155, "lft": 126.8573, "rgt": 126.9175},
        "금천구": {"lat": 37.4568, "lon": 126.8955, "btm": 37.4367, "top": 37.4769, "lft": 126.8754, "rgt": 126.9156},
        "노원구": {"lat": 37.6542, "lon": 127.0568, "btm": 37.6250, "top": 37.6834, "lft": 127.0367, "rgt": 127.0769},
        "도봉구": {"lat": 37.6688, "lon": 127.0471, "btm": 37.6400, "top": 37.6976, "lft": 127.0200, "rgt": 127.0742},
        "동대문구": {"lat": 37.5744, "lon": 127.0396, "btm": 37.5543, "top": 37.5945, "lft": 127.0195, "rgt": 127.0597},
        "동작구": {"lat": 37.5124, "lon": 126.9395, "btm": 37.4923, "top": 37.5325, "lft": 126.9194, "rgt": 126.9596},
        "마포구": {"lat": 37.5663, "lon": 126.9014, "btm": 37.5400, "top": 37.5926, "lft": 126.8813, "rgt": 126.9215},
        "서대문구": {"lat": 37.5791, "lon": 126.9368, "btm": 37.5590, "top": 37.5992, "lft": 126.9100, "rgt": 126.9636},
        "서초구": {"lat": 37.4837, "lon": 127.0324, "btm": 37.4500, "top": 37.5174, "lft": 127.0000, "rgt": 127.0648},
        "성동구": {"lat": 37.5634, "lon": 127.0370, "btm": 37.5433, "top": 37.5835, "lft": 127.0100, "rgt": 127.0640},
        "성북구": {"lat": 37.5894, "lon": 127.0167, "btm": 37.5693, "top": 37.6095, "lft": 126.9966, "rgt": 127.0368},
        "송파구": {"lat": 37.5145, "lon": 127.1066, "btm": 37.4900, "top": 37.5390, "lft": 127.0765, "rgt": 127.1367},
        "양천구": {"lat": 37.5170, "lon": 126.8665, "btm": 37.4969, "top": 37.5371, "lft": 126.8464, "rgt": 126.8866},
        "영등포구": {"lat": 37.5264, "lon": 126.8963, "btm": 37.5063, "top": 37.5465, "lft": 126.8762, "rgt": 126.9164},
        "용산구": {"lat": 37.5324, "lon": 126.9907, "btm": 37.5123, "top": 37.5525, "lft": 126.9706, "rgt": 127.0108},
        "은평구": {"lat": 37.6027, "lon": 126.9291, "btm": 37.5750, "top": 37.6304, "lft": 126.9000, "rgt": 126.9582},
        "종로구": {"lat": 37.5735, "lon": 126.9790, "btm": 37.5534, "top": 37.5936, "lft": 126.9589, "rgt": 126.9991},
        "중구": {"lat": 37.5641, "lon": 126.9979, "btm": 37.5440, "top": 37.5842, "lft": 126.9778, "rgt": 127.0180},
        "중랑구": {"lat": 37.6063, "lon": 127.0928, "btm": 37.5862, "top": 37.6264, "lft": 127.0727, "rgt": 127.1129},
    },
    "경기": {
        "수원시": {"lat": 37.2636, "lon": 127.0286, "btm": 37.2300, "top": 37.2972, "lft": 126.9600, "rgt": 127.0972},
        "성남시": {"lat": 37.4201, "lon": 127.1265, "btm": 37.3800, "top": 37.4602, "lft": 127.0864, "rgt": 127.1666},
        "용인시": {"lat": 37.2410, "lon": 127.1775, "btm": 37.2009, "top": 37.2811, "lft": 127.1374, "rgt": 127.2176},
        "고양시": {"lat": 37.6584, "lon": 126.8320, "btm": 37.6200, "top": 37.6968, "lft": 126.7800, "rgt": 126.8840},
        "부천시": {"lat": 37.5034, "lon": 126.7660, "btm": 37.4833, "top": 37.5235, "lft": 126.7459, "rgt": 126.7861},
        "안양시": {"lat": 37.3943, "lon": 126.9568, "btm": 37.3742, "top": 37.4144, "lft": 126.9267, "rgt": 126.9869},
        "안산시": {"lat": 37.3219, "lon": 126.8309, "btm": 37.2800, "top": 37.3638, "lft": 126.7800, "rgt": 126.8818},
        "화성시": {"lat": 37.1995, "lon": 126.8312, "btm": 37.1500, "top": 37.2490, "lft": 126.7700, "rgt": 126.8924},
        "평택시": {"lat": 36.9921, "lon": 127.1127, "btm": 36.9500, "top": 37.0342, "lft": 127.0600, "rgt": 127.1654},
        "시흥시": {"lat": 37.3800, "lon": 126.8030, "btm": 37.3400, "top": 37.4200, "lft": 126.7600, "rgt": 126.8460},
        "파주시": {"lat": 37.7599, "lon": 126.7800, "btm": 37.7100, "top": 37.8098, "lft": 126.7200, "rgt": 126.8400},
        "김포시": {"lat": 37.6154, "lon": 126.7156, "btm": 37.5753, "top": 37.6555, "lft": 126.6700, "rgt": 126.7612},
        "광명시": {"lat": 37.4786, "lon": 126.8644, "btm": 37.4585, "top": 37.4987, "lft": 126.8443, "rgt": 126.8845},
        "광주시": {"lat": 37.4294, "lon": 127.2551, "btm": 37.3893, "top": 37.4695, "lft": 127.2000, "rgt": 127.3102},
        "하남시": {"lat": 37.5393, "lon": 127.2146, "btm": 37.5100, "top": 37.5686, "lft": 127.1800, "rgt": 127.2492},
        "군포시": {"lat": 37.3616, "lon": 126.9352, "btm": 37.3415, "top": 37.3817, "lft": 126.9151, "rgt": 126.9553},
        "의왕시": {"lat": 37.3449, "lon": 126.9685, "btm": 37.3248, "top": 37.3650, "lft": 126.9484, "rgt": 126.9886},
        "오산시": {"lat": 37.1499, "lon": 127.0775, "btm": 37.1298, "top": 37.1700, "lft": 127.0574, "rgt": 127.0976},
        "이천시": {"lat": 37.2720, "lon": 127.4350, "btm": 37.2319, "top": 37.3121, "lft": 127.3849, "rgt": 127.4851},
        "양주시": {"lat": 37.7854, "lon": 127.0457, "btm": 37.7453, "top": 37.8255, "lft": 127.0000, "rgt": 127.0914},
        "구리시": {"lat": 37.5943, "lon": 127.1296, "btm": 37.5742, "top": 37.6144, "lft": 127.1095, "rgt": 127.1497},
        "남양주시": {"lat": 37.6360, "lon": 127.2163, "btm": 37.5900, "top": 37.6820, "lft": 127.1600, "rgt": 127.2726},
        "의정부시": {"lat": 37.7381, "lon": 127.0337, "btm": 37.7180, "top": 37.7582, "lft": 127.0136, "rgt": 127.0538},
        "동두천시": {"lat": 37.9035, "lon": 127.0604, "btm": 37.8834, "top": 37.9236, "lft": 127.0403, "rgt": 127.0805},
        "안성시": {"lat": 37.0080, "lon": 127.2797, "btm": 36.9679, "top": 37.0481, "lft": 127.2296, "rgt": 127.3298},
        "포천시": {"lat": 37.8949, "lon": 127.2003, "btm": 37.8448, "top": 37.9450, "lft": 127.1402, "rgt": 127.2604},
        "양평군": {"lat": 37.4912, "lon": 127.4876, "btm": 37.4411, "top": 37.5413, "lft": 127.4275, "rgt": 127.5477},
        "여주시": {"lat": 37.2983, "lon": 127.6363, "btm": 37.2482, "top": 37.3484, "lft": 127.5762, "rgt": 127.6964},
        "가평군": {"lat": 37.8313, "lon": 127.5105, "btm": 37.7812, "top": 37.8814, "lft": 127.4504, "rgt": 127.5706},
        "연천군": {"lat": 38.0965, "lon": 127.0748, "btm": 38.0464, "top": 38.1466, "lft": 127.0147, "rgt": 127.1349},
    },
    "인천": {
        "중구": {"lat": 37.4736, "lon": 126.6214, "btm": 37.4535, "top": 37.4937, "lft": 126.6013, "rgt": 126.6415},
        "동구": {"lat": 37.4736, "lon": 126.6432, "btm": 37.4535, "top": 37.4937, "lft": 126.6231, "rgt": 126.6633},
        "미추홀구": {"lat": 37.4429, "lon": 126.6503, "btm": 37.4228, "top": 37.4630, "lft": 126.6302, "rgt": 126.6704},
        "연수구": {"lat": 37.4102, "lon": 126.6783, "btm": 37.3801, "top": 37.4403, "lft": 126.6482, "rgt": 126.7084},
        "남동구": {"lat": 37.4488, "lon": 126.7316, "btm": 37.4187, "top": 37.4789, "lft": 126.6915, "rgt": 126.7717},
        "부평구": {"lat": 37.5086, "lon": 126.7218, "btm": 37.4885, "top": 37.5287, "lft": 126.7017, "rgt": 126.7419},
        "계양구": {"lat": 37.5371, "lon": 126.7370, "btm": 37.5170, "top": 37.5572, "lft": 126.7100, "rgt": 126.7640},
        "서구": {"lat": 37.5450, "lon": 126.6760, "btm": 37.5100, "top": 37.5800, "lft": 126.6300, "rgt": 126.7220},
        "강화군": {"lat": 37.7473, "lon": 126.4878, "btm": 37.7072, "top": 37.7874, "lft": 126.4377, "rgt": 126.5379},
        "옹진군": {"lat": 37.4466, "lon": 126.6360, "btm": 37.4065, "top": 37.4867, "lft": 126.5859, "rgt": 126.6861},
    },
}

# ──────────────────────────────────────────────
# 유틸리티
# ──────────────────────────────────────────────

def parse_price(s: str) -> int:
    if not s:
        return 0
    s = s.replace(",", "").replace(" ", "").strip()
    total = 0
    if "억" in s:
        parts = s.split("억")
        total += int(parts[0]) * 10000
        s = parts[1] if len(parts) > 1 else ""
    if s and s.isdigit():
        total += int(s)
    return total


def fetch_articles(session, coords, rlet_types=DEFAULT_TYPES, trade_type=DEFAULT_TRADE):
    """매물 목록 조회 — 429 시 자동 대기 후 재시도"""

    url = "https://m.land.naver.com/cluster/ajax/articleList"
    params = {
        "rletTpCd": rlet_types,
        "tradTpCd": trade_type,
        "z": "14",
        "lat": str(coords["lat"]),
        "lon": str(coords["lon"]),
        "btm": str(coords["btm"]),
        "lft": str(coords["lft"]),
        "top": str(coords["top"]),
        "rgt": str(coords["rgt"]),
        "spcMin": "",
        "spcMax": "",
        "showR0": "",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = session.get(url, params=params, timeout=15)

            if res.status_code == 200:
                data = res.json()
                return data.get("body", [])

            elif res.status_code == 429:
                wait = RETRY_DELAY * attempt
                print(f"\n    ⏳ 429 차단 — {wait}초 대기 후 재시도 ({attempt}/{MAX_RETRIES})", end="", flush=True)
                time.sleep(wait)
                continue

            else:
                print(f"\n    ⚠ HTTP {res.status_code}", end="")
                return []

        except Exception as e:
            print(f"\n    ⚠ 오류: {e}", end="")
            if attempt < MAX_RETRIES:
                time.sleep(10)
            continue

    print(f"\n    ❌ {MAX_RETRIES}회 재시도 실패", end="")
    return []


def process_item(raw, sido, sigungu):
    spc1 = float(raw.get("spc1") or 0)
    spc2 = float(raw.get("spc2") or 0)
    area_py = round(spc1 / 3.3058, 2) if spc1 > 0 else 0
    price = parse_price(raw.get("hanPrc", ""))
    price_per_py = round(price / area_py) if area_py > 0 else 0

    return {
        "id": raw.get("atclNo", ""),
        "name": raw.get("atclNm", ""),
        "type": raw.get("rletTpCd", ""),
        "typeName": raw.get("rletTpNm", ""),
        "tradeTp": raw.get("tradTpNm", ""),
        "sido": sido,
        "sigungu": sigungu,
        "areaM2": round(spc1, 2),
        "areaM2Exclusive": round(spc2, 2),
        "areaPy": area_py,
        "price": price,
        "priceStr": raw.get("hanPrc", ""),
        "pricePerPy": price_per_py,
        "floor": raw.get("flrInfo", "-"),
        "desc": raw.get("atclFetrDesc", ""),
        "cfmDate": raw.get("cfmYmd", ""),
        "direction": raw.get("direction", ""),
        "url": f"https://new.land.naver.com/articles/{raw.get('atclNo', '')}",
    }


# ──────────────────────────────────────────────
# 메인 수집
# ──────────────────────────────────────────────

def collect(session, target_sido=None, target_gu=None,
            trade_type=DEFAULT_TRADE, rlet_types=DEFAULT_TYPES):
    all_items = []
    seen_ids = set()

    sidos = [target_sido] if target_sido else list(REGIONS.keys())

    for sido in sidos:
        if sido not in REGIONS:
            print(f"⚠ 알 수 없는 시도: {sido}")
            continue

        regions = REGIONS[sido]
        if target_gu:
            if target_gu in regions:
                targets = {target_gu: regions[target_gu]}
            else:
                print(f"⚠ {sido}에 {target_gu} 없음")
                continue
        else:
            targets = regions

        print(f"\n{'='*50}")
        print(f"📍 {sido} — {len(targets)}개 시군구 수집 시작")
        print(f"{'='*50}")

        for gu_name, coords in targets.items():
            print(f"\n  🔍 {sido} {gu_name} ...", end=" ", flush=True)

            raw_items = fetch_articles(session, coords, rlet_types, trade_type)

            count = 0
            for raw in raw_items:
                item = process_item(raw, sido, gu_name)
                if item["id"] and item["id"] not in seen_ids:
                    seen_ids.add(item["id"])
                    all_items.append(item)
                    count += 1

            print(f"✅ {count}건 (누적 {len(all_items)}건)")

            # 요청 간 대기
            time.sleep(REQUEST_DELAY)

    return all_items


def save_json(items, output_path="data/commercial.json"):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    items.sort(key=lambda x: x.get("pricePerPy", 0))

    output = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updatedISO": datetime.now().isoformat(),
        "count": len(items),
        "items": items,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n💾 저장 완료: {output_path} ({len(items)}건)")


def git_push():
    try:
        subprocess.run(["git", "add", "data/commercial.json"], check=True)
        msg = f"📊 매물 업데이트 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        result = subprocess.run(["git", "diff", "--staged", "--quiet"], capture_output=True)
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", msg], check=True)
            subprocess.run(["git", "push"], check=True)
            print("✅ git push 완료!")
        else:
            print("ℹ️  변경사항 없어서 push 생략")
    except subprocess.CalledProcessError as e:
        print(f"⚠ git 오류: {e}")
    except FileNotFoundError:
        print("⚠ git이 설치되어 있지 않거나 PATH에 없습니다")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="네이버 부동산 상업용 매물 수집기 (로컬)")
    parser.add_argument("--sido", type=str, default=None, help="시도 (서울/경기/인천)")
    parser.add_argument("--gu", type=str, default=None, help="시군구 (강남구, 수원시 등)")
    parser.add_argument("--trade", type=str, default="A1", help="A1(매매), B1(전세), B2(월세)")
    parser.add_argument("--types", type=str, default=DEFAULT_TYPES, help="매물유형")
    parser.add_argument("--output", type=str, default="data/commercial.json", help="출력 경로")
    parser.add_argument("--push", action="store_true", help="수집 후 자동 git push")
    args = parser.parse_args()

    print("🏢 네이버 부동산 상업용 매물 수집기")
    print(f"   시도: {args.sido or '전체'}")
    print(f"   시군구: {args.gu or '전체'}")
    print(f"   거래: {args.trade}")
    print(f"   요청간격: {REQUEST_DELAY}초")
    print()

    # 세션 생성 (쿠키 획득)
    session = create_session()
    if not session:
        print("\n❌ 세션 생성 실패. 종료합니다.")
        sys.exit(1)

    # 수집
    items = collect(session, args.sido, args.gu, args.trade, args.types)
    save_json(items, args.output)

    # 통계
    if items:
        print(f"\n📊 수집 통계:")
        type_counts = {}
        for it in items:
            tp = it.get("typeName", "기타")
            type_counts[tp] = type_counts.get(tp, 0) + 1
        for tp, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"   {tp}: {cnt}건")
        prices = [it["pricePerPy"] for it in items if it["pricePerPy"] > 0]
        if prices:
            print(f"   평당가 — 최저: {min(prices):,}만 / 평균: {sum(prices)//len(prices):,}만 / 최고: {max(prices):,}만")
    else:
        print("\n⚠ 수집된 매물이 없습니다.")

    # git push
    if args.push:
        print()
        git_push()


if __name__ == "__main__":
    main()
