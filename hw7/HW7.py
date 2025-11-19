import requests
import pandas as pd
from datetime import datetime, timedelta
import urllib3
import json

# ----------------------------------------------------------
# 1. 설정 영역 (사진 속 인증키 적용)
# ----------------------------------------------------------
API_KEY = "526b01c6365799e06da88d2d3d8ca3c33514cf8c66c20aeb3474be9f44ca116d"

# 기본 URL
BASE_URL = "http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

# 지점 번호 (서울)
STATION_ID = "108"

# ----------------------------------------------------------
# 2. 날짜 계산 (과제 제출일 11/19 기준 2일 전 -> 11/17)
# ----------------------------------------------------------
submit_date = datetime(2025, 11, 19)
target_date_dynamic = submit_date - timedelta(days=2)
dynamic_date_str = target_date_dynamic.strftime("%Y%m%d")

targets = [
    {"date": "20241204", "start_h": 15, "end_h": 18},
    {"date": "20250604", "start_h": 12, "end_h": 16},
    {"date": dynamic_date_str, "start_h": 0, "end_h": 3}
]


# ----------------------------------------------------------
# 3. 핵심 함수 (GPT 방식 적용: URL 직접 조립)
# ----------------------------------------------------------
def get_weather_data(date_str, hour):
    hour_str = str(hour).zfill(2)

    # [핵심 수정] params 딕셔너리에 키를 넣지 않고, URL 뒤에 직접 붙입니다.
    # 이렇게 하면 requests가 인증키를 멋대로 인코딩하는 것을 막을 수 있습니다.
    # numofRows도 100으로 넉넉하게 잡았습니다.
    query_url = f"{BASE_URL}?serviceKey={API_KEY}&pageNo=1&numOfRows=100&dataType=JSON&dataCd=ASOS&dateCd=HR&startDt={date_str}&startHh={hour_str}&endDt={date_str}&endHh={hour_str}&stnIds={STATION_ID}"

    try:
        response = requests.get(query_url, verify=False)

        if response.status_code != 200:
            print(f"   [HTTP 에러 {response.status_code}]")
            return []

        try:
            data = response.json()
            # API 정상 응답 확인
            if data["response"]["header"]["resultCode"] != "00":
                print(f"   [API 에러] {data['response']['header']['resultMsg']}")
                return []

            return data["response"]["body"]["items"]["item"]

        except json.JSONDecodeError:
            print(f"   [파싱 에러] 응답이 JSON이 아님: {response.text[:100]}")
            return []

    except Exception as e:
        print(f"   [시스템 에러] {e}")
        return []


# ----------------------------------------------------------
# 4. 실행 로직
# ----------------------------------------------------------
if __name__ == "__main__":
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    all_data = []
    print(f"데이터 수집 시작... (인증키 URL 직접 주입 방식)")

    for target in targets:
        date = target["date"]
        for h in range(target["start_h"], target["end_h"] + 1):
            print(f">> 수집 중: {date} {h}시")
            items = get_weather_data(date, h)
            if items:
                all_data.extend(items)

    if all_data:
        # 결과 저장
        df = pd.DataFrame(all_data)

        # 필요한 컬럼만 깔끔하게 저장 (옵션)
        # cols = ['tm', 'stnId', 'ta', 'rn', 'ws', 'wd', 'hm']
        # df = df[cols] if set(cols).issubset(df.columns) else df

        output_filename = "HW7_result.csv"
        df.to_csv(output_filename, index=False, encoding="utf-8-sig")

        print("-" * 60)
        print(f"[성공] 총 {len(df)}건 수집 완료. '{output_filename}' 저장됨.")
        print("-" * 60)
    else:
        print("[실패] 데이터가 수집되지 않았습니다.")