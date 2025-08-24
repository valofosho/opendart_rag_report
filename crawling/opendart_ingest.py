import os
import io
import zipfile
import requests
import datetime as dt
from typing import Optional, Tuple, Dict
import pandas as pd
from lxml import etree
import xml.etree.ElementTree as ET
import json

BASE = "https://opendart.fss.or.kr/api"

try:
    with open("./src/config/api_keys.json", "r", encoding="utf-8") as f:
        DART_API_KEY = json.load(f)[0]["DART_API_KEY"]
except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
    DART_API_KEY = None

def load_corp_codes(corpcode_path: str = "./data/corp_codes/CORPCODE.xml") -> pd.DataFrame:
    """
    CORPCODE.xml 파일에서 기업코드 정보를 pandas DataFrame으로 불러옵니다.
    
    Args:
        corpcode_path: CORPCODE.xml 파일 경로
        
    Returns:
        DataFrame (컬럼: corp_code, corp_name, corp_eng_name, stock_code, modify_date)
    """
    try:
        tree = ET.parse(corpcode_path)
        root = tree.getroot()
        corp_items = []
        
        for corp_data in root.iter('list'):
            item = {
                "corp_code": (corp_data.findtext('corp_code') or '').strip(),
                "corp_name": (corp_data.findtext('corp_name') or '').strip(),
                "corp_eng_name": (corp_data.findtext('corp_eng_name') or '').strip(),
                "stock_code": (corp_data.findtext('stock_code') or '').strip(),
                "modify_date": (corp_data.findtext('modify_date') or '').strip(),
            }
            corp_items.append(item)
        
        return pd.DataFrame(corp_items)
    
    except FileNotFoundError:
        print(f"경고: CORPCODE.xml 파일을 찾을 수 없습니다: {corpcode_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"CORPCODE.xml 로딩 중 오류 발생: {e}")
        return pd.DataFrame()

def get_corp_code_from_df(corp_df: pd.DataFrame, corp_name: str) -> Optional[str]:
    hit = corp_df.loc[corp_df["corp_name"] == corp_name, "corp_code"]
    return str(hit.iloc[0]) if not hit.empty else None

def get_corp_code_by_name(corp_name: str, corpcode_path: str = "./data/corp_codes/CORPCODE.xml") -> Optional[str]:
    """
    회사명을 이용해 기업코드를 반환합니다. 필요시 CORPCODE.xml을 로드합니다.
    
    Args:
        corp_name: 찾고자 하는 회사명
        corpcode_path: CORPCODE.xml 파일 경로
        
    Returns:
        기업코드(문자열, 없으면 None)
    """
    corp_df = load_corp_codes(corpcode_path)
    if corp_df.empty:
        return None
    return get_corp_code_from_df(corp_df, corp_name)

def get_latest_business_report_by_corp_name(api_key: str, corp_name: str, corpcode_path: str = "./data/corp_codes/CORPCODE.xml", days_back=900) -> Tuple[str, Dict]:
    """
    회사명으로 최신 사업보고서를 조회합니다.
    
    Args:
        api_key: DART API 키
        corp_name: 회사명
        corpcode_path: CORPCODE.xml 파일 경로
        days_back: 조회 기간(일 단위)
        
    Returns:
        (접수번호, 보고서 데이터) 튜플
        
    Raises:
        ValueError: 회사가 없거나 사업보고서가 없을 때
    """
    corp_code = get_corp_code_by_name(corp_name, corpcode_path)
    if not corp_code:
        raise ValueError(f"회사 '{corp_name}'를 CORPCODE.xml에서 찾을 수 없습니다.")
    
    return list_latest_business_report(api_key, corp_code, days_back)

def _is_business_report(report_nm: str) -> bool:
    return ("사업보고서" in report_nm) and ("정정" not in report_nm)

def list_latest_business_report(api_key: str, corp_code: str, days_back=900) -> Tuple[str, Dict]:
    end_de = dt.date.today().strftime("%Y%m%d")
    bgn_de = (dt.date.today() - dt.timedelta(days=days_back)).strftime("%Y%m%d")
    page_no, page_count, best = 1, 100, None

    while True:
        params = {
            "crtfc_key": api_key,
            "corp_code": corp_code,
            "bgn_de": bgn_de,
            "end_de": end_de,
            "page_no": page_no,
            "page_count": page_count
        }
        r = requests.get(f"{BASE}/list.json", params=params, timeout=60)
        r.raise_for_status()
        js = r.json()
        if js.get("status") != "000":
            break
        items = [it for it in js.get("list", []) if _is_business_report(it.get("report_nm", ""))]
        for it in items:
            if (best is None) or (it.get("rcept_dt", "") > best.get("rcept_dt", "")):
                best = it
        if len(js.get("list", [])) < page_count:
            break
        page_no += 1

    if not best:
        raise ValueError("최근 사업보고서 없음")
    return best["rcept_no"], best

def download_document_zip(api_key: str, rcept_no: str) -> bytes:
    r = requests.get(
        f"{BASE}/document.xml",
        params={"crtfc_key": api_key, "rcept_no": rcept_no},
        timeout=90
    )
    r.raise_for_status()
    return r.content

def list_zip_entries(zip_bytes: bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return [i.filename for i in zf.infolist()]


if __name__ == "__main__":
    # 직접 실행 시 함수 테스트
    print("opendart_ingest.py 함수 테스트 중...")
    
    # 테스트 1: API 키 로드 확인
    if DART_API_KEY:
        print(f"API 키가 정상적으로 로드되었습니다.")
    else:
        print("API 키를 찾을 수 없습니다. src/config/api_keys.json 파일을 확인하세요.")
    
    # 테스트 2: CORPCODE.xml 로딩 테스트
    print("\nCORPCODE.xml 로딩 테스트:")
    corp_df = load_corp_codes()
    if not corp_df.empty:
        print(f"✓ CORPCODE.xml이 정상적으로 로드되었습니다. 기업 수: {len(corp_df)}")
        print(f"  샘플 기업: {corp_df['corp_name'].head(3).tolist()}")
    else:
        print("✗ CORPCODE.xml을 불러오지 못했습니다. ./data/corp_codes/CORPCODE.xml 파일을 확인하세요.")
    
    # 테스트 3: _is_business_report 함수 테스트
    test_reports = [
        "사업보고서",
        "사업보고서정정",
        "정정사업보고서",
        "기타보고서",
        "분기보고서"
    ]
    
    print("\n_is_business_report 함수 테스트:")
    for report in test_reports:
        result = _is_business_report(report)
        print(f"  '{report}' -> {result}")
    
    # 테스트 4: get_corp_code_from_df 함수 테스트
    print("\nget_corp_code_from_df 함수 테스트:")
    test_df = pd.DataFrame({
        "corp_name": ["삼성전자", "SK하이닉스", "LG전자"],
        "corp_code": ["00126380", "00164779", "00164779"]
    })
    
    test_corp_names = ["삼성전자", "SK하이닉스", "존재하지않는회사"]
    for corp_name in test_corp_names:
        result = get_corp_code_from_df(test_df, corp_name)
        print(f"  '{corp_name}' -> {result}")
    
    # 테스트 5: get_corp_code_by_name 함수 테스트 (CORPCODE.xml이 있을 때)
    if not corp_df.empty:
        print("\nget_corp_code_by_name 함수 테스트:")
        test_corp_names = ["삼성전자", "SK하이닉스", "LG전자"]
        for corp_name in test_corp_names:
            result = get_corp_code_by_name(corp_name)
            if result:
                print(f"  '{corp_name}' -> {result}")
            else:
                print(f"  '{corp_name}' -> 찾을 수 없음")
    
    # 테스트 6: list_zip_entries 함수 테스트 (임시 zip 파일 사용)
    print("\nlist_zip_entries 함수 테스트:")
    try:
        # 메모리 내에 간단한 테스트용 zip 파일 생성
        test_zip = io.BytesIO()
        with zipfile.ZipFile(test_zip, 'w') as zf:
            zf.writestr("test1.txt", "test content 1")
            zf.writestr("test2.txt", "test content 2")
            zf.writestr("folder/test3.txt", "test content 3")
        
        test_zip.seek(0)
        entries = list_zip_entries(test_zip.getvalue())
        print(f"  테스트 zip 파일 내 항목: {entries}")
    except Exception as e:
        print(f"  zip 함수 테스트 중 오류: {e}")
    
    print("\n✓ 모든 테스트가 완료되었습니다!")
    
    # 참고: 아래 함수들은 실제 API 호출이 필요하므로 여기서는 테스트하지 않습니다.
    # - list_latest_business_report() : 유효한 API 키와 corp_code 필요
    # - download_document_zip() : 유효한 API 키와 rcept_no 필요
    # - get_latest_business_report_by_corp_name() : 유효한 API 키와 CORPCODE.xml 필요
    print("\n참고: API가 필요한 함수들(list_latest_business_report, download_document_zip,")
    print("get_latest_business_report_by_corp_name)은 실제 API 키와 네트워크가 필요하므로")
    print("여기서는 테스트하지 않습니다.")
    
    # 예시 사용법
    if DART_API_KEY and not corp_df.empty:
        print("\n예시 사용법:")
        print("  # 회사명으로 최신 사업보고서 조회")
        print("  try:")
        print("      rcept_no, report_data = get_latest_business_report_by_corp_name(DART_API_KEY, '삼성전자')")
        print("      print(f'최신 보고서: {rcept_no}')")
        print("  except ValueError as e:")
        print("      print(f'오류: {e}')")
