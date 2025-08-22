import os, io, zipfile, requests, datetime as dt
from typing import Optional, Tuple, Dict
import pandas as pd
from lxml import etree

BASE = "https://opendart.fss.or.kr/api"
DART_API_KEY = os.environ.get("DART_API_KEY")

def get_corp_code_from_df(corp_df: pd.DataFrame, corp_name: str) -> Optional[str]:
    hit = corp_df.loc[corp_df["corp_name"] == corp_name, "corp_code"]
    return str(hit.iloc[0]) if not hit.empty else None

def _is_business_report(report_nm: str) -> bool:
    return ("사업보고서" in report_nm) and ("정정" not in report_nm)

def list_latest_business_report(api_key: str, corp_code: str, days_back=900) -> Tuple[str, Dict]:
    end_de = dt.date.today().strftime("%Y%m%d")
    bgn_de = (dt.date.today() - dt.timedelta(days=days_back)).strftime("%Y%m%d")
    page_no, page_count, best = 1, 100, None

    while True:
        params = {"crtfc_key": api_key, "corp_code": corp_code,
                  "bgn_de": bgn_de, "end_de": end_de,
                  "page_no": page_no, "page_count": page_count}
        r = requests.get(f"{BASE}/list.json", params=params, timeout=60)
        r.raise_for_status()
        js = r.json()
        if js.get("status") != "000": break
        items = [it for it in js.get("list", []) if _is_business_report(it.get("report_nm",""))]
        for it in items:
            if (best is None) or (it["rcept_dt"] > best["rcept_dt"]):
                best = it
        if len(js.get("list", [])) < page_count: break
        page_no += 1

    if not best: raise ValueError("최근 사업보고서 없음")
    return best["rcept_no"], best

def download_document_zip(api_key: str, rcept_no: str) -> bytes:
    r = requests.get(f"{BASE}/document.xml",
                     params={"crtfc_key": api_key, "rcept_no": rcept_no}, timeout=90)
    r.raise_for_status()
    return r.content

def list_zip_entries(zip_bytes: bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        return [i.filename for i in zf.infolist()]
