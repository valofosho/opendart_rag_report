import io
import zipfile
import re
from lxml import etree, html

def _decode_kr(data: bytes) -> str:
    """
    다양한 한글 인코딩(cp949, euc-kr, utf-8)으로 디코딩 시도 후 실패 시 무시 옵션으로 utf-8 디코딩
    """
    for enc in ("cp949", "euc-kr", "utf-8"):  # 대표적인 한글 인코딩 우선순위로 시도
        try:
            return data.decode(enc)  # 성공 시 바로 반환
        except UnicodeDecodeError:
            continue  # 실패 시 다음 인코딩 시도
    return data.decode("utf-8", "ignore")  # 모두 실패하면 손상 문자 무시하고 utf-8로 디코딩

def choose_main_entry(names, rcept_no):
    """
    zip 내 주요 본문 파일(HTML/XML) 추정: 접수번호로 시작, '사업보고서'/'본문' 포함, 확장자 우선순위 등으로 선택
    """
    pri_ext = ["html", "htm", "xml"]  # 우선적으로 선택할 확장자 순서
    def rank(n):
        m = re.search(r"\.([a-z0-9]+)$", n.lower())  # 파일 확장자 추출
        ext = m.group(1) if m else ""
        return pri_ext.index(ext) if ext in pri_ext else 999  # 우선순위 미포함시 큰 값 부여
    # 1순위: 접수번호로 시작하고 html/xml 확장자
    cands = [n for n in names if n.lower().startswith(rcept_no.lower()) and re.search(r"\.(html?|xml)$", n, re.I)]
    if not cands:
        # 2순위: '사업보고서' 또는 '본문' 포함하고 html/xml 확장자
        cands = [n for n in names if re.search(r"(사업보고서|본문)", n) and re.search(r"\.(html?|xml)$", n, re.I)]
    if not cands:
        # 3순위: html/xml 확장자
        cands = [n for n in names if re.search(r"\.(html?|xml)$", n, re.I)]
    if not cands:
        return None  # 후보 없으면 None 반환
    return sorted(cands, key=rank)[0]  # 우선순위 높은 확장자 우선 반환

def extract_text_from_zip_entry(zip_bytes: bytes, entry_name: str) -> str:
    """
    zip 파일 내 특정 entry에서 텍스트 추출: XML 우선, 실패 시 HTML 파싱
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:  # 메모리 zip 파일 열기
        data = zf.read(entry_name)  # 지정 entry의 바이너리 데이터 읽기
    txt = _decode_kr(data)  # 한글 인코딩 자동 감지 및 디코딩
    try:
        root = etree.fromstring(txt.encode("utf-8"), etree.XMLParser(recover=True))  # XML 파싱 시도
        return "".join(root.itertext())  # XML 내 모든 텍스트 추출
    except etree.XMLSyntaxError:
        root = html.fromstring(txt)  # XML 실패 시 HTML 파싱
        return root.text_content()  # HTML 내 모든 텍스트 추출
