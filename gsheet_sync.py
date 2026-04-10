# -*- coding: utf-8 -*-
"""만족도분석기 — 구글 시트 동기화"""

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


class SatisfactionSheetSync:
    """만족도 데이터 ↔ 구글 스프레드시트 양방향 동기화"""

    def __init__(self, credentials_path=None, credentials_dict=None,
                 spreadsheet_id=""):
        if credentials_dict:
            creds = Credentials.from_service_account_info(
                credentials_dict, scopes=SCOPES)
        else:
            creds = Credentials.from_service_account_file(
                credentials_path, scopes=SCOPES)
        self.gc = gspread.authorize(creds)
        self.sh = self.gc.open_by_key(spreadsheet_id)

    def _ws(self, title, rows=500, cols=15):
        try:
            return self.sh.worksheet(title)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(title, rows=rows, cols=cols)

    def upload_all(self, dm):
        # 회차정보
        ws1 = self._ws("회차정보")
        data1 = [["회차", "공연일", "출연단체", "장르", "응답자수", "보충여부"]]
        for rnd in sorted(dm.rounds.keys()):
            r = dm.rounds[rnd]
            data1.append([rnd, r.get("공연일", ""), r.get("출연단체", ""),
                          r.get("장르", ""), r.get("응답자수", 0),
                          "Y" if r.get("보충", False) else ""])
        ws1.clear()
        ws1.update(data1, value_input_option="RAW")

        # 만족도 (long format)
        ws2 = self._ws("만족도")
        data2 = [["회차", "항목코드", "항목명", "매우그렇다", "그렇다",
                  "보통", "그렇지않다", "매우그렇지않다"]]
        for rnd in sorted(dm.satisfaction.keys()):
            for q_code, vals in dm.satisfaction[rnd].items():
                data2.append([
                    rnd, q_code,
                    dm.Q_LABELS.get(q_code, q_code),
                    vals.get("매우그렇다", 0),
                    vals.get("그렇다", 0),
                    vals.get("보통", 0),
                    vals.get("그렇지않다", 0),
                    vals.get("매우그렇지않다", 0),
                ])
        ws2.clear()
        ws2.update(data2, value_input_option="RAW")

        # 인구통계 (long format)
        ws3 = self._ws("인구통계")
        data3 = [["회차", "카테고리", "항목", "비율"]]
        for rnd in sorted(dm.demographics.keys()):
            for cat, items in dm.demographics[rnd].items():
                for item_name, ratio in items.items():
                    data3.append([rnd, cat, item_name, ratio])
        ws3.clear()
        ws3.update(data3, value_input_option="RAW")

    def download_all(self, dm):
        # 회차정보
        try:
            ws1 = self.sh.worksheet("회차정보")
            rows = ws1.get_all_values()
            dm.rounds = {}
            for row in rows[1:]:
                if not row[0]:
                    continue
                rnd = int(row[0])
                dm.rounds[rnd] = {
                    "공연일": row[1] if len(row) > 1 else "",
                    "출연단체": row[2] if len(row) > 2 else "",
                    "장르": row[3] if len(row) > 3 else "",
                    "응답자수": int(row[4]) if len(row) > 4 and row[4] else 0,
                    "보충": (row[5] == "Y") if len(row) > 5 else False,
                }
        except Exception:
            pass

        # 만족도
        try:
            ws2 = self.sh.worksheet("만족도")
            rows = ws2.get_all_values()
            dm.satisfaction = {}
            for row in rows[1:]:
                if not row[0] or len(row) < 8:
                    continue
                rnd = int(row[0])
                q_code = row[1]
                if rnd not in dm.satisfaction:
                    dm.satisfaction[rnd] = {}
                dm.satisfaction[rnd][q_code] = {
                    "매우그렇다": float(row[3]) if row[3] else 0,
                    "그렇다": float(row[4]) if row[4] else 0,
                    "보통": float(row[5]) if row[5] else 0,
                    "그렇지않다": float(row[6]) if row[6] else 0,
                    "매우그렇지않다": float(row[7]) if row[7] else 0,
                }
        except Exception:
            pass

        # 인구통계
        try:
            ws3 = self.sh.worksheet("인구통계")
            rows = ws3.get_all_values()
            dm.demographics = {}
            for row in rows[1:]:
                if not row[0] or len(row) < 4:
                    continue
                rnd = int(row[0])
                cat = row[1]
                item = row[2]
                ratio = float(row[3]) if row[3] else 0
                if rnd not in dm.demographics:
                    dm.demographics[rnd] = {}
                if cat not in dm.demographics[rnd]:
                    dm.demographics[rnd][cat] = {}
                dm.demographics[rnd][cat][item] = ratio
        except Exception:
            pass
