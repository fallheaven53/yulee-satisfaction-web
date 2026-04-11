# -*- coding: utf-8 -*-
"""만족도분석기 — 구글 시트 동기화 (22문항 스키마)"""

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

    def _ws(self, title, rows=1000, cols=10):
        try:
            return self.sh.worksheet(title)
        except gspread.WorksheetNotFound:
            return self.sh.add_worksheet(title, rows=rows, cols=cols)

    def reset_all(self):
        """회차정보·응답분포·주관식 시트의 데이터 전부 삭제 (헤더만 남김)"""
        targets = [
            ("회차정보", ["회차", "공연일", "출연단체", "장르", "응답자수", "보충여부"]),
            ("응답분포", ["회차", "Q코드", "보기", "값"]),
            ("주관식",   ["회차", "Q코드", "순번", "내용"]),
        ]
        for title, header in targets:
            ws = self._ws(title)
            ws.clear()
            ws.update([header], value_input_option="RAW")

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

        # 응답분포 (long format)
        ws2 = self._ws("응답분포")
        data2 = [["회차", "Q코드", "보기", "값"]]
        for rnd in sorted(dm.responses.keys()):
            for q_code, dist in dm.responses[rnd].items():
                for opt, val in dist.items():
                    data2.append([rnd, q_code, opt, val])
        ws2.clear()
        ws2.update(data2, value_input_option="RAW")

        # 주관식 (long format)
        ws3 = self._ws("주관식")
        data3 = [["회차", "Q코드", "순번", "내용"]]
        for rnd in sorted(dm.texts.keys()):
            for q_code, texts in dm.texts[rnd].items():
                for i, t in enumerate(texts, start=1):
                    if t and str(t).strip():
                        data3.append([rnd, q_code, i, str(t)])
        ws3.clear()
        ws3.update(data3, value_input_option="RAW")

    def download_all(self, dm):
        # 회차정보
        try:
            ws1 = self.sh.worksheet("회차정보")
            rows = ws1.get_all_values()
            dm.rounds = {}
            for row in rows[1:]:
                if not row or not row[0]:
                    continue
                try:
                    rnd = int(row[0])
                except ValueError:
                    continue
                dm.rounds[rnd] = {
                    "공연일": row[1] if len(row) > 1 else "",
                    "출연단체": row[2] if len(row) > 2 else "",
                    "장르": row[3] if len(row) > 3 else "",
                    "응답자수": int(row[4]) if len(row) > 4 and row[4] else 0,
                    "보충": (row[5] == "Y") if len(row) > 5 else False,
                }
        except Exception:
            pass

        # 응답분포
        try:
            ws2 = self.sh.worksheet("응답분포")
            rows = ws2.get_all_values()
            dm.responses = {}
            for row in rows[1:]:
                if not row or len(row) < 4 or not row[0]:
                    continue
                try:
                    rnd = int(row[0])
                except ValueError:
                    continue
                q_code = row[1]
                opt = row[2]
                try:
                    val = float(row[3]) if row[3] else 0
                except ValueError:
                    val = 0
                dm.responses.setdefault(rnd, {}).setdefault(q_code, {})[opt] = val
        except Exception:
            pass

        # 주관식
        try:
            ws3 = self.sh.worksheet("주관식")
            rows = ws3.get_all_values()
            dm.texts = {}
            for row in rows[1:]:
                if not row or len(row) < 4 or not row[0]:
                    continue
                try:
                    rnd = int(row[0])
                except ValueError:
                    continue
                q_code = row[1]
                content = row[3]
                dm.texts.setdefault(rnd, {}).setdefault(q_code, []).append(content)
        except Exception:
            pass
