# -*- coding: utf-8 -*-
"""관객통계 시트 읽기 전용 연동 (만족도분석기 → 관객통계)"""

import streamlit as st
import gspread
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


@st.cache_data(ttl=300, show_spinner=False)
def load_audience_all():
    """
    관객통계 시트에서 회차별 관객수·공연일·출연단체·장르를 읽어 dict로 반환.
    {회차(int): {공연일, 출연단체, 장르, 공연관객수, 체험참여수}}
    실패 시 빈 dict 반환.
    """
    if "gcp_service_account" not in st.secrets:
        return {}
    sheet_id = st.secrets.get("audience_sheet_id", "")
    if not sheet_id:
        return {}
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=SCOPES)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)

        result = {}

        # 회차기본 (회차, 공연일, 출연단체, 장르, 날씨)
        try:
            ws = sh.worksheet("회차기본")
            rows = ws.get_all_values()
            for row in rows[1:]:
                if not row or not row[0]:
                    continue
                try:
                    rnd = int(row[0])
                except ValueError:
                    continue
                result[rnd] = {
                    "공연일": row[1] if len(row) > 1 else "",
                    "출연단체": row[2] if len(row) > 2 else "",
                    "장르": row[3] if len(row) > 3 else "",
                    "공연관객수": 0,
                    "체험참여수": 0,
                }
        except Exception:
            pass

        # 관객수 (회차, 공연관객수, 체험참여수, ...)
        try:
            ws = sh.worksheet("관객수")
            rows = ws.get_all_values()
            for row in rows[1:]:
                if not row or not row[0]:
                    continue
                try:
                    rnd = int(row[0])
                except ValueError:
                    continue
                entry = result.setdefault(rnd, {
                    "공연일": "", "출연단체": "", "장르": "",
                    "공연관객수": 0, "체험참여수": 0,
                })
                try:
                    entry["공연관객수"] = int(row[1]) if len(row) > 1 and row[1] else 0
                    entry["체험참여수"] = int(row[2]) if len(row) > 2 and row[2] else 0
                except ValueError:
                    pass
        except Exception:
            pass

        return result
    except Exception:
        return {}


def clear_audience_cache():
    load_audience_all.clear()
