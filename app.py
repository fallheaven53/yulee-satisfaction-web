# -*- coding: utf-8 -*-
"""
율이공방 — 만족도분석기 웹앱 (네이버폼 22문항)
2026 토요상설공연 만족도조사 등록·분석
"""

import re
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from io import BytesIO

st.set_page_config(
    page_title="율이공방 — 만족도분석기",
    page_icon="📋",
    layout="wide",
)

from data_manager import (
    SatisfactionManager, GENRES, QUESTIONS, Q_BY_CODE,
    SCALE_STD, SCALE5_CODES, DIST_CODES, TEXT_CODES,
    POSITIVE_LEVELS, MIN_RESPONDENTS, options_of, normalize_pct,
)
from cross_sync import load_audience_all, clear_audience_cache

# ══════════════════════════════════════════════════════════════
#  데이터 연결
# ══════════════════════════════════════════════════════════════

@st.cache_resource
def get_dm():
    gsheet = None
    try:
        from gsheet_sync import SatisfactionSheetSync
        if "gcp_service_account" in st.secrets:
            gsheet = SatisfactionSheetSync(
                credentials_dict=dict(st.secrets["gcp_service_account"]),
                spreadsheet_id=st.secrets["spreadsheet_id"],
            )
    except Exception as e:
        st.sidebar.warning(f"구글 시트 연결 실패: {e}")
    return SatisfactionManager(gsheet_sync=gsheet)


def reload_dm():
    get_dm.clear()
    st.rerun()


def load_target_dates():
    """정산관리 구글 시트에서 출연단체·공연일 매핑"""
    if "target_dates" in st.session_state:
        return st.session_state["target_dates"]
    result = {}
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        if "gcp_service_account" not in st.secrets:
            return result
        if "settlement_spreadsheet_id" not in st.secrets or not st.secrets["settlement_spreadsheet_id"]:
            return result
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(st.secrets["settlement_spreadsheet_id"])

        cur_year = str(datetime.now().year)
        ws1 = sh.worksheet("단체정보")
        rows = ws1.get_all_values()
        id_to_name = {}
        for row in rows[1:]:
            if row[0] and row[1]:
                id_to_name[row[0].strip()] = row[1].strip()

        ws2 = sh.worksheet("출연이력")
        rows2 = ws2.get_all_values()
        for row in rows2[1:]:
            if len(row) > 4 and row[2].strip() == cur_year:
                tid = row[1].strip()
                name = id_to_name.get(tid, "")
                rnd_str = row[3].strip()
                date_val = row[4].strip()
                if name and date_val and rnd_str.isdigit():
                    result[int(rnd_str)] = (name, date_val)
    except Exception:
        pass
    st.session_state["target_dates"] = result
    return result


# ══════════════════════════════════════════════════════════════
#  네이버폼 CSV 파싱 (Q1~Q22)
# ══════════════════════════════════════════════════════════════

def _read_csv(file_bytes):
    for enc in ("utf-8-sig", "utf-8", "cp949", "euc-kr"):
        try:
            return pd.read_csv(BytesIO(file_bytes), encoding=enc)
        except Exception:
            continue
    return None


def _extract_round(val):
    """'1회차', '제 1회차', '1' 등에서 정수 추출"""
    if pd.isna(val):
        return None
    s = str(val)
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else None


def parse_naver_csv(file_bytes):
    """
    네이버폼 CSV → {회차: {"resp": {Q코드:{보기:카운트}}, "texts": {Q코드:[...]}, "n": 응답자수}}
    '회차' 키워드 포함 컬럼을 Q1 앵커로 삼아, 그 이후 컬럼을 Q2~Q22로 순서대로 매핑.
    회차 앞쪽의 메타데이터(응답일시·참여자 번호 등)는 자동으로 무시됨.
    """
    df = _read_csv(file_bytes)
    if df is None or df.empty:
        return {}

    cols = list(df.columns)
    # 회차 컬럼(Q1 앵커) 탐색
    round_idx = None
    for i, c in enumerate(cols):
        if "회차" in str(c):
            round_idx = i
            break
    if round_idx is None:
        return {}  # 회차 컬럼 못 찾으면 포기

    # 회차 컬럼부터 최대 22개를 Q1~Q22로 매핑
    q_cols = cols[round_idx:round_idx + 22]
    round_col = q_cols[0]

    result = {}
    for _, row in df.iterrows():
        rnd = _extract_round(row[round_col])
        if rnd is None:
            continue
        bucket = result.setdefault(rnd, {"resp": {}, "texts": {}, "n": 0})
        bucket["n"] += 1

        for idx, col in enumerate(q_cols):
            q_code = f"Q{idx + 1}"
            q = Q_BY_CODE.get(q_code)
            if not q or q["type"] == "round":
                continue
            val = row[col]
            if pd.isna(val):
                continue
            sval = str(val).strip()
            if not sval:
                continue
            if q["type"] in ("text", "free"):
                bucket["texts"].setdefault(q_code, []).append(sval)
            else:
                # 단일선택 / scale5
                opts = options_of(q_code)
                # 정확 매칭 우선, 없으면 부분 매칭
                matched = None
                for opt in opts:
                    if sval == opt:
                        matched = opt
                        break
                if matched is None:
                    for opt in opts:
                        if opt in sval or sval in opt:
                            matched = opt
                            break
                if matched is None:
                    matched = sval  # 매칭 실패는 원문 그대로
                d = bucket["resp"].setdefault(q_code, {opt: 0 for opt in opts})
                d[matched] = d.get(matched, 0) + 1
    return result


# ══════════════════════════════════════════════════════════════
#  사이드바
# ══════════════════════════════════════════════════════════════

dm = get_dm()
records = dm.get_round_records()

st.sidebar.title("📋 만족도분석기")
st.sidebar.caption("2026 토요상설공연")
st.sidebar.divider()

if dm.gsheet:
    st.sidebar.success("✅ 구글 시트 연결됨")
    if st.sidebar.button("🔄 구글 시트 새로고침"):
        clear_audience_cache()
        reload_dm()

    with st.sidebar.expander("⚠ 시트 초기화", expanded=False):
        st.caption("회차정보·응답분포·주관식 시트의 모든 데이터를 지웁니다. 되돌릴 수 없습니다.")
        confirm = st.text_input("확인을 위해 '초기화'를 입력하세요", key="reset_confirm")
        if st.button("🗑 전부 삭제", type="secondary", use_container_width=True):
            if confirm == "초기화":
                try:
                    dm.gsheet.reset_all()
                    dm.rounds = {}
                    dm.responses = {}
                    dm.texts = {}
                    st.success("시트 초기화 완료")
                    reload_dm()
                except Exception as e:
                    st.error(f"초기화 실패: {e}")
            else:
                st.warning("확인 문구가 일치하지 않습니다")
else:
    st.sidebar.info("⚠ 로컬 모드 (시트 비연결)")

# 관객통계 연동 상태
_aud_data = load_audience_all()
if _aud_data:
    st.sidebar.success(f"🔗 관객통계 연동: {len(_aud_data)}회차")
elif "audience_sheet_id" in st.secrets and st.secrets.get("audience_sheet_id"):
    st.sidebar.warning("🔗 관객통계 연동 실패")
else:
    st.sidebar.caption("🔗 관객통계 미연동 (secrets)")

st.sidebar.metric("등록 회차", f"{len(records)}회")
st.sidebar.metric("총 응답자 수", f"{sum(r['응답자수'] for r in records):,}명")

insuf = dm.insufficient_rounds()
if insuf:
    st.sidebar.warning(f"⚠ 기준치({MIN_RESPONDENTS}명) 미달: {len(insuf)}회차")

st.title("📋 만족도 분석기")
st.caption(f"네이버폼 22문항 기준 · 오늘 {datetime.now().strftime('%Y-%m-%d')}")

tab1, tab2, tab3, tab4 = st.tabs([
    "① 만족도 입력·관리",
    "② 인구통계 입력",
    "③ 분석 대시보드",
    "④ 보고서 내보내기",
])

# ══════════════════════════════════════════════════════════════
#  탭 1 — 만족도 입력·관리 (Q1~Q17)
# ══════════════════════════════════════════════════════════════

with tab1:
    # ── 관객통계 연동 조회 (읽기 전용) ──
    with st.expander("🔗 관객통계 연동 조회 (회차별)", expanded=False):
        aud_data = load_audience_all()
        cols_ref = st.columns([1, 3])
        with cols_ref[0]:
            ref_rnd = st.number_input("조회할 회차", min_value=1, max_value=99, value=1, step=1,
                                       key="ref_aud_rnd")
            if st.button("🔄 관객통계 새로고침", key="btn_aud_refresh"):
                clear_audience_cache()
                st.rerun()
        with cols_ref[1]:
            info = aud_data.get(int(ref_rnd))
            if info is None:
                if aud_data:
                    st.warning(f"{int(ref_rnd)}회차 — 관객통계 미등록")
                else:
                    st.caption("관객통계 시트 비연동 또는 불러오기 실패")
            else:
                mc = st.columns(5)
                mc[0].metric("공연일", info.get("공연일", "-") or "-")
                mc[1].metric("출연단체", info.get("출연단체", "-") or "-")
                mc[2].metric("장르", info.get("장르", "-") or "-")
                mc[3].metric("공연관객수", f"{info.get('공연관객수', 0):,}")
                mc[4].metric("체험참여수", f"{info.get('체험참여수', 0):,}")
                st.caption("※ 읽기 전용 — 값은 관객통계 웹앱에서 수정하세요.")

    st.subheader("📥 네이버폼 CSV 불러오기")
    st.caption("컬럼이 Q1~Q22 순서여야 합니다. 중복 회차는 누적됩니다.")
    up = st.file_uploader("CSV 파일", type=["csv"], key="csv_upload")
    if up is not None:
        parsed = parse_naver_csv(up.getvalue())
        if not parsed:
            st.error("회차(Q1)를 인식하지 못했습니다. CSV 형식을 확인해주세요.")
        else:
            st.success(f"파싱 완료: {len(parsed)}개 회차, 총 {sum(b['n'] for b in parsed.values())}건 응답")
            preview = pd.DataFrame([{
                "회차": rnd, "응답자수": b["n"],
                "분포 문항 수": len(b["resp"]),
                "주관식 응답 수": sum(len(v) for v in b["texts"].values()),
            } for rnd, b in sorted(parsed.items())])
            st.dataframe(preview, use_container_width=True, hide_index=True)

            target_dates = load_target_dates()
            if st.button("💾 모든 회차 저장 (분포 + 주관식)", type="primary"):
                for rnd, b in parsed.items():
                    info = dm.rounds.get(rnd, {}).copy()
                    if rnd in target_dates:
                        name, date_val = target_dates[rnd]
                        info.setdefault("출연단체", name)
                        info.setdefault("공연일", date_val)
                    info["응답자수"] = b["n"]
                    info.setdefault("장르", info.get("장르", ""))
                    dm.rounds[rnd] = info
                    dm.responses[rnd] = b["resp"]
                    dm.texts[rnd] = b["texts"]
                dm._sync()
                st.success("저장 완료")
                st.rerun()

    st.divider()

    # ── 수동 입력 폼 ──
    st.subheader("✍ 수동 입력 / 수정")
    edit_mode = "edit_target" in st.session_state and st.session_state["edit_target"] in dm.rounds
    target_rnd = st.session_state.get("edit_target")
    if edit_mode:
        st.info(f"📝 {target_rnd}회차 수정 중")

    target_dates = load_target_dates()
    base = dm.rounds.get(target_rnd, {}) if edit_mode else {}
    base_resp = dm.responses.get(target_rnd, {}) if edit_mode else {}
    base_texts = dm.texts.get(target_rnd, {}) if edit_mode else {}

    with st.form("manual_form", clear_on_submit=False):
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            rnd_in = st.number_input("회차", min_value=1, max_value=99,
                                      value=int(target_rnd) if edit_mode else 1, step=1)
        # 회차 변경 시 자동 매핑
        auto_name, auto_date = "", ""
        if rnd_in in target_dates:
            auto_name, auto_date = target_dates[rnd_in]
        with c2:
            date_in = st.text_input("공연일", value=base.get("공연일") or auto_date)
        with c3:
            name_in = st.text_input("출연단체", value=base.get("출연단체") or auto_name)
        with c4:
            genre_in = st.selectbox("장르", GENRES,
                index=GENRES.index(base["장르"]) if base.get("장르") in GENRES else 0)

        n_in = st.number_input("응답자 수", min_value=0, value=int(base.get("응답자수", 0)), step=1)

        st.markdown("##### 만족도 분포 입력 (Q2~Q17)")
        st.caption("5점 척도(Q4~Q9, Q16~Q17)는 비율(%) 입력. 단일선택 문항은 응답 수 입력.")
        new_resp = {}
        for q in QUESTIONS:
            if q["type"] == "round" or q["code"] in TEXT_CODES:
                continue
            with st.expander(f"{q['code']}. {q['label']}", expanded=False):
                opts = options_of(q["code"])
                cols_in = st.columns(min(len(opts), 5))
                row_vals = {}
                cur = base_resp.get(q["code"], {})
                for i, opt in enumerate(opts):
                    with cols_in[i % len(cols_in)]:
                        v = st.number_input(opt, min_value=0.0, step=1.0,
                                            value=float(cur.get(opt, 0)),
                                            key=f"{q['code']}_{opt}_{target_rnd or 'new'}")
                        row_vals[opt] = v
                if any(row_vals.values()):
                    new_resp[q["code"]] = row_vals

        st.markdown("##### 주관식 응답 (Q20~Q22, 줄바꿈으로 구분)")
        text_inputs = {}
        for q_code in TEXT_CODES:
            q = Q_BY_CODE[q_code]
            existing = "\n".join(base_texts.get(q_code, []))
            txt = st.text_area(f"{q_code}. {q['label']}", value=existing,
                               key=f"text_{q_code}_{target_rnd or 'new'}", height=80)
            if txt.strip():
                text_inputs[q_code] = [line for line in txt.split("\n") if line.strip()]

        c_btn1, c_btn2 = st.columns(2)
        with c_btn1:
            submitted = st.form_submit_button(
                "💾 저장" if not edit_mode else "💾 수정 저장", type="primary", use_container_width=True)
        with c_btn2:
            cancel = st.form_submit_button("❌ 취소", use_container_width=True)

    if submitted:
        info = {
            "공연일": date_in, "출연단체": name_in, "장르": genre_in,
            "응답자수": int(n_in), "보충": dm.rounds.get(int(rnd_in), {}).get("보충", False),
        }
        dm.save_round_info(int(rnd_in), info)
        if new_resp:
            dm.save_responses(int(rnd_in), new_resp)
        if text_inputs:
            dm.save_texts(int(rnd_in), text_inputs)
        st.session_state.pop("edit_target", None)
        st.success(f"{int(rnd_in)}회차 저장 완료")
        st.rerun()
    if cancel and edit_mode:
        st.session_state.pop("edit_target", None)
        st.rerun()

    st.divider()

    # ── 회차 목록 ──
    st.subheader("📊 등록 회차 목록")
    if records:
        df_rec = pd.DataFrame(records)
        sel = st.dataframe(
            df_rec, use_container_width=True, hide_index=True,
            on_select="rerun", selection_mode="single-row",
        )
        if sel.selection.rows:
            sel_idx = sel.selection.rows[0]
            if sel_idx >= len(records):
                st.rerun()
            sel_rnd = records[sel_idx]["회차"]
            cb1, cb2 = st.columns(2)
            with cb1:
                if st.button(f"✏ {sel_rnd}회차 수정", use_container_width=True):
                    st.session_state["edit_target"] = sel_rnd
                    st.rerun()
            with cb2:
                if st.button(f"🗑 {sel_rnd}회차 삭제", use_container_width=True):
                    dm.delete_round(sel_rnd)
                    st.rerun()
    else:
        st.info("등록된 회차가 없습니다. CSV 업로드 또는 수동 입력으로 시작하세요.")

    if insuf:
        st.divider()
        st.subheader("⚠ 기준치 미달 회차 보충")
        st.caption(f"응답자 수가 {MIN_RESPONDENTS}명 미만인 회차를 전체 평균으로 보충합니다.")
        cols_fill = st.columns(min(len(insuf), 6))
        for i, rnd in enumerate(insuf):
            with cols_fill[i % len(cols_fill)]:
                if st.button(f"{rnd}회차 보충", key=f"fill_{rnd}"):
                    dm.fill_insufficient(rnd)
                    st.success(f"{rnd}회차 보충 완료")
                    st.rerun()

# ══════════════════════════════════════════════════════════════
#  탭 2 — 인구통계 입력 (Q14, Q15, Q18, Q19, Q20)
# ══════════════════════════════════════════════════════════════

with tab2:
    st.subheader("👥 인구통계 입력 / 수정")
    st.caption("탭1과 동일한 데이터 저장소를 사용합니다. 여기서는 인구통계 문항만 빠르게 입력합니다.")
    DEMO_QCODES = ["Q14", "Q15", "Q18", "Q19", "Q20"]

    if not records:
        st.info("먼저 탭1에서 회차를 등록하세요.")
    else:
        rnd_options = [r["회차"] for r in records]
        sel_rnd = st.selectbox("회차 선택", rnd_options, key="demo_rnd")
        cur_resp = dm.responses.get(sel_rnd, {})
        cur_texts = dm.texts.get(sel_rnd, {})

        with st.form("demo_form"):
            new_resp_demo = {}
            for q_code in ["Q14", "Q15", "Q18", "Q19"]:
                q = Q_BY_CODE[q_code]
                st.markdown(f"**{q_code}. {q['label']}**")
                opts = options_of(q_code)
                cols_in = st.columns(min(len(opts), 5))
                row_vals = {}
                cur = cur_resp.get(q_code, {})
                for i, opt in enumerate(opts):
                    with cols_in[i % len(cols_in)]:
                        v = st.number_input(opt, min_value=0.0, step=1.0,
                                            value=float(cur.get(opt, 0)),
                                            key=f"demo_{q_code}_{opt}")
                        row_vals[opt] = v
                if any(row_vals.values()):
                    new_resp_demo[q_code] = row_vals

            st.markdown("**Q20. 거주 주소 (줄바꿈으로 구분)**")
            existing = "\n".join(cur_texts.get("Q20", []))
            q20_text = st.text_area("거주 주소 목록", value=existing, height=120, key="demo_q20")

            if st.form_submit_button("💾 인구통계 저장", type="primary"):
                merged_resp = dict(cur_resp)
                merged_resp.update(new_resp_demo)
                dm.save_responses(sel_rnd, merged_resp)
                merged_texts = dict(cur_texts)
                if q20_text.strip():
                    merged_texts["Q20"] = [line for line in q20_text.split("\n") if line.strip()]
                else:
                    merged_texts.pop("Q20", None)
                dm.save_texts(sel_rnd, merged_texts)
                st.success(f"{sel_rnd}회차 인구통계 저장")
                st.rerun()

# ══════════════════════════════════════════════════════════════
#  탭 3 — 분석 대시보드
# ══════════════════════════════════════════════════════════════

with tab3:
    summary = dm.calc_summary()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("등록 회차", f"{summary['total_rounds']}회")
    c2.metric("총 응답자 수", f"{summary['total_resp']:,}명")
    c3.metric("평균 긍정률(Q4~Q9)", f"{summary['avg_pos']}%")
    c4.metric("최고 회차", summary["max_round"])

    if not records:
        st.info("등록된 데이터가 없습니다.")
    else:
        st.divider()
        st.subheader("📈 만족도 6개 문항(Q4~Q9) 긍정응답률 추이")
        scale6_codes = ["Q4", "Q5", "Q6", "Q7", "Q8", "Q9"]
        rows = []
        for q_code in scale6_codes:
            for rnd, pos in dm.positive_trend(q_code):
                rows.append({"회차": rnd, "문항": f"{q_code}.{Q_BY_CODE[q_code]['label']}", "긍정률(%)": pos})
        if rows:
            df_t = pd.DataFrame(rows)
            fig = px.line(df_t, x="회차", y="긍정률(%)", color="문항", markers=True,
                          template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("Q4~Q9 데이터가 없습니다.")

        st.divider()
        st.subheader("🔁 재참여 의향(Q16) · 추천 의향(Q17) 추이")
        rows2 = []
        for q_code in ["Q16", "Q17"]:
            for rnd, pos in dm.positive_trend(q_code):
                rows2.append({"회차": rnd, "문항": f"{q_code}.{Q_BY_CODE[q_code]['label']}", "긍정률(%)": pos})
        if rows2:
            df_t2 = pd.DataFrame(rows2)
            fig2 = px.line(df_t2, x="회차", y="긍정률(%)", color="문항", markers=True,
                           template="plotly_dark")
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.caption("Q16~Q17 데이터가 없습니다.")

        st.divider()
        col_l, col_r = st.columns(2)

        # Q3 정보습득경로
        with col_l:
            st.subheader("Q3. 정보 습득 경로")
            d = dm.aggregate_dist("Q3")
            if d:
                fig = px.pie(values=list(d.values()), names=list(d.keys()),
                             template="plotly_dark", hole=0.4)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.caption("데이터 없음")

        # Q2 방문 횟수
        with col_r:
            st.subheader("Q2. 방문 횟수 (신규 vs 재방문)")
            d = dm.aggregate_dist("Q2")
            if d:
                df_b = pd.DataFrame({"보기": list(d.keys()), "비율(%)": list(d.values())})
                fig = px.bar(df_b, x="보기", y="비율(%)", template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.caption("데이터 없음")

        # Q10 불편사항
        st.subheader("Q10. 불편사항 빈도")
        d = dm.aggregate_dist("Q10")
        if d:
            df_b = pd.DataFrame({"보기": list(d.keys()), "비율(%)": list(d.values())})
            fig = px.bar(df_b, x="보기", y="비율(%)", template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.caption("데이터 없음")

        # Q11~Q13 변형 척도
        st.subheader("Q11~Q13. 자막 도움도 · QR 편의성 · 디지털 안내")
        for q_code in ["Q11", "Q12", "Q13"]:
            d = dm.aggregate_dist(q_code)
            if d:
                df_b = pd.DataFrame({"보기": list(d.keys()), "비율(%)": list(d.values())})
                fig = px.bar(df_b, x="보기", y="비율(%)",
                             title=f"{q_code}. {Q_BY_CODE[q_code]['label']}",
                             template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)

        st.divider()
        col_l2, col_r2 = st.columns(2)
        with col_l2:
            st.subheader("Q14. 교통수단")
            d = dm.aggregate_dist("Q14")
            if d:
                fig = px.pie(values=list(d.values()), names=list(d.keys()),
                             template="plotly_dark", hole=0.4)
                st.plotly_chart(fig, use_container_width=True)
        with col_r2:
            st.subheader("Q15. 친환경 인식")
            d = dm.aggregate_dist("Q15")
            if d:
                df_b = pd.DataFrame({"보기": list(d.keys()), "비율(%)": list(d.values())})
                fig = px.bar(df_b, x="보기", y="비율(%)", template="plotly_dark")
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Q19. 연령대")
        d = dm.aggregate_dist("Q19")
        if d:
            fig = px.pie(values=list(d.values()), names=list(d.keys()),
                         template="plotly_dark", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()
        st.subheader("🎭 장르별 평균 긍정률(Q4~Q9)")
        gd = dm.calc_genre_positive()
        if gd:
            df_g = pd.DataFrame({"장르": list(gd.keys()), "긍정률(%)": list(gd.values())})
            fig = px.bar(df_g, x="장르", y="긍정률(%)", template="plotly_dark")
            st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════
#  탭 4 — 보고서 내보내기
# ══════════════════════════════════════════════════════════════

with tab4:
    st.subheader("📤 종합 보고서 엑셀 내보내기")
    if not records:
        st.info("등록된 데이터가 없습니다.")
    else:
        if st.button("📥 엑셀 파일 생성", type="primary"):
            buf = BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                # 회차 목록
                pd.DataFrame(records).to_excel(writer, sheet_name="회차목록", index=False)

                # 회차별 분포
                dist_rows = []
                for rnd in sorted(dm.rounds.keys()):
                    for q_code in DIST_CODES:
                        d = dm.responses.get(rnd, {}).get(q_code, {})
                        if not d:
                            continue
                        pct = normalize_pct(d)
                        for opt, v in pct.items():
                            dist_rows.append({
                                "회차": rnd, "Q코드": q_code,
                                "문항": Q_BY_CODE[q_code]["label"],
                                "보기": opt, "비율(%)": v,
                            })
                if dist_rows:
                    pd.DataFrame(dist_rows).to_excel(writer, sheet_name="응답분포", index=False)

                # 긍정률 요약 (Q4~Q9, Q16~Q17)
                pos_rows = []
                for rnd in sorted(dm.rounds.keys()):
                    row = {"회차": rnd}
                    for q_code in SCALE5_CODES:
                        row[f"{q_code}.{Q_BY_CODE[q_code]['label']}"] = dm.positive_rate(rnd, q_code)
                    pos_rows.append(row)
                if pos_rows:
                    pd.DataFrame(pos_rows).to_excel(writer, sheet_name="긍정률요약", index=False)

                # Q21·Q22 주관식
                for q_code in ["Q21", "Q22"]:
                    texts = dm.collect_texts(q_code)
                    if texts:
                        pd.DataFrame(texts, columns=["회차", "내용"]).to_excel(
                            writer, sheet_name=f"{q_code}_{Q_BY_CODE[q_code]['label']}", index=False)

                # Q20 거주주소
                addr = dm.collect_texts("Q20")
                if addr:
                    pd.DataFrame(addr, columns=["회차", "주소"]).to_excel(
                        writer, sheet_name="Q20_거주주소", index=False)

            buf.seek(0)
            st.download_button(
                "💾 엑셀 다운로드",
                data=buf.getvalue(),
                file_name=f"만족도분석_보고서_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        st.divider()
        st.subheader("📄 평가 요약")
        summary = dm.calc_summary()
        st.write(f"- **등록 회차**: {summary['total_rounds']}회")
        st.write(f"- **총 응답자**: {summary['total_resp']:,}명")
        st.write(f"- **평균 긍정률(Q4~Q9)**: {summary['avg_pos']}%")
        st.write(f"- **최고 만족 회차**: {summary['max_round']}")
        st.write(f"- **최저 만족 회차**: {summary['min_round']}")
        if insuf:
            st.write(f"- ⚠ **기준치 미달**: {', '.join(str(r) + '회차' for r in insuf)}")
