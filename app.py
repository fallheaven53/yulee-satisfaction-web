# -*- coding: utf-8 -*-
"""
율이공방 — 만족도분석기 웹앱
2026 토요상설공연 만족도조사 등록·분석
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO

st.set_page_config(
    page_title="율이공방 — 만족도분석기",
    page_icon="📋",
    layout="wide",
)

from data_manager import (
    SatisfactionManager, GENRES, SATISFACTION_ITEMS,
    SCALE_LEVELS, DEMO_CATEGORIES, MIN_RESPONDENTS
)

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
    result = {}  # {회차: (단체명, 공연일)}
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
#  CSV 자동 파싱 (네이버폼)
# ══════════════════════════════════════════════════════════════

def parse_naver_csv(file_bytes):
    """네이버폼 CSV → {회차: {Q코드: {레벨: %}}, ...}"""
    try:
        df = pd.read_csv(BytesIO(file_bytes), encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_csv(BytesIO(file_bytes), encoding="cp949")
        except Exception as e:
            return None, str(e)

    # 회차 컬럼 찾기
    rnd_col = None
    for col in df.columns:
        if "회차" in str(col):
            rnd_col = col
            break
    if not rnd_col:
        return None, "회차 컬럼을 찾을 수 없습니다"

    # 회차 정수화
    def parse_rnd(v):
        if pd.isna(v):
            return None
        s = str(v)
        import re
        m = re.search(r"\d+", s)
        return int(m.group()) if m else None

    df["_rnd"] = df[rnd_col].apply(parse_rnd)
    df = df[df["_rnd"].notna()]

    # 만족도 항목 매칭 (키워드)
    item_keywords = {
        "Q4": ["감동"],
        "Q5": ["시작.*시간", "시작시간"],
        "Q6": ["소요.*시간", "소요시간"],
        "Q7": ["재미"],
        "Q8": ["기대"],
        "Q9": ["작품성"],
        "Q10": ["편리"],
        "Q11": ["친절"],
        "Q12": ["만족"],
        "Q13": ["추천"],
        "Q14": ["향유", "기여"],
    }

    import re
    matched_cols = {}
    for q_code, kws in item_keywords.items():
        for col in df.columns:
            col_str = str(col)
            for kw in kws:
                if re.search(kw, col_str):
                    matched_cols[q_code] = col
                    break
            if q_code in matched_cols:
                break

    # 5점 척도 정규화
    scale_map = {}
    for v5, v4, v3, v2, v1 in [
        ("매우 그렇다", "그렇다", "보통이다", "그렇지 않다", "매우 그렇지 않다"),
        ("매우 그렇다", "그렇다", "보통이다", "그렇지 않다", "전혀 그렇지 않다"),
        ("매우 만족", "만족", "보통", "불만족", "매우 불만족"),
    ]:
        scale_map[v5] = "매우그렇다"; scale_map[v4] = "그렇다"
        scale_map[v3] = "보통"; scale_map[v2] = "그렇지않다"
        scale_map[v1] = "매우그렇지않다"

    result = {}
    counts = {}

    for rnd, group in df.groupby("_rnd"):
        rnd = int(rnd)
        counts[rnd] = len(group)
        result[rnd] = {}
        for q_code, col in matched_cols.items():
            level_counts = {lv: 0 for lv in SCALE_LEVELS}
            for v in group[col]:
                if pd.isna(v):
                    continue
                normalized = scale_map.get(str(v).strip())
                if normalized:
                    level_counts[normalized] += 1
            total = sum(level_counts.values())
            if total > 0:
                result[rnd][q_code] = {
                    lv: round(level_counts[lv] / total * 100, 1)
                    for lv in SCALE_LEVELS
                }

    return (result, counts), None


# ══════════════════════════════════════════════════════════════
#  탭 1: 만족도 데이터 입력·관리
# ══════════════════════════════════════════════════════════════

def render_tab_satisfaction():
    dm = get_dm()
    target_dates = load_target_dates()

    st.subheader("📋 만족도 데이터 입력")

    # ── CSV 업로드 ──
    with st.expander("📁 네이버폼 CSV 불러오기", expanded=False):
        upload = st.file_uploader("CSV 파일 선택", type=["csv"], key="csv_up")
        if upload:
            data, err = parse_naver_csv(upload.getvalue())
            if err:
                st.error(f"파싱 오류: {err}")
            else:
                sat_data, counts = data
                st.success(f"파싱 완료: {len(sat_data)}개 회차")
                st.dataframe(pd.DataFrame([
                    {"회차": r, "응답자수": counts[r],
                     "항목수": len(sat_data[r])}
                    for r in sorted(sat_data.keys())
                ]), hide_index=True)
                if st.button("⬇ 일괄 등록", type="primary"):
                    for rnd in sat_data:
                        # 회차정보 (없으면 자동 생성)
                        if rnd not in dm.rounds:
                            tg = target_dates.get(rnd, ("", ""))
                            dm.rounds[rnd] = {
                                "공연일": tg[1],
                                "출연단체": tg[0],
                                "장르": "",
                                "응답자수": counts[rnd],
                                "보충": False,
                            }
                        else:
                            dm.rounds[rnd]["응답자수"] = counts[rnd]
                        dm.satisfaction[rnd] = sat_data[rnd]
                    dm._sync()
                    st.success("등록 완료!")
                    st.rerun()

    # ── 수동 입력 폼 ──
    edit_mode = st.session_state.get("sat_edit_mode", False)
    edit_rnd = st.session_state.get("sat_edit_rnd", None)

    with st.form("sat_form", clear_on_submit=not edit_mode):
        st.markdown(f"### {'✏ ' + str(edit_rnd) + '회차 수정' if edit_mode else '➕ 수동 등록'}")
        c1, c2, c3, c4 = st.columns([1, 2, 2, 1])
        with c1:
            rnd_options = list(range(1, 25))
            default_rnd = rnd_options.index(edit_rnd) if edit_mode and edit_rnd in rnd_options else 0
            rnd = st.selectbox("회차", rnd_options, index=default_rnd, key="f_rnd")

        # 자동 매칭
        auto_target, auto_date = target_dates.get(rnd, ("", ""))

        if edit_mode and edit_rnd in dm.rounds:
            r = dm.rounds[edit_rnd]
            def_target = r.get("출연단체", "")
            def_date = r.get("공연일", "")
            def_genre = r.get("장르", "")
            def_resp = r.get("응답자수", 0)
        else:
            def_target = auto_target
            def_date = auto_date
            def_genre = ""
            def_resp = 0

        with c2:
            target = st.text_input("출연단체", value=def_target, key="f_target")
        with c3:
            date_str = st.text_input("공연일", value=def_date, key="f_date")
        with c4:
            genre_opts = [""] + GENRES
            g_idx = genre_opts.index(def_genre) if def_genre in genre_opts else 0
            genre = st.selectbox("장르", genre_opts, index=g_idx, key="f_genre")

        respondents = st.number_input("응답자 수", min_value=0,
                                      value=def_resp, key="f_resp")

        st.markdown("**📊 만족도 항목별 비율 (단위: %)**")
        st.caption("각 항목별로 5단계 척도의 비율을 입력 (합계 100% 권장)")

        sat_inputs = {}
        save_rnd = edit_rnd if edit_mode else rnd
        existing_sat = dm.satisfaction.get(save_rnd, {})

        for q_code, q_label in SATISFACTION_ITEMS:
            st.markdown(f"**{q_code}. {q_label}**")
            cs = st.columns(5)
            ex = existing_sat.get(q_code, {})
            vals = {}
            for i, lv in enumerate(SCALE_LEVELS):
                with cs[i]:
                    vals[lv] = st.number_input(
                        lv, min_value=0.0, max_value=100.0,
                        value=float(ex.get(lv, 0)), step=1.0,
                        key=f"f_{q_code}_{lv}",
                        label_visibility="visible",
                    )
            sat_inputs[q_code] = vals

        fc1, fc2 = st.columns(2)
        with fc1:
            submitted = st.form_submit_button(
                "수정 저장" if edit_mode else "등록",
                use_container_width=True, type="primary")
        with fc2:
            if edit_mode:
                cancel = st.form_submit_button("취소", use_container_width=True)
            else:
                cancel = False

    if cancel:
        st.session_state["sat_edit_mode"] = False
        st.session_state["sat_edit_rnd"] = None
        st.rerun()

    if submitted:
        rnd_to_save = edit_rnd if edit_mode else rnd
        dm.rounds[rnd_to_save] = {
            "공연일": date_str,
            "출연단체": target,
            "장르": genre,
            "응답자수": respondents,
            "보충": dm.rounds.get(rnd_to_save, {}).get("보충", False),
        }
        # 0이 아닌 항목만 저장
        non_zero_sat = {q: v for q, v in sat_inputs.items()
                        if any(val > 0 for val in v.values())}
        if non_zero_sat:
            dm.satisfaction[rnd_to_save] = non_zero_sat
        dm._sync()
        st.session_state["sat_edit_mode"] = False
        st.session_state["sat_edit_rnd"] = None
        st.success(f"{rnd_to_save}회차 저장 완료!")
        st.rerun()

    # ── 등록 회차 목록 ──
    st.divider()
    st.subheader("📜 등록된 회차")
    records = dm.get_round_records()
    if records:
        df = pd.DataFrame(records)
        sel = st.dataframe(df, use_container_width=True, hide_index=True,
                           on_select="rerun", selection_mode="single-row",
                           key="sat_table")
        selected = sel.get("selection", {}).get("rows", [])
        if selected:
            sel_idx = selected[0]
            if sel_idx >= len(records):
                st.rerun()
                return
            sel_rec = records[sel_idx]
            bc1, bc2 = st.columns(2)
            with bc1:
                if st.button("수정", key="sat_edit_btn", use_container_width=True):
                    st.session_state["sat_edit_mode"] = True
                    st.session_state["sat_edit_rnd"] = sel_rec["회차"]
                    st.rerun()
            with bc2:
                if st.button("삭제", key="sat_del_btn",
                             use_container_width=True, type="primary"):
                    dm.delete_round(sel_rec["회차"])
                    st.success("삭제 완료!")
                    st.rerun()

        # 부족 회차 경고
        insuf = dm.insufficient_rounds()
        if insuf:
            st.warning(f"⚠ 응답자 {MIN_RESPONDENTS}명 미만 회차: {insuf}")
            with st.expander("기존 평균 기반 자동 보충"):
                fill_rnd = st.selectbox("보충 회차 선택", insuf, key="fill_sel")
                if st.button("자동 보충 실행", type="primary", key="fill_btn"):
                    dm.fill_insufficient(fill_rnd)
                    st.success(f"{fill_rnd}회차 보충 완료 (보충 데이터로 표시됨)")
                    st.rerun()

        # 엑셀 내보내기
        buf = BytesIO()
        df.to_excel(buf, index=False, engine="openpyxl")
        st.download_button("📥 엑셀 다운로드", data=buf.getvalue(),
                           file_name=f"만족도_{datetime.now():%Y%m%d}.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    else:
        st.info("등록된 만족도 데이터가 없습니다.")


# ══════════════════════════════════════════════════════════════
#  탭 2: 인구통계 입력·관리
# ══════════════════════════════════════════════════════════════

def render_tab_demographics():
    dm = get_dm()

    st.subheader("👥 인구통계 데이터 입력")

    rnd_options = sorted(dm.rounds.keys()) if dm.rounds else list(range(1, 25))
    rnd = st.selectbox("회차 선택", rnd_options, key="dm_rnd")

    existing = dm.demographics.get(rnd, {})

    new_demo = {}
    with st.form("demo_form"):
        st.caption("각 카테고리별로 항목별 비율(%)을 입력하세요")
        for cat, items in DEMO_CATEGORIES.items():
            st.markdown(f"**{cat}**")
            ex_cat = existing.get(cat, {})
            cols = st.columns(len(items))
            cat_data = {}
            for i, item in enumerate(items):
                with cols[i]:
                    cat_data[item] = st.number_input(
                        item, min_value=0.0, max_value=100.0,
                        value=float(ex_cat.get(item, 0)), step=1.0,
                        key=f"d_{cat}_{item}",
                    )
            new_demo[cat] = cat_data

        submitted = st.form_submit_button("저장", type="primary",
                                          use_container_width=True)

    if submitted:
        # 0이 아닌 항목만 저장
        clean_demo = {}
        for cat, items in new_demo.items():
            non_zero = {k: v for k, v in items.items() if v > 0}
            if non_zero:
                clean_demo[cat] = non_zero
        if clean_demo:
            dm.save_demographics(rnd, clean_demo)
            st.success(f"{rnd}회차 인구통계 저장 완료!")
            st.rerun()
        else:
            st.warning("입력된 데이터가 없습니다.")

    # 현재 등록된 인구통계 표시
    if dm.demographics:
        st.divider()
        st.subheader("📜 등록된 인구통계 회차")
        st.write(f"총 {len(dm.demographics)}개 회차: {sorted(dm.demographics.keys())}")


# ══════════════════════════════════════════════════════════════
#  탭 3: 분석 대시보드
# ══════════════════════════════════════════════════════════════

def render_tab_dashboard():
    import plotly.graph_objects as go

    dm = get_dm()
    summary = dm.calc_summary()

    # 핵심 지표
    st.subheader("📊 핵심 지표")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("총 응답자 수", f"{summary['total_resp']:,}명")
    c2.metric("총 회차", f"{summary['total_rounds']}회")
    c3.metric("평균 긍정응답률", f"{summary['avg_pos']}%")
    c4.metric("최고 만족 회차", summary["max_round"])
    c5.metric("최저 만족 회차", summary["min_round"])

    if not dm.satisfaction:
        st.info("등록된 만족도 데이터가 없습니다.")
        return

    st.divider()

    # 항목별 회차 추이
    st.subheader("📈 만족도 항목별 회차 추이")
    sat_rounds = sorted(dm.satisfaction.keys())
    fig = go.Figure()
    for q_code, q_label in SATISFACTION_ITEMS:
        ys = []
        for r in sat_rounds:
            if q_code in dm.satisfaction.get(r, {}):
                vals = dm.satisfaction[r][q_code]
                pos = vals.get("매우그렇다", 0) + vals.get("그렇다", 0)
                ys.append(pos)
            else:
                ys.append(None)
        fig.add_trace(go.Scatter(
            x=[f"{r}회차" for r in sat_rounds],
            y=ys, mode="lines+markers",
            name=q_label,
        ))
    fig.update_layout(
        yaxis_title="긍정응답률(%)",
        height=450, template="plotly_dark",
        legend=dict(orientation="h", y=-0.2),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # 누적 항목별 비율 표
    st.subheader("📋 전체 회차 누적 항목별 비율")
    table_data = []
    for q_code, q_label in SATISFACTION_ITEMS:
        agg = dm.calc_item_avg(q_code)
        if any(agg.values()):
            row = {"항목": q_label}
            row.update(agg)
            row["긍정응답률(%)"] = round(agg["매우그렇다"] + agg["그렇다"], 1)
            table_data.append(row)
    if table_data:
        st.dataframe(pd.DataFrame(table_data), use_container_width=True,
                     hide_index=True)

    st.divider()

    # 장르별 만족도
    st.subheader("🎭 장르별 만족도 비교")
    genre_data = dm.calc_genre_satisfaction()
    if genre_data:
        fig_g = go.Figure()
        fig_g.add_trace(go.Bar(
            x=list(genre_data.keys()),
            y=list(genre_data.values()),
            marker_color=["#89B4FA", "#A6E3A1", "#FAB387", "#CBA6F7"],
        ))
        fig_g.update_layout(
            yaxis_title="평균 긍정응답률(%)",
            height=350, template="plotly_dark",
        )
        st.plotly_chart(fig_g, use_container_width=True)

    st.divider()

    # 인구통계 파이차트
    if dm.demographics:
        st.subheader("👥 인구통계 (전체 회차 평균)")
        c1, c2 = st.columns(2)
        with c1:
            age_data = dm.calc_demographic_total("연령")
            if any(age_data.values()):
                fig_a = go.Figure(data=[go.Pie(
                    labels=list(age_data.keys()),
                    values=list(age_data.values()),
                    hole=0.4,
                )])
                fig_a.update_layout(
                    title="연령대 구성", template="plotly_dark", height=350,
                )
                st.plotly_chart(fig_a, use_container_width=True)
        with c2:
            info_data = dm.calc_demographic_total("정보습득경로")
            if any(info_data.values()):
                fig_i = go.Figure(data=[go.Pie(
                    labels=list(info_data.keys()),
                    values=list(info_data.values()),
                    hole=0.4,
                )])
                fig_i.update_layout(
                    title="정보 습득 경로", template="plotly_dark", height=350,
                )
                st.plotly_chart(fig_i, use_container_width=True)


# ══════════════════════════════════════════════════════════════
#  탭 4: 보고서 내보내기
# ══════════════════════════════════════════════════════════════

def render_tab_export():
    dm = get_dm()
    summary = dm.calc_summary()

    st.subheader("📑 보고서 내보내기")

    # 종합 보고서
    st.markdown("### 📊 연간 만족도 종합 보고서")
    if st.button("종합 보고서 엑셀 생성", type="primary", use_container_width=True):
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            # 요약
            summary_df = pd.DataFrame([
                {"항목": "총 응답자 수", "값": summary["total_resp"]},
                {"항목": "총 회차", "값": summary["total_rounds"]},
                {"항목": "평균 긍정응답률(%)", "값": summary["avg_pos"]},
                {"항목": "최고 만족 회차", "값": summary["max_round"]},
                {"항목": "최저 만족 회차", "값": summary["min_round"]},
            ])
            summary_df.to_excel(writer, sheet_name="요약", index=False)

            # 회차별
            round_df = pd.DataFrame(dm.get_round_records())
            round_df.to_excel(writer, sheet_name="회차별", index=False)

            # 항목별 누적
            item_data = []
            for q_code, q_label in SATISFACTION_ITEMS:
                agg = dm.calc_item_avg(q_code)
                if any(agg.values()):
                    row = {"항목": q_label}
                    row.update(agg)
                    row["긍정응답률(%)"] = round(agg["매우그렇다"] + agg["그렇다"], 1)
                    item_data.append(row)
            if item_data:
                pd.DataFrame(item_data).to_excel(writer, sheet_name="항목별누적", index=False)

        st.download_button(
            "📥 다운로드", buf.getvalue(),
            file_name=f"만족도_종합보고서_{datetime.now():%Y%m%d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    st.divider()

    # 정부합동평가용 1장 요약
    st.markdown("### 📃 정부합동평가용 1장 요약")
    if st.button("1장 요약 생성", use_container_width=True):
        buf = BytesIO()
        eval_df = pd.DataFrame([
            {"지표": "토요상설공연 만족도조사 결과",
             "값": f"평균 긍정응답률 {summary['avg_pos']}% / 응답자 {summary['total_resp']}명 / {summary['total_rounds']}회차"},
        ])
        eval_df.to_excel(buf, index=False, engine="openpyxl")
        st.download_button(
            "📥 다운로드", buf.getvalue(),
            file_name=f"만족도_평가요약_{datetime.now():%Y%m%d}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ══════════════════════════════════════════════════════════════
#  메인
# ══════════════════════════════════════════════════════════════

def main():
    st.title("📋 율이공방 — 만족도분석기")
    st.caption("2026 토요상설공연 만족도조사 등록·분석")

    with st.sidebar:
        st.header("설정")
        if st.button("🔄 구글 시트 새로고침", use_container_width=True):
            reload_dm()
        st.divider()
        dm = get_dm()
        st.metric("등록 회차", len(dm.rounds))
        st.metric("총 응답자", f"{dm.calc_summary()['total_resp']:,}명")

    tab1, tab2, tab3, tab4 = st.tabs([
        "📋 만족도 입력·관리",
        "👥 인구통계 입력",
        "📊 분석 대시보드",
        "📑 보고서 내보내기",
    ])

    with tab1:
        render_tab_satisfaction()
    with tab2:
        render_tab_demographics()
    with tab3:
        render_tab_dashboard()
    with tab4:
        render_tab_export()


if __name__ == "__main__":
    main()
