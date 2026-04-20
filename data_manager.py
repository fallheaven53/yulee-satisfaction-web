# -*- coding: utf-8 -*-
"""만족도분석기 웹앱 — 데이터 매니저 (네이버폼 22문항)"""

GENRES = ["판소리·산조", "국악창작", "연희·무용", "무형유산"]

# ── 5점 척도 표준 ──
SCALE_STD = ["매우 그렇다", "그렇다", "보통", "그렇지 않다", "매우 그렇지 않다"]

# ── 22문항 정의 (네이버폼 순서) ──
QUESTIONS = [
    {"code": "Q1",  "label": "관람 회차",          "type": "round"},
    {"code": "Q2",  "label": "방문 횟수",          "type": "single",
     "options": ["처음이에요", "2~3번째", "4~5번째", "6~10번째", "10번 이상"]},
    {"code": "Q3",  "label": "정보 습득 경로",     "type": "single",
     "options": ["지인 소개", "SNS(인스타·블로그 등)", "홈페이지(재단·전통문화관)",
                 "신문·방송 등 언론보도", "현장 방문 중", "카카오톡·문자", "기타"]},
    {"code": "Q4",  "label": "전반적 만족",        "type": "scale5"},
    {"code": "Q5",  "label": "공연 재미",          "type": "scale5"},
    {"code": "Q6",  "label": "공연 감동",          "type": "scale5"},
    {"code": "Q7",  "label": "시간·구성 적절성",   "type": "scale5"},
    {"code": "Q8",  "label": "관계자 친절도",      "type": "scale5"},
    {"code": "Q9",  "label": "시작·소요시간 적절성", "type": "scale5"},
    {"code": "Q10", "label": "불편사항",           "type": "single",
     "options": ["없었다", "좌석이 불편했다", "안내가 부족했다", "접근이 불편했다", "기타"]},
    {"code": "Q11", "label": "자막·해설 도움도",   "type": "single",
     "options": ["매우 도움이 되었다", "도움이 되었다", "보통이다",
                 "도움이 되지 않았다", "보지 못했다"]},
    {"code": "Q12", "label": "QR 편의성",          "type": "single",
     "options": ["매우 편리했다", "편리했다", "보통이다", "불편했다", "이용하지 않았다"]},
    {"code": "Q13", "label": "디지털 안내 반응",   "type": "single",
     "options": ["매우 좋다", "좋다", "보통이다", "글씨가 더 크면 좋겠다", "찾기 어려웠다"]},
    {"code": "Q14", "label": "교통수단",           "type": "single",
     "options": ["도보", "자전거", "대중교통(버스·지하철)", "자가용", "택시·카풀", "기타"]},
    {"code": "Q15", "label": "친환경 인식",        "type": "single",
     "options": ["매우 중요하다", "어느 정도 고려한다", "보통이다",
                 "별로 고려하지 않는다", "전혀 고려하지 않는다"]},
    {"code": "Q16", "label": "재참여 의향",        "type": "scale5"},
    {"code": "Q17", "label": "추천 의향",          "type": "scale5"},
    {"code": "Q18", "label": "성별",               "type": "single",
     "options": ["남성", "여성"]},
    {"code": "Q19", "label": "연령대",             "type": "single",
     "options": ["10대", "20대", "30대", "40대", "50대", "60대 이상"]},
    {"code": "Q20", "label": "거주 주소",          "type": "text"},
    {"code": "Q21", "label": "좋았던 점",          "type": "free"},
    {"code": "Q22", "label": "건의사항",           "type": "free"},
]

Q_BY_CODE = {q["code"]: q for q in QUESTIONS}

# 만족도 5점 척도 문항 (긍정률 계산 대상)
SCALE5_CODES = [q["code"] for q in QUESTIONS if q["type"] == "scale5"]

# 분포형 문항 (단일선택 + 변형 5점)
DIST_CODES = [q["code"] for q in QUESTIONS if q["type"] in ("single", "scale5")]

# 주관식 문항
TEXT_CODES = [q["code"] for q in QUESTIONS if q["type"] in ("text", "free")]

POSITIVE_LEVELS = ["매우 그렇다", "그렇다"]
MIN_RESPONDENTS = 30  # 기준치


def options_of(q_code):
    """문항의 보기 목록 반환 (5점척도는 표준 5단계)"""
    q = Q_BY_CODE.get(q_code)
    if not q:
        return []
    if q["type"] == "scale5":
        return list(SCALE_STD)
    return list(q.get("options", []))


def normalize_pct(dist):
    """{보기: 값} → {보기: %} (합계 100 기준 정규화)"""
    if not dist:
        return {}
    total = sum(v for v in dist.values() if v)
    if total <= 0:
        return {k: 0.0 for k in dist}
    return {k: round(v / total * 100, 1) for k, v in dist.items()}


class SatisfactionManager:
    QUESTIONS = QUESTIONS
    Q_BY_CODE = Q_BY_CODE

    def __init__(self, gsheet_sync=None):
        self.rounds = {}        # {회차: {공연일, 출연단체, 장르, 응답자수, 보충}}
        self.responses = {}     # {회차: {Q코드: {보기: 값}}}
        self.texts = {}         # {회차: {Q코드: [문자열, ...]}}
        self.gsheet = gsheet_sync
        if self.gsheet:
            self.gsheet.download_all(self)

    def _sync(self):
        if self.gsheet:
            self.gsheet.upload_all(self)

    # ── CRUD ──
    def save_round_info(self, rnd, info):
        self.rounds[rnd] = info
        self._sync()

    def save_responses(self, rnd, resp_dict):
        """{Q코드: {보기: 값}} 통째로 저장"""
        self.responses[rnd] = resp_dict
        self._sync()

    def save_texts(self, rnd, text_dict):
        """{Q코드: [문자열,...]} 통째로 저장"""
        self.texts[rnd] = text_dict
        self._sync()

    def delete_round(self, rnd):
        self.rounds.pop(rnd, None)
        self.responses.pop(rnd, None)
        self.texts.pop(rnd, None)
        self._sync()

    # ── 집계 ──
    def positive_rate(self, rnd, q_code):
        """특정 회차·문항의 긍정응답률(매우 그렇다 + 그렇다)"""
        d = self.responses.get(rnd, {}).get(q_code, {})
        if not d:
            return 0.0
        pct = normalize_pct(d)
        return round(sum(pct.get(lv, 0) for lv in POSITIVE_LEVELS), 1)

    def round_overall_positive(self, rnd):
        """회차의 5점 척도 6개 문항(Q4~Q9) 평균 긍정률"""
        rates = [self.positive_rate(rnd, c) for c in ["Q4","Q5","Q6","Q7","Q8","Q9"]
                 if self.responses.get(rnd, {}).get(c)]
        return round(sum(rates) / len(rates), 1) if rates else 0.0

    def get_round_records(self):
        records = []
        for rnd in sorted(self.rounds.keys()):
            r = self.rounds[rnd]
            records.append({
                "회차": rnd,
                "공연일": r.get("공연일", ""),
                "출연단체": r.get("출연단체", ""),
                "장르": r.get("장르", ""),
                "응답자수": r.get("응답자수", 0),
                "긍정응답률(%)": self.round_overall_positive(rnd),
                "보충": "Y" if r.get("보충", False) else "",
            })
        return records

    def calc_summary(self):
        records = self.get_round_records()
        active = [r for r in records if r["응답자수"] > 0]
        total_resp = sum(r["응답자수"] for r in active)
        pos_rates = [r["긍정응답률(%)"] for r in active if r["긍정응답률(%)"] > 0]
        avg_pos = round(sum(pos_rates) / len(pos_rates), 1) if pos_rates else 0
        max_rec = max((r for r in active if r["긍정응답률(%)"] > 0),
                      key=lambda x: x["긍정응답률(%)"], default=None)
        min_rec = min((r for r in active if r["긍정응답률(%)"] > 0),
                      key=lambda x: x["긍정응답률(%)"], default=None)
        return {
            "total_resp": total_resp,
            "avg_pos": avg_pos,
            "max_round": f"{max_rec['회차']}회차 ({max_rec['긍정응답률(%)']}%)" if max_rec else "-",
            "min_round": f"{min_rec['회차']}회차 ({min_rec['긍정응답률(%)']}%)" if min_rec else "-",
            "total_rounds": len(active),
        }

    def calc_genre_positive(self):
        """장르별 평균 긍정응답률(Q4~Q9)"""
        result = {}
        for rnd in self.rounds:
            genre = self.rounds[rnd].get("장르", "")
            if not genre:
                continue
            pos = self.round_overall_positive(rnd)
            if pos == 0:
                continue
            result.setdefault(genre, []).append(pos)
        return {g: round(sum(v) / len(v), 1) for g, v in result.items() if v}

    def aggregate_dist(self, q_code):
        """전체 회차 합산 분포 (정규화된 %)"""
        agg = {}
        for rnd, resp in self.responses.items():
            d = resp.get(q_code)
            if not d:
                continue
            pct = normalize_pct(d)
            for k, v in pct.items():
                agg[k] = agg.get(k, 0) + v
        # 평균
        cnt = sum(1 for r, resp in self.responses.items() if resp.get(q_code))
        if cnt > 0:
            agg = {k: round(v / cnt, 1) for k, v in agg.items()}
        return agg

    def positive_trend(self, q_code):
        """회차별 긍정률 추이 [(회차, %)]"""
        out = []
        for rnd in sorted(self.rounds.keys()):
            if self.responses.get(rnd, {}).get(q_code):
                out.append((rnd, self.positive_rate(rnd, q_code)))
        return out

    def collect_texts(self, q_code):
        """회차별 주관식 응답 [(회차, 텍스트), ...]"""
        out = []
        for rnd in sorted(self.texts.keys()):
            for t in self.texts[rnd].get(q_code, []):
                if t and str(t).strip():
                    out.append((rnd, str(t).strip()))
        return out

    def insufficient_rounds(self):
        return [rnd for rnd, r in self.rounds.items()
                if r.get("응답자수", 0) < MIN_RESPONDENTS]

    def fill_insufficient(self, rnd):
        """기존 회차 평균을 기반으로 부족 회차 보충"""
        new_resp = {}
        for q_code in DIST_CODES:
            agg = self.aggregate_dist(q_code)
            if any(agg.values()):
                new_resp[q_code] = agg
        if new_resp:
            self.responses[rnd] = new_resp
        if rnd in self.rounds:
            self.rounds[rnd]["보충"] = True
        self._sync()
