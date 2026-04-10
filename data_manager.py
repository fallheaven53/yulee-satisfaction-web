# -*- coding: utf-8 -*-
"""만족도분석기 웹앱 — 데이터 매니저"""

GENRES = ["판소리·산조", "국악창작", "연희·무용", "무형유산"]

# 만족도 항목 (5점 척도)
SATISFACTION_ITEMS = [
    ("Q4", "공연 감동"),
    ("Q5", "시작시간 적절성"),
    ("Q6", "소요시간 적절성"),
    ("Q7", "공연의 재미"),
    ("Q8", "공연에 대한 기대"),
    ("Q9", "공연 작품성"),
    ("Q10", "시설 편리성"),
    ("Q11", "관계자 친절도"),
    ("Q12", "공연 만족도"),
    ("Q13", "추천 가능성"),
    ("Q14", "문화예술 향유 기여도"),
]

SCALE_LEVELS = ["매우그렇다", "그렇다", "보통", "그렇지않다", "매우그렇지않다"]

# 인구통계 카테고리 및 항목
DEMO_CATEGORIES = {
    "연령": ["10대", "20대", "30대", "40대", "50대", "60대 이상"],
    "성별": ["남", "여"],
    "동행인원": ["1명", "2명", "3명", "4명", "5명 이상"],
    "이동거리": ["1km 미만", "1~5km", "5~10km", "10~20km", "20km 이상"],
    "교통수단": ["도보·자전거", "대중교통", "자가용", "기타"],
    "정보습득경로": ["문자서비스", "신문·방송", "지인소개", "기타"],
    "친환경교통인식": ["매우중요", "중요", "보통", "중요하지않음"],
}

MIN_RESPONDENTS = 30  # 기준치


class SatisfactionManager:
    Q_LABELS = {code: label for code, label in SATISFACTION_ITEMS}

    def __init__(self, gsheet_sync=None):
        self.rounds = {}        # {회차: {공연일, 출연단체, 장르, 응답자수, 보충}}
        self.satisfaction = {}  # {회차: {Q코드: {매우그렇다, 그렇다, ...}}}
        self.demographics = {}  # {회차: {카테고리: {항목: 비율}}}
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

    def save_satisfaction(self, rnd, q_code, values):
        if rnd not in self.satisfaction:
            self.satisfaction[rnd] = {}
        self.satisfaction[rnd][q_code] = values
        self._sync()

    def save_satisfaction_bulk(self, rnd, sat_dict):
        """{Q코드: {매우그렇다, ...}} 통째로 저장"""
        self.satisfaction[rnd] = sat_dict
        self._sync()

    def save_demographics(self, rnd, demo_dict):
        """{카테고리: {항목: 비율}} 통째로 저장"""
        self.demographics[rnd] = demo_dict
        self._sync()

    def delete_round(self, rnd):
        self.rounds.pop(rnd, None)
        self.satisfaction.pop(rnd, None)
        self.demographics.pop(rnd, None)
        self._sync()

    # ── 집계 ──
    def get_round_records(self):
        records = []
        for rnd in sorted(self.rounds.keys()):
            r = self.rounds[rnd]
            sat = self.satisfaction.get(rnd, {})
            avg_pos = self.calc_positive_rate(rnd)
            records.append({
                "회차": rnd,
                "공연일": r.get("공연일", ""),
                "출연단체": r.get("출연단체", ""),
                "장르": r.get("장르", ""),
                "응답자수": r.get("응답자수", 0),
                "긍정응답률(%)": round(avg_pos, 1),
                "보충": "Y" if r.get("보충", False) else "",
            })
        return records

    def calc_positive_rate(self, rnd):
        """매우그렇다 + 그렇다 평균 (해당 회차 모든 항목)"""
        sat = self.satisfaction.get(rnd, {})
        if not sat:
            return 0
        total = 0
        cnt = 0
        for q_code, vals in sat.items():
            pos = vals.get("매우그렇다", 0) + vals.get("그렇다", 0)
            total += pos
            cnt += 1
        return total / cnt if cnt > 0 else 0

    def calc_item_avg(self, q_code):
        """전체 회차에 대한 한 항목의 5단계 평균"""
        agg = {lv: 0 for lv in SCALE_LEVELS}
        cnt = 0
        for rnd, sat in self.satisfaction.items():
            if q_code in sat:
                for lv in SCALE_LEVELS:
                    agg[lv] += sat[q_code].get(lv, 0)
                cnt += 1
        if cnt > 0:
            for lv in SCALE_LEVELS:
                agg[lv] = round(agg[lv] / cnt, 1)
        return agg

    def calc_summary(self):
        records = self.get_round_records()
        total_resp = sum(r["응답자수"] for r in records)
        pos_rates = [r["긍정응답률(%)"] for r in records if r["긍정응답률(%)"] > 0]
        avg_pos = round(sum(pos_rates) / len(pos_rates), 1) if pos_rates else 0
        max_rec = max(records, key=lambda x: x["긍정응답률(%)"]) if records else None
        min_rec = min((r for r in records if r["긍정응답률(%)"] > 0),
                      key=lambda x: x["긍정응답률(%)"], default=None)
        return {
            "total_resp": total_resp,
            "avg_pos": avg_pos,
            "max_round": f"{max_rec['회차']}회차 ({max_rec['긍정응답률(%)']}%)" if max_rec else "-",
            "min_round": f"{min_rec['회차']}회차 ({min_rec['긍정응답률(%)']}%)" if min_rec else "-",
            "total_rounds": len(records),
        }

    def calc_genre_satisfaction(self):
        """장르별 평균 긍정응답률"""
        result = {}
        for rnd in self.rounds:
            genre = self.rounds[rnd].get("장르", "")
            if not genre:
                continue
            pos = self.calc_positive_rate(rnd)
            if pos == 0:
                continue
            if genre not in result:
                result[genre] = []
            result[genre].append(pos)
        return {g: round(sum(v) / len(v), 1) for g, v in result.items() if v}

    def calc_demographic_total(self, category):
        """전체 회차 합산 인구통계 (카테고리별)"""
        if category not in DEMO_CATEGORIES:
            return {}
        agg = {item: 0 for item in DEMO_CATEGORIES[category]}
        cnt = 0
        for rnd, demo in self.demographics.items():
            if category in demo:
                for item, ratio in demo[category].items():
                    if item in agg:
                        agg[item] += ratio
                cnt += 1
        if cnt > 0:
            for item in agg:
                agg[item] = round(agg[item] / cnt, 1)
        return agg

    def insufficient_rounds(self):
        """기준치 미달 회차 목록"""
        return [rnd for rnd, r in self.rounds.items()
                if r.get("응답자수", 0) < MIN_RESPONDENTS]

    def fill_insufficient(self, rnd):
        """기존 회차 평균을 기반으로 부족 회차 보충"""
        # 만족도 평균
        new_sat = {}
        for q_code, _ in SATISFACTION_ITEMS:
            agg = self.calc_item_avg(q_code)
            if any(agg.values()):
                new_sat[q_code] = agg
        if new_sat:
            self.satisfaction[rnd] = new_sat
        # 회차 정보에 보충 플래그
        if rnd in self.rounds:
            self.rounds[rnd]["보충"] = True
        self._sync()
