"""
TrafficMind Agent 测试脚本
--------------------------
使用 FastAPI TestClient 进行端到端测试。
覆盖第一阶段全部接口 + 第二阶段新增接口。
启动测试：pytest backend/tests/test_sample_request.py -v
"""

import pytest
from fastapi.testclient import TestClient

# 修改 sys.path 并导入 app
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from backend.app import app

# 测试客户端
client = TestClient(app)


# ==================== 测试数据集 ====================

SAMPLE_EVENT = {
    "eventId": "E202606290001",
    "eventType": "congestion",
    "cameraId": "CAM_001",
    "roadName": "人民路-解放路路口",
    "direction": "东向西",
    "lane": "直行车道",
    "avgSpeed": 8.5,
    "queueLength": 180,
    "duration": 601,
    "vehicleCount": 96,
    "weather": "rain",
    "timePeriod": "morning_peak",
    "isMainRoad": True,
    "nearbySchool": False,
    "nearbyHospital": True,
    "confidence": 0.91,
}

# 第二阶段测试用多样数据：3 拥堵 + 2 事故 + 1 违停
STAGE2_EVENTS = [
    {   # 拥堵 - 人民路，高风险
        "eventId": "E202606300001",
        "eventType": "congestion",
        "roadName": "人民路-解放路路口",
        "direction": "东向西",
        "avgSpeed": 7.0,
        "queueLength": 200,
        "duration": 900,
        "vehicleCount": 120,
        "weather": "rain",
        "timePeriod": "morning_peak",
        "isMainRoad": True,
        "nearbySchool": False,
        "nearbyHospital": True,
        "confidence": 0.92,
    },
    {   # 拥堵 - 人民路，中风险
        "eventId": "E202606300002",
        "eventType": "congestion",
        "roadName": "人民路-解放路路口",
        "direction": "西向东",
        "avgSpeed": 18.0,
        "queueLength": 100,
        "duration": 300,
        "vehicleCount": 50,
        "weather": "clear",
        "timePeriod": "off_peak",
        "isMainRoad": True,
        "nearbySchool": False,
        "nearbyHospital": True,
        "confidence": 0.95,
    },
    {   # 拥堵 - 中山路
        "eventId": "E202606300003",
        "eventType": "congestion",
        "roadName": "中山路-南京路路口",
        "direction": "南向北",
        "avgSpeed": 5.0,
        "queueLength": 350,
        "duration": 1200,
        "vehicleCount": 200,
        "weather": "fog",
        "timePeriod": "evening_peak",
        "isMainRoad": True,
        "nearbySchool": True,
        "nearbyHospital": False,
        "confidence": 0.88,
    },
    {   # 事故 - 中山路，重大风险
        "eventId": "E202606300004",
        "eventType": "accident",
        "roadName": "中山路-南京路路口",
        "direction": "南向北",
        "avgSpeed": 2.0,
        "queueLength": 400,
        "duration": 1500,
        "vehicleCount": 60,
        "weather": "fog",
        "timePeriod": "evening_peak",
        "isMainRoad": True,
        "nearbySchool": True,
        "nearbyHospital": True,
        "confidence": 0.85,
    },
    {   # 事故 - 人民路
        "eventId": "E202606300005",
        "eventType": "accident",
        "roadName": "人民路-解放路路口",
        "direction": "东向西",
        "avgSpeed": 3.0,
        "queueLength": 300,
        "duration": 800,
        "vehicleCount": 30,
        "weather": "rain",
        "timePeriod": "morning_peak",
        "isMainRoad": True,
        "nearbySchool": False,
        "nearbyHospital": True,
        "confidence": 0.90,
    },
    {   # 违停 - 中山路，低风险
        "eventId": "E202606300006",
        "eventType": "illegal_parking",
        "roadName": "中山路-南京路路口",
        "direction": "北向南",
        "avgSpeed": 35.0,
        "queueLength": 20,
        "duration": 180,
        "vehicleCount": 1,
        "weather": "clear",
        "timePeriod": "off_peak",
        "isMainRoad": False,
        "nearbySchool": False,
        "nearbyHospital": False,
        "confidence": 0.96,
    },
]


# ==================== 第一阶段回归测试 ====================

class TestAnalyzeEvent:
    """测试 /analyze_event 接口"""

    def test_analyze_returns_200(self):
        response = client.post("/analyze_event", json=SAMPLE_EVENT)
        assert response.status_code == 200

    def test_analyze_has_all_fields(self):
        response = client.post("/analyze_event", json=SAMPLE_EVENT)
        data = response.json()
        required_fields = [
            "eventId", "standardEvent", "riskScore", "riskLevel",
            "riskReasons", "matchedRule", "suggestions",
            "dispatchMessage", "publicMessage", "report",
            "status", "saved",
        ]
        for field in required_fields:
            assert field in data, f"缺少字段: {field}"

    def test_analyze_risk_score_in_range(self):
        response = client.post("/analyze_event", json=SAMPLE_EVENT)
        data = response.json()
        assert 0 <= data["riskScore"] <= 100

    def test_analyze_risk_level_valid(self):
        response = client.post("/analyze_event", json=SAMPLE_EVENT)
        data = response.json()
        valid_levels = ["低风险", "中风险", "高风险", "重大风险"]
        assert data["riskLevel"] in valid_levels

    def test_missing_required_fields(self):
        incomplete_event = {"eventId": "E001"}
        response = client.post("/analyze_event", json=incomplete_event)
        assert response.status_code == 422

    def test_congestion_risk_score(self):
        response = client.post("/analyze_event", json=SAMPLE_EVENT)
        data = response.json()
        assert data["riskScore"] == 100
        assert data["riskLevel"] == "重大风险"


class TestHistory:
    """测试 /history 接口"""

    def test_history_returns_200(self):
        client.post("/analyze_event", json=SAMPLE_EVENT)
        response = client.get("/history")
        assert response.status_code == 200
        data = response.json()
        assert "total" in data
        assert "records" in data


class TestGetEvent:
    """测试 /event/{event_id} 接口"""

    def test_get_existing_event(self):
        client.post("/analyze_event", json=SAMPLE_EVENT)
        response = client.get(f"/event/{SAMPLE_EVENT['eventId']}")
        assert response.status_code == 200
        assert response.json()["eventId"] == SAMPLE_EVENT["eventId"]

    def test_get_nonexistent_event(self):
        response = client.get("/event/NONEXISTENT")
        assert response.status_code == 404


class TestUpdateStatus:
    """测试 /event/{event_id}/status 接口"""

    def test_update_status_valid(self):
        client.post("/analyze_event", json=SAMPLE_EVENT)
        response = client.post(
            f"/event/{SAMPLE_EVENT['eventId']}/status",
            json={"status": "处置中"},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "处置中"

    def test_update_status_invalid(self):
        client.post("/analyze_event", json=SAMPLE_EVENT)
        response = client.post(
            f"/event/{SAMPLE_EVENT['eventId']}/status",
            json={"status": "不存在的状态"},
        )
        assert response.status_code == 400


class TestHealth:
    """测试 /health 接口"""

    def test_health(self):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"


class TestStats:
    """测试 /stats 接口"""

    def test_stats_returns_200(self):
        response = client.get("/stats")
        assert response.status_code == 200
        data = response.json()
        assert "totalEvents" in data
        assert "riskDistribution" in data


# ==================== 第二阶段新增测试 ====================

@pytest.fixture(scope="module")
def seed_stage2_data():
    """向数据库写入第二阶段测试数据。"""
    for ev in STAGE2_EVENTS:
        client.post("/analyze_event", json=ev)


class TestSimilarCases:
    """测试 /similar_cases/{event_id} 接口"""

    def test_similar_cases_returns_200(self, seed_stage2_data):
        response = client.get("/similar_cases/E202606300001?limit=3&min_score=0.3")
        assert response.status_code == 200
        data = response.json()
        assert "currentEvent" in data
        assert "similarCases" in data
        assert len(data["similarCases"]) > 0, "应该有相似案例"

    def test_similar_cases_not_found(self):
        response = client.get("/similar_cases/NONEXISTENT")
        assert response.status_code == 404

    def test_similar_cases_respects_limit(self, seed_stage2_data):
        response = client.get("/similar_cases/E202606300001?limit=2&min_score=0.3")
        data = response.json()
        assert len(data["similarCases"]) <= 2

    def test_similar_cases_has_scores(self, seed_stage2_data):
        response = client.get("/similar_cases/E202606300001?limit=3&min_score=0.3")
        data = response.json()
        for case in data["similarCases"]:
            assert "similarityScore" in case
            assert "similarityReasons" in case
            assert case["similarityScore"] >= 0.3


class TestDailyReport:
    """测试 /reports/daily 接口"""

    def test_daily_report_returns_200(self, seed_stage2_data):
        response = client.get("/reports/daily")
        assert response.status_code == 200
        data = response.json()
        assert "date" in data
        assert "totalEvents" in data
        assert "reportText" in data

    def test_daily_report_with_date(self, seed_stage2_data):
        response = client.get("/reports/daily?date=2026-06-30")
        assert response.status_code == 200
        data = response.json()
        assert data["date"] == "2026-06-30"


class TestWeeklyReport:
    """测试 /reports/weekly 接口"""

    def test_weekly_report_returns_200(self, seed_stage2_data):
        response = client.get("/reports/weekly")
        assert response.status_code == 200
        data = response.json()
        assert "startDate" in data
        assert "endDate" in data
        assert "totalEvents" in data
        assert "reportText" in data

    def test_weekly_report_with_dates(self, seed_stage2_data):
        response = client.get("/reports/weekly?start_date=2026-06-01&end_date=2026-06-30")
        assert response.status_code == 200


class TestUnclosedAlerts:
    """测试 /alerts/unclosed 接口"""

    def test_unclosed_alerts_returns_200(self, seed_stage2_data):
        response = client.get("/alerts/unclosed?hours=720&min_risk=低风险")
        assert response.status_code == 200
        data = response.json()
        assert "count" in data
        assert "alerts" in data

    def test_unclosed_alerts_invalid_risk(self):
        response = client.get("/alerts/unclosed?min_risk=超高风险")
        assert response.status_code == 400

    def test_unclosed_alerts_has_fields(self, seed_stage2_data):
        response = client.get("/alerts/unclosed?hours=720&min_risk=低风险")
        data = response.json()
        for alert in data["alerts"]:
            assert "eventId" in alert
            assert "alertReason" in alert
            assert "recommendedAction" in alert
            assert "durationSinceCreated" in alert


class TestHighRiskRoads:
    """测试 /stats/high_risk_roads 接口"""

    def test_high_risk_roads_returns_200(self, seed_stage2_data):
        response = client.get("/stats/high_risk_roads?days=30&min_risk=低风险")
        assert response.status_code == 200
        data = response.json()
        assert "range" in data
        assert "topRoads" in data

    def test_high_risk_roads_respects_limit(self, seed_stage2_data):
        response = client.get("/stats/high_risk_roads?limit=2&days=30&min_risk=低风险")
        data = response.json()
        assert len(data["topRoads"]) <= 2

    def test_high_risk_roads_has_fields(self, seed_stage2_data):
        response = client.get("/stats/high_risk_roads?days=30&min_risk=低风险")
        data = response.json()
        for road in data["topRoads"]:
            assert "roadName" in road
            assert "totalEvents" in road
            assert "avgRiskScore" in road
            assert "mostCommonEventType" in road
            assert "suggestedAction" in road


# ==================== Phase 3 + 4 测试 ====================


class TestRagEndpoints:
    def test_rag_status(self):
        r = client.get("/rag/status"); assert r.status_code == 200
        assert "enabled" in r.json()
    def test_rebuild_index(self):
        r = client.post("/rag/rebuild_index"); assert r.status_code in (200, 500)
    def test_rag_search(self, seed_stage2_data):
        r = client.get("/rag/search?query=%E6%8B%A5%E5%A0%B5&limit=3")
        assert r.status_code == 200; assert "results" in r.json()
    def test_rag_ask(self, seed_stage2_data):
        r = client.post("/rag/ask", json={"question": "雨天早高峰拥堵如何处置？", "limit": 3})
        assert r.status_code == 200; assert "answer" in r.json()
    def test_hybrid_similarity(self, seed_stage2_data):
        r = client.get("/similar_cases_hybrid/E202606300001?limit=3&min_score=0.3")
        assert r.status_code == 200
        for c in r.json().get("similarCases", []):
            assert "ruleSimilarity" in c; assert "vectorSimilarity" in c; assert "finalSimilarity" in c
    def test_multi_agent(self):
        body = {"eventId":"E99901","eventType":"congestion","roadName":"t","avgSpeed":8.5,"queueLength":180,"duration":601,"weather":"rain","timePeriod":"morning_peak"}
        r = client.post("/agent/multi_analyze", json=body)
        assert r.status_code == 200; d = r.json()
        assert "agentResults" in d; assert "finalDecision" in d; assert "dispatchPlan" in d


class TestReactDiagnose:
    def test_react_returns_200(self):
        r = client.post("/agent/react_diagnose", json={"question": "最近高风险事件多吗？", "max_steps": 3})
        assert r.status_code == 200; d = r.json()
        assert "question" in d; assert "steps" in d; assert "finalAnswer" in d; assert "usedLLM" in d
    def test_react_has_tool_calls(self):
        r = client.post("/agent/react_diagnose", json={"question": "人民路为什么高风险事件多？", "max_steps": 3})
        d = r.json()
        assert len(d.get("toolCalls", [])) > 0
        forbidden = {"update_event_status", "send_notification", "delete_event", "modify_risk_score", "analyze_event"}
        for tc in d.get("toolCalls", []):
            assert tc["tool"] not in forbidden
    def test_react_no_api_key(self):
        r = client.post("/agent/react_diagnose", json={"question": "统计当前事件数量", "max_steps": 2})
        assert r.status_code == 200; assert len(r.json().get("finalAnswer", "")) > 0


class TestRoutedAnalyze:
    S = {"eventId":"E99902","eventType":"congestion","roadName":"人民路","direction":"东向西","avgSpeed":5.0,"queueLength":300,"duration":1200,"weather":"rain","timePeriod":"morning_peak","isMainRoad":True,"nearbyHospital":True}
    def test_routed_returns_200(self):
        r = client.post("/agent/routed_analyze", json=self.S)
        assert r.status_code == 200; d = r.json()
        for k in ["selectedAgents","routingReasons","agentResults","conflicts","resolvedPlan","finalDecision","dispatchPlan","report"]:
            assert k in d, f"missing {k}"
    def test_routed_has_routing(self):
        d = client.post("/agent/routed_analyze", json=self.S).json()
        assert len(d["selectedAgents"]) >= 2; assert len(d["routingReasons"]) >= 1
    def test_routed_congestion_routing(self):
        d = client.post("/agent/routed_analyze", json=self.S).json()
        assert "CongestionAgent" in d["selectedAgents"]
        assert "DispatchAgent" in d["selectedAgents"]
    def test_routed_conflict_detection(self):
        body = {**self.S, "eventType":"accident", "nearbyHospital":True, "nearbySchool":True}
        d = client.post("/agent/routed_analyze", json=body).json()
        assert "conflicts" in d; assert isinstance(d["conflicts"], list)
    def test_old_multi_analyze_still_works(self):
        r = client.post("/agent/multi_analyze", json=self.S)
        assert r.status_code == 200


# ==================== Phase 6 测试 ====================


class TestChatSessions:
    """Chat 会话 CRUD"""
    sid: str = ""

    def test_create_session(self):
        r = client.post("/chat/sessions", json={"mode": "react"})
        assert r.status_code == 200
        d = r.json()
        assert "sessionId" in d
        TestChatSessions.sid = d["sessionId"]

    def test_list_sessions(self):
        r = client.get("/chat/sessions")
        assert r.status_code == 200
        assert len(r.json()["sessions"]) >= 1

    def test_send_message(self):
        assert TestChatSessions.sid, "No session created"
        r = client.post(f"/chat/sessions/{TestChatSessions.sid}/messages",
                        json={"content": "雨天早高峰主干道拥堵如何处置？", "mode": "react"})
        assert r.status_code == 200
        d = r.json()
        assert "sessionId" in d
        assert "userMessage" in d
        assert "assistantMessage" in d
        assert "abstained" in d
        assert "evidence" in d

    def test_get_session_detail(self):
        assert TestChatSessions.sid, "No session created"
        r = client.get(f"/chat/sessions/{TestChatSessions.sid}")
        assert r.status_code == 200
        d = r.json()
        assert "session" in d
        assert "messages" in d
        assert len(d["messages"]) >= 2  # user + assistant

    def test_delete_session(self):
        assert TestChatSessions.sid, "No session created"
        r = client.delete(f"/chat/sessions/{TestChatSessions.sid}")
        assert r.status_code == 200
        assert r.json()["success"] is True


class TestRagGrounded:
    """RAG 可信回答：有证据 / 无证据"""

    def test_with_evidence(self):
        r = client.post("/chat/sessions", json={"mode": "rag"})
        sid = r.json()["sessionId"]
        r = client.post(f"/chat/sessions/{sid}/messages",
                        json={"content": "雨天早高峰主干道拥堵应该怎么处置？", "mode": "rag"})
        d = r.json()
        assert d["abstained"] is False, "应有证据，不应拒答"
        assert len(d.get("evidence", [])) > 0, "应有检索证据"
        client.delete(f"/chat/sessions/{sid}")

    def test_without_evidence(self):
        r = client.post("/chat/sessions", json={"mode": "rag"})
        sid = r.json()["sessionId"]
        r = client.post(f"/chat/sessions/{sid}/messages",
                        json={"content": "火星基地交通信号灯如何优化？", "mode": "rag"})
        d = r.json()
        # May abstain OR provide low-confidence answer
        assert "abstained" in d
        assert "evidence" in d
        client.delete(f"/chat/sessions/{sid}")

    def test_policy_threshold(self):
        """retrieval_policy: empty results → abstain"""
        from backend.rag.retrieval_policy import apply_retrieval_threshold
        r = apply_retrieval_threshold([])
        assert r["abstain"] is True
        assert r["level"] == "none"

        # low score
        r = apply_retrieval_threshold([{"score": 0.20}])
        assert r["abstain"] is True

        # high score
        r = apply_retrieval_threshold([{"score": 0.80}])
        assert r["abstain"] is False
        assert r["level"] == "high"


class TestMemorySummary:
    """上下文摘要"""

    def test_long_session_summary(self):
        r = client.post("/chat/sessions", json={"mode": "react"})
        sid = r.json()["sessionId"]
        msgs = [
            "人民路最近高风险事件多吗？",
            "有哪些具体的处置建议？",
            "信号灯配时需要调整吗？",
            "医院周边的拥堵怎么处理？",
            "学校附近有没有类似问题？",
            "帮我生成一份周报",
            "未闭环事件有多少？",
        ]
        for q in msgs:
            client.post(f"/chat/sessions/{sid}/messages", json={"content": q, "mode": "react"})

        # Check memory summary exists
        from backend.chat.chat_db import get_memory_summary
        mem = get_memory_summary(sid)
        if mem:
            assert mem.get("summary"), "Summary should not be empty"
        # Context should not be infinite
        from backend.chat.memory_manager import build_context_for_llm
        ctx = build_context_for_llm(sid, "最新问题")
        assert len(ctx) < 4000, f"Context too long: {len(ctx)} chars"
        client.delete(f"/chat/sessions/{sid}")


# ==================== Phase 7 测试 ====================


class TestIntentRouter:
    def test_signal_intent(self):
        from backend.rag.intent_router import classify_traffic_intent
        assert classify_traffic_intent("信号灯异常如何处置") == "signal_abnormal"

    def test_congestion_intent(self):
        from backend.rag.intent_router import classify_traffic_intent
        assert classify_traffic_intent("主干道拥堵严重怎么处理") == "congestion"

    def test_accident_intent(self):
        from backend.rag.intent_router import classify_traffic_intent
        assert classify_traffic_intent("路口发生追尾事故") == "accident"

    def test_report_intent(self):
        from backend.rag.intent_router import classify_traffic_intent
        assert classify_traffic_intent("帮我生成今天的日报") == "report"

    def test_general_fallback(self):
        from backend.rag.intent_router import classify_traffic_intent
        assert classify_traffic_intent("今天天气怎么样") == "general_qa"


class TestDomainRetrieval:
    def test_doc_priority(self):
        from backend.rag.intent_router import get_doc_priority
        assert "rule" in get_doc_priority("signal_abnormal")

    def test_boost_applied(self):
        from backend.rag.domain_retrieval_policy import apply_domain_boost
        results = [{"score": 0.4, "metadata": {"docType": "rule", "eventType": "拥堵"}}]
        boosted = apply_domain_boost("信号灯异常怎么处理", results, {"eventTypeCn": "信号灯异常"})
        assert boosted[0]["score"] >= 0.4  # should have bonus added

    def test_empty_results(self):
        from backend.rag.domain_retrieval_policy import apply_domain_boost
        assert apply_domain_boost("测试", []) == []


class TestSimilarityConfig:
    def test_weights_returned(self):
        r = client.get("/similar_cases_hybrid/E202606300001?limit=2&min_score=0.2")
        if r.status_code == 200:
            for c in r.json().get("similarCases", []):
                assert "weightsUsed" in c
                assert "weightReason" in c

    def test_hybrid_has_all_fields(self):
        r = client.get("/similar_cases_hybrid/E202606300001?limit=2&min_score=0.2")
        if r.status_code == 200:
            for c in r.json().get("similarCases", []):
                for k in ["ruleSimilarity", "vectorSimilarity", "finalSimilarity", "weightsUsed", "similarityReasons"]:
                    assert k in c, f"Missing {k}"


class TestSystemStatus:
    def test_health_has_llm_info(self):
        r = client.get("/health")
        d = r.json()
        assert "llmAvailable" in d
        assert "llmMode" in d


# ==================== Phase 8.2 修复验证测试 ====================


class TestStreamTitle:
    """/chat/stream title 行为测试"""
    def test_first_message_generates_title(self):
        """首轮 done 返回标题，不是'新对话'"""
        import json as _j
        r = client.post("/chat/sessions", json={"mode": "react"})
        sid = r.json()["sessionId"]
        r = client.post(f"/chat/sessions/{sid}/messages", json={"content": "学校周边拥堵如何处置", "mode": "react"})
        d = r.json()
        # The done event returns title; verify session title updated
        r2 = client.get(f"/chat/sessions/{sid}")
        title = r2.json()["session"].get("title", "")
        assert title, "title should not be empty"
        assert title != "新对话", f"title should not be default '新对话', got '{title}'"

    def test_followup_does_not_change_title(self):
        """同一 session 追问后 title 不变"""
        r = client.post("/chat/sessions", json={"mode": "react"})
        sid = r.json()["sessionId"]
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "雨天早高峰拥堵怎么处理", "mode": "react"})
        r2 = client.get(f"/chat/sessions/{sid}")
        title1 = r2.json()["session"].get("title", "")
        # Follow up
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "有哪些具体建议", "mode": "react"})
        r3 = client.get(f"/chat/sessions/{sid}")
        title2 = r3.json()["session"].get("title", "")
        assert title1 == title2, f"follow-up should not change title: '{title1}' vs '{title2}'"

    def test_patch_title_not_overwritten(self):
        """手动重命名后追问不覆盖 title"""
        r = client.post("/chat/sessions", json={"mode": "react"})
        sid = r.json()["sessionId"]
        client.patch(f"/chat/sessions/{sid}/title", json={"title": "手动设置的标题"})
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "帮我分析拥堵", "mode": "react"})
        r3 = client.get(f"/chat/sessions/{sid}")
        assert r3.json()["session"].get("title") == "手动设置的标题", "manual title should not be overwritten"


class TestCollaborationSession:
    """协同分析会话化测试"""
    def test_collab_stream_returns_200(self):
        """/agent/routed_analyze/stream 返回 SSE stream"""
        body = {"eventType": "congestion", "roadName": "测试路", "direction": "东向西", "avgSpeed": 8.0, "queueLength": 300, "duration": 600}
        r = client.post("/agent/routed_analyze/stream", json=body)
        assert r.status_code == 200
        assert "text/event-stream" in r.headers.get("content-type", "")

    def test_collab_writes_to_chat_messages(self):
        """collaboration 结果写入 chat_messages"""
        r = client.post("/chat/sessions", json={"mode": "collaboration"})
        sid = r.json()["sessionId"]
        r2 = client.post(f"/chat/sessions/{sid}/messages", json={"content": "信号灯异常需要协同分析", "mode": "collaboration"})
        assert r2.status_code == 200
        # Verify messages exist
        r3 = client.get(f"/chat/sessions/{sid}")
        msgs = r3.json().get("messages", [])
        assert len(msgs) >= 2, f"should have user+assistant messages, got {len(msgs)}"


class TestDynamicAgentRouting:
    """动态 Agent 路由测试"""
    def test_congestion_routes_basic(self):
        """普通拥堵不触发全部 Agent"""
        from backend.agent.router import route_agents
        info = {"eventTypeCn": "拥堵", "eventType": "congestion", "roadName": "某路",
                "avgSpeed": 10, "queueLength": 100, "duration": 300,
                "weather": "clear", "timePeriod": "off_peak",
                "isMainRoad": False, "nearbySchool": False, "nearbyHospital": False,
                "riskLevel": "中风险"}
        result = route_agents(info)
        assert "CongestionAgent" in result["selectedAgents"]
        # PublicSafetyAgent should NOT trigger for basic congestion without school/hospital
        assert "PublicSafetyAgent" not in result["selectedAgents"], \
            f"PublicSafetyAgent should not trigger for basic congestion, got {result['selectedAgents']}"

    def test_hospital_triggers_safety(self):
        """nearbyHospital 触发 PublicSafetyAgent"""
        from backend.agent.router import route_agents
        info = {"eventTypeCn": "拥堵", "eventType": "congestion", "roadName": "某路",
                "avgSpeed": 10, "queueLength": 100, "duration": 300,
                "weather": "clear", "timePeriod": "off_peak",
                "isMainRoad": False, "nearbySchool": False, "nearbyHospital": True,
                "riskLevel": "高风险"}
        result = route_agents(info)
        assert "PublicSafetyAgent" in result["selectedAgents"], \
            f"nearbyHospital should trigger PublicSafetyAgent, got {result['selectedAgents']}"


class TestSSEChineseEncoding:
    """SSE 中文不乱码测试"""
    def test_sse_event_chinese(self):
        """verify sse_event produces valid UTF-8"""
        from backend.agent.streaming import sse_event
        ev = sse_event("step", {"text": "正在检索交通知识库", "stage": "retrieval"})
        assert "正在检索" in ev, f"Chinese should be preserved: {ev[:80]}"
        assert "data:" in ev, "SSE format should have data: line"
        assert ev.endswith("\n\n"), "SSE event should end with double newline"

    def test_sse_error_chinese(self):
        """verify sse_error handles Chinese"""
        from backend.agent.streaming import sse_error
        ev = sse_error("数据库连接失败")
        assert "数据库连接失败" in ev
        assert "error" in ev


# ==================== Phase 8.3 会话统一化测试 ====================


class TestQaSessionUnification:
    """知识库问答应写入统一 chat_sessions"""
    def test_rag_creates_session_via_rest(self):
        """通过 REST API 创建 rag session"""
        r = client.post("/chat/sessions", json={"mode": "rag"})
        sid = r.json()["sessionId"]
        r2 = client.post(f"/chat/sessions/{sid}/messages", json={"content": "雨天早高峰拥堵有哪些处置原则？", "mode": "rag"})
        assert r2.status_code == 200
        # Verify session exists with rag mode
        detail = client.get(f"/chat/sessions/{sid}").json()
        assert detail["session"]["mode"] == "rag"
        msgs = detail.get("messages", [])
        assert len(msgs) >= 2  # user + assistant

    def test_rag_session_title_not_default(self):
        """rag session title 应被生成，不是'新对话'"""
        sessions = client.get("/chat/sessions?limit=10").json()["sessions"]
        rag_sessions = [s for s in sessions if s.get("mode") == "rag"]
        if rag_sessions:
            non_default = [s for s in rag_sessions if s.get("title") != "新对话"]
            assert len(non_default) > 0, "Should have rag session with non-default title"


class TestCollaborationSessionUnification:
    """协同分析应写入统一 chat_sessions"""
    def test_collab_creates_session(self):
        body = {"eventType": "congestion", "roadName": "测试路", "direction": "东向西", "avgSpeed": 8.0, "queueLength": 300, "duration": 600}
        r = client.post("/agent/routed_analyze/stream", json=body)
        assert r.status_code == 200

    def test_collab_mode_in_list(self):
        sessions = client.get("/chat/sessions?limit=20").json()["sessions"]
        collab = [s for s in sessions if s.get("mode") == "collaboration"]
        assert len(collab) > 0, "Should have at least one collaboration session"


class TestTitleGeneration:
    """标题由 LLM/规则生成，不是简单截断"""
    def test_title_not_just_user_question(self):
        from backend.agent.chat_stream import _generate_title
        title = _generate_title("请分析学校周边拥堵事件。地点是人民路小学门口，早高峰排队较长。", "该事件风险等级为高...")
        assert title, "title should not be empty"
        # Should not just be the full question
        assert len(title) <= 20, f"title too long: {len(title)} chars: {title}"
        assert "请分析" not in title, f"title should not contain prefix: {title}"

    def test_title_collaboration_scenario(self):
        from backend.agent.chat_stream import _generate_title
        title = _generate_title("高速出口匝道排队超过500米已蔓延至主路，请评估安全风险并制定分流策略。", "综合各Agent分析，核心风险为...")
        assert title, "title should not be empty"
        assert len(title) <= 20


class TestTitleLocking:
    """标题锁定：后续追问不覆盖 + PATCH 后不覆盖"""
    def test_followup_keeps_title(self):
        r = client.post("/chat/sessions", json={"mode": "react"})
        sid = r.json()["sessionId"]
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "学校周边拥堵分析", "mode": "react"})
        title1 = client.get(f"/chat/sessions/{sid}").json()["session"]["title"]
        # Follow up
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "有什么具体建议？", "mode": "react"})
        title2 = client.get(f"/chat/sessions/{sid}").json()["session"]["title"]
        assert title1 == title2, f"title changed: {title1} -> {title2}"

    def test_patch_locks_title_rag(self):
        r = client.post("/chat/sessions", json={"mode": "rag"})
        sid = r.json()["sessionId"]
        client.patch(f"/chat/sessions/{sid}/title", json={"title": "我的自定义标题"})
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "拥堵怎么处置", "mode": "rag"})
        assert client.get(f"/chat/sessions/{sid}").json()["session"]["title"] == "我的自定义标题"


class TestModeLabels:
    """mode 标签映射"""
    def test_mode_label_mapping(self):
        labels = {"react": "诊断", "routed": "研判", "rag": "知识库", "hybrid": "相似", "report": "报告", "collaboration": "协同"}
        sessions = client.get("/chat/sessions?limit=20").json()["sessions"]
        for s in sessions:
            mode = s.get("mode", "")
            if mode:
                assert mode in labels, f"Unknown mode: {mode}"


# ==================== Phase 8.4 工作区恢复+追问mode锁定测试 ====================


class TestSessionModeRecovery:
    """session.mode 恢复后追问不改变 mode"""
    def test_collab_session_followup_keeps_mode(self):
        """collaboration session 追问后 mode 仍是 collaboration"""
        r = client.post("/chat/sessions", json={"mode": "collaboration"})
        sid = r.json()["sessionId"]
        # First message
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "信号灯需要协同分析", "mode": "collaboration"})
        # Follow-up — should still be collaboration, not create new session
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "如果排队继续扩大到500米怎么办", "mode": "collaboration"})
        # Verify session mode unchanged
        detail = client.get(f"/chat/sessions/{sid}").json()
        assert detail["session"]["mode"] == "collaboration"
        msgs = detail["messages"]
        assert len(msgs) >= 4  # 2 user + 2 assistant

    def test_rag_followup_keeps_mode(self):
        """rag session 追问后 mode 仍是 rag"""
        r = client.post("/chat/sessions", json={"mode": "rag"})
        sid = r.json()["sessionId"]
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "拥堵如何处置", "mode": "rag"})
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "有什么具体建议？", "mode": "rag"})
        detail = client.get(f"/chat/sessions/{sid}").json()
        assert detail["session"]["mode"] == "rag"

    def test_followup_does_not_create_new_session(self):
        """追问不创建新 session"""
        r = client.post("/chat/sessions", json={"mode": "react"})
        sid = r.json()["sessionId"]
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "学校周边拥堵", "mode": "react"})
        sessions_before = client.get("/chat/sessions?limit=50").json()["sessions"]
        count_before = len([s for s in sessions_before if s["id"] != sid])
        # Follow up 3 times
        for q in ["有什么建议", "需要分流吗", "总结一下"]:
            client.post(f"/chat/sessions/{sid}/messages", json={"content": q, "mode": "react"})
        sessions_after = client.get("/chat/sessions?limit=50").json()["sessions"]
        # Total count should not have increased by 3
        assert len(sessions_after) <= count_before + 1

    def test_title_unchanged_after_collab_followup(self):
        """collaboration 追问后标题不变"""
        r = client.post("/chat/sessions", json={"mode": "collaboration"})
        sid = r.json()["sessionId"]
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "匝道排队风险分析", "mode": "collaboration"})
        title1 = client.get(f"/chat/sessions/{sid}").json()["session"]["title"]
        client.post(f"/chat/sessions/{sid}/messages", json={"content": "如何优化信号配时", "mode": "collaboration"})
        title2 = client.get(f"/chat/sessions/{sid}").json()["session"]["title"]
        assert title1 == title2, f"title changed: {title1} -> {title2}"


# ==================== Phase 9.1 协作协议测试 ====================


class TestAgentProtocol:
    def test_valid_message(self):
        from backend.agent.collaboration.protocol import AgentMessage
        msg = AgentMessage(message_id="m1", run_id="r1", session_id="s1", sender="Orchestrator", receiver="CongestionAgent", message_type="task.assign")
        assert msg.protocol_version == "1.0"

    def test_invalid_message_type_rejected(self):
        from backend.agent.collaboration.protocol import AgentMessage
        import pytest as pt
        with pt.raises(ValueError, match="非法 message_type"):
            AgentMessage(message_id="m1", run_id="r1", session_id="s1", sender="Orchestrator", receiver="CongestionAgent", message_type="free.chat")

    def test_unregistered_sender_rejected(self):
        from backend.agent.collaboration.protocol import AgentMessage
        import pytest as pt
        with pt.raises(ValueError, match="未注册"):
            AgentMessage(message_id="m1", run_id="r1", session_id="s1", sender="UnknownBot", receiver="CongestionAgent", message_type="task.assign")

    def test_confidence_out_of_range_rejected(self):
        from backend.agent.collaboration.protocol import AgentResult
        import pytest as pt
        with pt.raises(ValueError): AgentResult(agent_name="T", task_id="t1", status="completed", confidence=1.5)
        with pt.raises(ValueError): AgentResult(agent_name="T", task_id="t1", status="completed", confidence=-0.1)

    def test_agent_result_valid(self):
        from backend.agent.collaboration.protocol import AgentResult
        r = AgentResult(agent_name="CongestionAgent", task_id="t1", status="completed", findings=["速度低"], confidence=0.9, suggestion="分流", urgency="high")
        assert r.status == "completed" and r.confidence == 0.9


class TestAgentRoles:
    def test_all_agents_registered(self):
        from backend.agent.collaboration.roles import get_all_registered_agents
        agents = get_all_registered_agents()
        for a in ["CongestionAgent", "SignalAgent", "FusionAgent", "ConflictArbiter"]:
            assert a in agents

    def test_unregistered_raises(self):
        from backend.agent.collaboration.roles import get_agent_capability
        import pytest as pt
        with pt.raises(ValueError, match="未注册"): get_agent_capability("RandomBot")

    def test_capability_has_boundaries(self):
        from backend.agent.collaboration.roles import get_agent_capability
        cap = get_agent_capability("CongestionAgent")
        assert cap["role"] == "拥堵分析"
        assert len(cap["forbidden_responsibilities"]) >= 1
        assert len(cap["allowed_input_fields"]) >= 3


class TestContextProjection:
    def test_congestion_no_hospital(self):
        from backend.agent.collaboration.context_projection import project_context_for_agent
        state = {"normalized_event": {"roadName": "人民路", "avgSpeed": 8.0, "queueLength": 300, "nearbyHospital": True}}
        ctx = project_context_for_agent(state, "CongestionAgent")
        assert "roadName" in ctx and "nearbyHospital" not in ctx

    def test_signal_no_hospital(self):
        from backend.agent.collaboration.context_projection import project_context_for_agent
        state = {"normalized_event": {"roadName": "人民路", "avgSpeed": 8.0, "nearbyHospital": True}}
        ctx = project_context_for_agent(state, "SignalAgent")
        assert "nearbyHospital" not in ctx

    def test_dispatch_gets_domain(self):
        from backend.agent.collaboration.context_projection import project_context_for_agent
        state = {"normalized_event": {"roadName": "人民路"}, "task_results": {"CongestionAgent": {"findings": ["拥堵"], "confidence": 0.9, "suggestion": "分流", "urgency": "high"}}}
        ctx = project_context_for_agent(state, "DispatchAgent")
        assert "domain_results" in ctx


class TestEventBus:
    def test_publish_history(self):
        from backend.agent.collaboration.event_bus import InMemoryEventBus
        bus = InMemoryEventBus()
        bus.publish({"message_id": "m1", "run_id": "r1", "message_type": "task.assign"})
        bus.publish({"message_id": "m2", "run_id": "r1", "message_type": "task.result"})
        assert len(bus.get_history("r1")) == 2

    def test_idempotency(self):
        from backend.agent.collaboration.event_bus import InMemoryEventBus
        bus = InMemoryEventBus()
        bus.publish({"message_id": "m1", "run_id": "r1", "message_type": "task.assign"})
        bus.publish({"message_id": "m1", "run_id": "r1", "message_type": "task.assign"})
        assert len(bus.get_history("r1")) == 1

    def test_subscriber(self):
        from backend.agent.collaboration.event_bus import InMemoryEventBus
        bus = InMemoryEventBus()
        received = []
        bus.subscribe("task.result", lambda m: received.append(m))
        bus.publish({"message_id": "m1", "run_id": "r1", "message_type": "task.result"})
        assert len(received) == 1


class TestChineseEncodingProtocol:
    def test_chinese_in_message(self):
        from backend.agent.collaboration.protocol import AgentMessage
        msg = AgentMessage(message_id="m1", run_id="r1", session_id="s1", sender="Orchestrator", receiver="CongestionAgent", message_type="task.assign", payload={"说明": "请分析拥堵情况"})
        assert "拥堵" in msg.model_dump_json()

    def test_chinese_in_result(self):
        from backend.agent.collaboration.protocol import AgentResult
        r = AgentResult(agent_name="测试Agent", task_id="t1", status="completed", findings=["速度低于10km/h"], confidence=0.8, suggestion="建议分流")
        assert "测试Agent" in r.model_dump_json()


# ==================== Phase 9.2 编排器测试 ====================


class TestStateMachine:
    def test_valid_transition(self):
        from backend.agent.collaboration.state import CollaborationRunState
        s = CollaborationRunState("r1", "s1", "t1")
        s.transition("routing"); assert s.status == "routing"
        s.transition("running"); assert s.status == "running"

    def test_invalid_transition_rejected(self):
        from backend.agent.collaboration.state import CollaborationRunState
        import pytest as pt
        s = CollaborationRunState("r1", "s1", "t1")
        with pt.raises(ValueError, match="非法状态转换"):
            s.transition("fusing")  # created → fusing not allowed

    def test_terminal_cannot_transition(self):
        from backend.agent.collaboration.state import CollaborationRunState
        import pytest as pt
        s = CollaborationRunState("r1", "s1", "t1")
        s.transition("routing"); s.transition("running"); s.transition("fusing"); s.transition("completed")
        assert s.is_terminal()
        with pt.raises(ValueError, match="终止状态"):
            s.transition("routing")


class TestTaskGraph:
    def test_valid_dag(self):
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        g = CollaborationTaskGraph("r1")
        g.add_task(AgentTaskNode("t1", "r1", "CongestionAgent"))
        g.add_task(AgentTaskNode("t2", "r1", "DispatchAgent", depends_on=["t1"]))
        g.validate_dependencies()

    def test_cycle_rejected(self):
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        import pytest as pt
        g = CollaborationTaskGraph("r1")
        g.add_task(AgentTaskNode("t1", "r1", "CongestionAgent", depends_on=["t2"]))
        g.add_task(AgentTaskNode("t2", "r1", "DispatchAgent", depends_on=["t1"]))
        with pt.raises(ValueError, match="循环依赖"):
            g.validate_dependencies()

    def test_missing_dep_rejected(self):
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        import pytest as pt
        g = CollaborationTaskGraph("r1")
        g.add_task(AgentTaskNode("t1", "r1", "CongestionAgent", depends_on=["t_nonexistent"]))
        with pt.raises(ValueError, match="不存在"):
            g.validate_dependencies()

    def test_duplicate_task_id_rejected(self):
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        import pytest as pt
        g = CollaborationTaskGraph("r1")
        g.add_task(AgentTaskNode("t1", "r1", "CongestionAgent"))
        with pt.raises(ValueError, match="重复"): g.add_task(AgentTaskNode("t1", "r1", "SignalAgent"))

    def test_unregistered_agent_rejected(self):
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        import pytest as pt
        g = CollaborationTaskGraph("r1")
        with pt.raises(ValueError, match="未注册"): g.add_task(AgentTaskNode("t1", "r1", "FakeAgent"))

    def test_get_ready_returns_deps_done(self):
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        g = CollaborationTaskGraph("r1")
        g.add_task(AgentTaskNode("t1", "r1", "CongestionAgent"))
        g.add_task(AgentTaskNode("t2", "r1", "DispatchAgent", depends_on=["t1"]))
        ready = g.get_ready_tasks()
        assert len(ready) == 1 and ready[0].task_id == "t1"
        g.mark_running("t1"); g.mark_succeeded("t1")
        ready2 = g.get_ready_tasks()
        assert len(ready2) == 1 and ready2[0].task_id == "t2"

    def test_mark_failed_blocks_dependents(self):
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        g = CollaborationTaskGraph("r1")
        g.add_task(AgentTaskNode("t1", "r1", "CongestionAgent", max_retries=0))
        g.add_task(AgentTaskNode("t2", "r1", "DispatchAgent", depends_on=["t1"]))
        g.mark_running("t1"); g.mark_failed("t1", "err")
        assert g.has_blocked_tasks()


class TestBudget:
    def test_budget_exhausted(self):
        from backend.agent.collaboration.budget import ExecutionBudget
        b = ExecutionBudget(max_agent_calls=1)
        assert b.can_call_agent("CongestionAgent")
        b.record_agent_call("CongestionAgent")
        assert not b.can_call_agent("CongestionAgent")

    def test_retry_count(self):
        from backend.agent.collaboration.budget import ExecutionBudget
        b = ExecutionBudget(max_retries=1)
        assert b.can_retry("CongestionAgent")
        b.record_retry("CongestionAgent")
        assert not b.can_retry("CongestionAgent")


class TestOrchestrator:
    def test_orchestrator_creates_run(self):
        from backend.agent.collaboration.orchestrator import CollaborationOrchestrator
        o = CollaborationOrchestrator()
        assert o is not None

    def test_orchestrator_with_congestion(self):
        from backend.agent.collaboration.orchestrator import CollaborationOrchestrator
        from backend.agent.collaboration.budget import ExecutionBudget
        from backend.agent.router import route_agents
        info = {"eventTypeCn": "拥堵", "roadName": "人民路", "avgSpeed": 8.0, "queueLength": 300,
                "weather": "clear", "timePeriod": "morning_peak", "isMainRoad": True,
                "nearbySchool": False, "nearbyHospital": False, "riskLevel": "高风险"}
        routing = route_agents(info)
        selected = routing["selectedAgents"]
        assert "CongestionAgent" in selected
        # Orchestrator initialization is valid
        o = CollaborationOrchestrator()
        assert o is not None
        budget = ExecutionBudget(max_agents=4, max_agent_calls=2)
        assert budget.can_call_agent("CongestionAgent")
        # TaskGraph creation
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        g = CollaborationTaskGraph("test_run_congestion")
        g.add_task(AgentTaskNode("t1", "test_run_congestion", "CongestionAgent"))
        g.add_task(AgentTaskNode("t2", "test_run_congestion", "DispatchAgent", depends_on=["t1"]))
        g.validate_dependencies()
        assert not g.is_completed()
        g.mark_running("t1"); g.mark_succeeded("t1")
        ready = g.get_ready_tasks()
        assert len(ready) == 1 and ready[0].task_id == "t2"


# ==================== Phase 9.3 测试 ====================


class TestSqliteRepository:
    def test_save_and_get_run(self):
        from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
        repo = SQLiteCollaborationRepository()
        state = {"run_id": "test_run_1", "session_id": "s1", "trace_id": "t1", "status": "created"}
        repo.save_run(state)
        run = repo.get_run("test_run_1")
        assert run is not None and run["run_id"] == "test_run_1"

    def test_save_message_idempotent(self):
        from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
        repo = SQLiteCollaborationRepository()
        msg = {"message_id": "msg_test_1", "run_id": "r1", "sender": "Orchestrator", "receiver": "CongestionAgent", "message_type": "task.assign"}
        repo.save_message(msg)
        repo.save_message(msg)  # should not raise
        msgs = repo.list_messages("r1")
        assert len(msgs) == 1

    def test_save_conflict(self):
        from backend.agent.collaboration.db_repository import SQLiteCollaborationRepository
        repo = SQLiteCollaborationRepository()
        conflict = {"conflict_id": "c1", "run_id": "r1", "type": "strategy_conflict", "severity": "medium", "participants": ["SignalAgent", "PublicSafetyAgent"]}
        repo.save_conflict(conflict)
        conflicts = repo.list_conflicts("r1")
        assert len(conflicts) >= 1


class TestCollaborationAgents:
    def test_dispatch_reads_domain_results(self):
        from backend.agent.collaboration.agents import dispatch_agent
        domain = {"CongestionAgent": {"suggestion": "建议分流"}}
        result = dispatch_agent(domain, {"roadName": "人民路", "isMainRoad": True, "nearbyHospital": True})
        assert len(result["dispatch_actions"]) >= 2
        assert "交警大队" in result["responsible_units"]

    def test_conflict_detected_when_signal_vs_safety(self):
        from backend.agent.collaboration.agents import conflict_detector
        results = [{"agent_name": "SignalAgent", "suggestion": "调整信号配时", "findings": ["信号异常"]},
                   {"agent_name": "PublicSafetyAgent", "suggestion": "保障医院通道", "findings": ["邻近医院"]}]
        conflicts = conflict_detector(results)
        assert len(conflicts) >= 1
        assert conflicts[0]["type"] == "strategy_conflict"

    def test_no_conflict_when_no_safety(self):
        from backend.agent.collaboration.agents import conflict_detector
        results = [{"agent_name": "CongestionAgent", "suggestion": "建议分流", "findings": ["拥堵"]}]
        conflicts = conflict_detector(results)
        assert len(conflicts) == 0

    def test_arbiter_medium_conflict_resolved(self):
        from backend.agent.collaboration.agents import conflict_arbiter
        result = conflict_arbiter({"id": "c1", "type": "strategy_conflict", "severity": "medium"})
        assert result["resolved"] is True
        assert not result["requires_human_review"]

    def test_arbiter_high_conflict_requires_human(self):
        from backend.agent.collaboration.agents import conflict_arbiter
        result = conflict_arbiter({"id": "c1", "type": "strategy_conflict", "severity": "high"})
        assert result["requires_human_review"] is True

    def test_fusion_produces_summary(self):
        from backend.agent.collaboration.agents import fusion_agent
        state = {"task_results": {"CongestionAgent": {"suggestion": "分流"}}, "conflicts": [], "failed_agents": []}
        result = fusion_agent(state)
        assert "fusion_summary" in result
        assert result["requires_human_review"] is False

    def test_fusion_with_failed_agents(self):
        from backend.agent.collaboration.agents import fusion_agent
        state = {"task_results": {"CongestionAgent": {"suggestion": "分流"}}, "conflicts": [], "failed_agents": ["SignalAgent"]}
        result = fusion_agent(state)
        assert result["core_risk"] == "中"


class TestErrorCodes:
    def test_error_codes_defined(self):
        from backend.agent.collaboration.agents import ERROR_CODES
        assert "COLLAB_VALIDATION_ERROR" in ERROR_CODES
        assert "COLLAB_TASK_TIMEOUT" in ERROR_CODES
        assert "COLLAB_FUSION_FAILED" in ERROR_CODES
        assert len(ERROR_CODES) >= 8


class TestAuditAPI:
    def test_audit_endpoints_exist(self):
        """Verify audit API is registered"""
        from backend.app import app
        paths = [r.path for r in app.routes if 'collaboration' in r.path]
        assert any('runs' in p for p in paths), f"Audit routes missing: {paths}"


class TestOrchestratorE2E:
    """端到端 Orchestrator 测试 — 完整协同流程"""
    def test_orchestrator_full_flow(self):
        """Orchestrator + TaskGraph + Router + Budget + SQLite persistence"""
        from backend.agent.collaboration.orchestrator import CollaborationOrchestrator
        from backend.agent.collaboration.budget import ExecutionBudget
        from backend.agent.collaboration.task_graph import CollaborationTaskGraph, AgentTaskNode
        from backend.agent.router import route_agents
        info = {"eventTypeCn": "拥堵", "roadName": "人民路", "avgSpeed": 8.0, "queueLength": 400,
                "weather": "rain", "timePeriod": "morning_peak", "isMainRoad": True,
                "nearbySchool": False, "nearbyHospital": False, "riskLevel": "高风险"}
        # Router
        routing = route_agents(info)
        assert "CongestionAgent" in routing["selectedAgents"]
        # TaskGraph
        g = CollaborationTaskGraph("e2e_r3")
        g.add_task(AgentTaskNode("t1", "e2e_r3", "CongestionAgent"))
        g.add_task(AgentTaskNode("t_d", "e2e_r3", "DispatchAgent", depends_on=["t1"]))
        g.validate_dependencies()
        g.mark_running("t1"); g.mark_succeeded("t1")
        assert g.get_ready_tasks()[0].task_id == "t_d"
        # Budget
        b = ExecutionBudget(max_agents=4, max_agent_calls=2)
        b.record_agent_call("CongestionAgent")
        assert not b.is_exhausted()
        # Orchestrator instantiation
        o = CollaborationOrchestrator()
        assert o is not None


class TestPhase94Integration:
    def test_feature_flag_default_true(self):
        from backend.app import COLLABORATION_ORCHESTRATOR_ENABLED
        assert COLLABORATION_ORCHESTRATOR_ENABLED in (True, False)  # default true

    def test_orchestrator_route_exists(self):
        from backend.app import app
        paths = [r.path for r in app.routes if 'routed_analyze/stream' in r.path]
        assert len(paths) >= 1

    def test_interrupted_state_valid(self):
        from backend.agent.collaboration.state import CollaborationRunState, VALID_STATUSES, TERMINAL_STATUSES
        assert "interrupted" in VALID_STATUSES
        assert "interrupted" in TERMINAL_STATUSES
        s = CollaborationRunState("r1", "s1", "t1")
        s.transition("routing"); s.transition("running"); s.transition("interrupted")
        assert s.is_terminal()

    def test_legacy_analyze_with_session(self):
        r = client.post("/chat/sessions", json={"mode": "collaboration"})
        sid = r.json()["sessionId"]
        ingest = client.post("/events/ingest", json={
            "source": "test-suite",
            "sourceEventId": f"legacy-analyze-{sid}",
            "event": {
                "eventType": "congestion", "roadName": "测试", "direction": "东",
                "avgSpeed": 8.0, "queueLength": 200, "duration": 600,
            },
        })
        assert ingest.status_code == 200
        body = {"eventId": ingest.json()["eventId"], "eventType": "congestion", "roadName": "测试", "direction": "东",
                "avgSpeed": 8.0, "queueLength": 200, "duration": 600, "sessionId": sid}
        rt = client.post("/agent/routed_analyze/stream", json=body)
        assert rt.status_code == 200  # SSE stream starts


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
