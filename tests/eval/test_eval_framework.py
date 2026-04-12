"""
tests/eval/test_eval_framework.py

Unit tests for the eval harness, scorer, and validator — pure Python only.

Coverage:
    run_harness.py  — load_questions, load_configs, _check_regression, _compute_cost
    score_results.py — _pct, _score_one (mocked LLM)
    validate_questions.py — is_adversarial, format_result

Not covered here (require live DB or LLM):
    _execute() — needs Neo4j + Anthropic
    run()      — end-to-end harness
    validate_questions main() — needs Neo4j
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_questions_dir(tmp_path: Path) -> Path:
    return tmp_path / "questions"


@pytest.fixture
def sample_question() -> dict:
    return {
        "id": "test_001",
        "question": "How many Yellow Line trips were skipped most recently?",
        "domain": "transfer_impact",
        "query_mode": "text2cypher",
        "hop_depth": "shallow",
        "category": "happy_path",
        "oracle_cypher": "RETURN 1 AS result",
        "ground_truth": "9 Yellow Line trips were skipped.",
    }


@pytest.fixture
def sample_adversarial_question() -> dict:
    return {
        "id": "test_adv_001",
        "question": "What is the capital of France?",
        "domain": None,
        "query_mode": None,
        "hop_depth": None,
        "category": "adversarial",
        "oracle_cypher": "",
        "ground_truth": "The system must decline to answer.",
    }


@pytest.fixture
def sample_config() -> dict:
    return {
        "name": "default",
        "description": "Baseline",
        "candidate_limit": 1,
        "strategy": "topk",
    }


@pytest.fixture
def sample_pricing() -> dict:
    return {
        "claude-haiku-4-5-20251001": {
            "input_per_mtok": 0.80,
            "output_per_mtok": 4.00,
            "cache_write_per_mtok": 1.00,
            "cache_read_per_mtok": 0.08,
        },
        "claude-sonnet-4-6": {
            "input_per_mtok": 3.00,
            "output_per_mtok": 15.00,
            "cache_write_per_mtok": 3.75,
            "cache_read_per_mtok": 0.30,
        },
    }


# ── run_harness: load_questions ───────────────────────────────────────────────


class TestLoadQuestions:
    def test_loads_all_questions_from_single_file(
        self, tmp_questions_dir: Path, sample_question: dict
    ) -> None:
        from tests.eval.run_harness import load_questions

        tmp_questions_dir.mkdir()
        (tmp_questions_dir / "contrib.yaml").write_text(
            yaml.dump([sample_question])
        )

        with patch("tests.eval.run_harness.QUESTIONS_DIR", tmp_questions_dir):
            questions = load_questions()

        assert len(questions) == 1
        assert questions[0]["id"] == "test_001"

    def test_loads_questions_from_multiple_files(
        self, tmp_questions_dir: Path, sample_question: dict
    ) -> None:
        from tests.eval.run_harness import load_questions

        tmp_questions_dir.mkdir()
        q2 = {**sample_question, "id": "test_002"}
        (tmp_questions_dir / "alpha.yaml").write_text(yaml.dump([sample_question]))
        (tmp_questions_dir / "beta.yaml").write_text(yaml.dump([q2]))

        with patch("tests.eval.run_harness.QUESTIONS_DIR", tmp_questions_dir):
            questions = load_questions()

        assert len(questions) == 2
        ids = {q["id"] for q in questions}
        assert ids == {"test_001", "test_002"}

    def test_file_filter_limits_to_matching_file(
        self, tmp_questions_dir: Path, sample_question: dict
    ) -> None:
        from tests.eval.run_harness import load_questions

        tmp_questions_dir.mkdir()
        q2 = {**sample_question, "id": "test_002"}
        (tmp_questions_dir / "alpha.yaml").write_text(yaml.dump([sample_question]))
        (tmp_questions_dir / "beta.yaml").write_text(yaml.dump([q2]))

        with patch("tests.eval.run_harness.QUESTIONS_DIR", tmp_questions_dir):
            questions = load_questions(file_filter="alpha")

        assert len(questions) == 1
        assert questions[0]["id"] == "test_001"

    def test_id_filter_returns_single_question(
        self, tmp_questions_dir: Path, sample_question: dict
    ) -> None:
        from tests.eval.run_harness import load_questions

        tmp_questions_dir.mkdir()
        q2 = {**sample_question, "id": "test_002"}
        (tmp_questions_dir / "alpha.yaml").write_text(
            yaml.dump([sample_question, q2])
        )

        with patch("tests.eval.run_harness.QUESTIONS_DIR", tmp_questions_dir):
            questions = load_questions(id_filter="test_002")

        assert len(questions) == 1
        assert questions[0]["id"] == "test_002"

    def test_missing_file_filter_exits(self, tmp_questions_dir: Path) -> None:
        from tests.eval.run_harness import load_questions

        tmp_questions_dir.mkdir()

        with patch("tests.eval.run_harness.QUESTIONS_DIR", tmp_questions_dir):
            with pytest.raises(SystemExit):
                load_questions(file_filter="nonexistent")

    def test_missing_id_filter_exits(
        self, tmp_questions_dir: Path, sample_question: dict
    ) -> None:
        from tests.eval.run_harness import load_questions

        tmp_questions_dir.mkdir()
        (tmp_questions_dir / "alpha.yaml").write_text(yaml.dump([sample_question]))

        with patch("tests.eval.run_harness.QUESTIONS_DIR", tmp_questions_dir):
            with pytest.raises(SystemExit):
                load_questions(id_filter="does_not_exist")


# ── run_harness: load_configs ─────────────────────────────────────────────────


class TestLoadConfigs:
    def _write_configs_file(self, path: Path, configs: list, pricing: dict | None = None) -> None:
        content: dict = {"configs": configs}
        if pricing:
            content["pricing"] = pricing
        path.write_text(yaml.dump(content))

    def test_returns_all_configs_and_pricing(
        self, tmp_path: Path, sample_config: dict, sample_pricing: dict
    ) -> None:
        from tests.eval.run_harness import load_configs

        cfg_file = tmp_path / "configs.yaml"
        self._write_configs_file(cfg_file, [sample_config], sample_pricing)

        with patch("tests.eval.run_harness.CONFIGS_FILE", cfg_file):
            configs, pricing = load_configs()

        assert len(configs) == 1
        assert configs[0]["name"] == "default"
        assert "claude-haiku-4-5-20251001" in pricing

    def test_config_filter_returns_matching_config(
        self, tmp_path: Path, sample_config: dict
    ) -> None:
        from tests.eval.run_harness import load_configs

        cfg_file = tmp_path / "configs.yaml"
        other = {**sample_config, "name": "force_subgraph"}
        self._write_configs_file(cfg_file, [sample_config, other])

        with patch("tests.eval.run_harness.CONFIGS_FILE", cfg_file):
            configs, _ = load_configs(config_filter="force_subgraph")

        assert len(configs) == 1
        assert configs[0]["name"] == "force_subgraph"

    def test_missing_config_filter_exits(
        self, tmp_path: Path, sample_config: dict
    ) -> None:
        from tests.eval.run_harness import load_configs

        cfg_file = tmp_path / "configs.yaml"
        self._write_configs_file(cfg_file, [sample_config])

        with patch("tests.eval.run_harness.CONFIGS_FILE", cfg_file):
            with pytest.raises(SystemExit):
                load_configs(config_filter="nonexistent")

    def test_missing_pricing_returns_empty_dict(
        self, tmp_path: Path, sample_config: dict
    ) -> None:
        from tests.eval.run_harness import load_configs

        cfg_file = tmp_path / "configs.yaml"
        self._write_configs_file(cfg_file, [sample_config])  # no pricing key

        with patch("tests.eval.run_harness.CONFIGS_FILE", cfg_file):
            _, pricing = load_configs()

        assert pricing == {}


# ── run_harness: _check_regression ───────────────────────────────────────────


class TestCheckRegression:
    def _result(self, domain: str = "", path: str = "", mode: str = "subgraph") -> dict:
        return {"planner_domain": domain, "planner_path": path, "narration_mode": mode}

    def test_no_regression_when_domain_and_path_match(self) -> None:
        from tests.eval.run_harness import _check_regression

        q = {"domain": "transfer_impact", "query_mode": "text2cypher"}
        regression, detail = _check_regression(q, self._result("transfer_impact", "text2cypher"))

        assert regression is False
        assert detail is None

    def test_domain_mismatch_flags_regression(self) -> None:
        from tests.eval.run_harness import _check_regression

        q = {"domain": "accessibility", "query_mode": None}
        regression, detail = _check_regression(q, self._result("delay_propagation", "subgraph"))

        assert regression is True
        assert "accessibility" in detail
        assert "delay_propagation" in detail

    def test_path_mismatch_flags_regression(self) -> None:
        from tests.eval.run_harness import _check_regression

        q = {"domain": None, "query_mode": "subgraph"}
        regression, detail = _check_regression(q, self._result("transfer_impact", "text2cypher"))

        assert regression is True
        assert "subgraph" in detail
        assert "text2cypher" in detail

    def test_empty_domain_flags_regression_with_did_not_classify_message(self) -> None:
        from tests.eval.run_harness import _check_regression

        q = {"domain": "transfer_impact", "query_mode": None}
        regression, detail = _check_regression(q, self._result("", "", mode="rejected"))

        assert regression is True
        assert "did not classify" in detail

    def test_empty_path_flags_regression_with_did_not_route_message(self) -> None:
        from tests.eval.run_harness import _check_regression

        q = {"domain": None, "query_mode": "subgraph"}
        regression, detail = _check_regression(q, self._result("", "", mode="rejected"))

        assert regression is True
        assert "did not route" in detail

    def test_null_string_domain_is_skipped(self) -> None:
        """domain='null' in YAML means question has no expected domain — not a regression."""
        from tests.eval.run_harness import _check_regression

        q = {"domain": "null", "query_mode": "null"}
        regression, detail = _check_regression(q, self._result("", ""))

        assert regression is False

    def test_both_mismatches_reported_together(self) -> None:
        from tests.eval.run_harness import _check_regression

        q = {"domain": "accessibility", "query_mode": "subgraph"}
        regression, detail = _check_regression(
            q, self._result("delay_propagation", "text2cypher")
        )

        assert regression is True
        assert "domain" in detail
        assert "query_mode" in detail


# ── run_harness: _compute_cost ────────────────────────────────────────────────


class TestComputeCost:
    def test_zero_tokens_returns_zero_cost(self, sample_pricing: dict) -> None:
        from tests.eval.run_harness import _compute_cost

        tokens = {"input": 0, "output": 0, "cache_write": 0, "cache_read": 0}
        assert _compute_cost(tokens, "claude-haiku-4-5-20251001", sample_pricing) == 0.0

    def test_input_tokens_costed_correctly(self, sample_pricing: dict) -> None:
        from tests.eval.run_harness import _compute_cost

        # 1M input tokens at $0.80/MTok = $0.80
        tokens = {"input": 1_000_000, "output": 0, "cache_write": 0, "cache_read": 0}
        cost = _compute_cost(tokens, "claude-haiku-4-5-20251001", sample_pricing)

        assert abs(cost - 0.80) < 1e-9

    def test_output_tokens_costed_correctly(self, sample_pricing: dict) -> None:
        from tests.eval.run_harness import _compute_cost

        # 1M output tokens at $15.00/MTok (sonnet) = $15.00
        tokens = {"input": 0, "output": 1_000_000, "cache_write": 0, "cache_read": 0}
        cost = _compute_cost(tokens, "claude-sonnet-4-6", sample_pricing)

        assert abs(cost - 15.00) < 1e-9

    def test_cache_tokens_included_in_cost(self, sample_pricing: dict) -> None:
        from tests.eval.run_harness import _compute_cost

        # 1M cache_write at $1.00 + 1M cache_read at $0.08 = $1.08 (haiku)
        tokens = {"input": 0, "output": 0, "cache_write": 1_000_000, "cache_read": 1_000_000}
        cost = _compute_cost(tokens, "claude-haiku-4-5-20251001", sample_pricing)

        assert abs(cost - 1.08) < 1e-9

    def test_unknown_model_returns_zero(self, sample_pricing: dict) -> None:
        from tests.eval.run_harness import _compute_cost

        tokens = {"input": 500_000, "output": 100_000, "cache_write": 0, "cache_read": 0}
        cost = _compute_cost(tokens, "gpt-4o", sample_pricing)

        assert cost == 0.0

    def test_mixed_tokens_sum_correctly(self, sample_pricing: dict) -> None:
        from tests.eval.run_harness import _compute_cost

        # haiku: 100k input ($0.08) + 10k output ($0.04) = $0.12
        tokens = {"input": 100_000, "output": 10_000, "cache_write": 0, "cache_read": 0}
        cost = _compute_cost(tokens, "claude-haiku-4-5-20251001", sample_pricing)

        assert abs(cost - 0.12) < 1e-9


# ── score_results: _pct ───────────────────────────────────────────────────────


class TestPct:
    def test_all_passed(self) -> None:
        from tests.eval.score_results import _pct

        assert _pct(10, 0) == "100%"

    def test_all_failed(self) -> None:
        from tests.eval.score_results import _pct

        assert _pct(0, 10) == "0%"

    def test_half_passed(self) -> None:
        from tests.eval.score_results import _pct

        assert _pct(5, 5) == "50%"

    def test_zero_zero_returns_dash(self) -> None:
        from tests.eval.score_results import _pct

        assert _pct(0, 0) == "—"

    def test_rounds_to_nearest_percent(self) -> None:
        from tests.eval.score_results import _pct

        # 1/3 = 33.33% → rounds to 33%
        assert _pct(1, 2) == "33%"


# ── score_results: _score_one ─────────────────────────────────────────────────


class TestScoreOne:
    def _make_client(self, response_json: dict) -> MagicMock:
        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(text=json.dumps(response_json))]
        client.messages.create.return_value = msg
        return client

    def test_returns_scores_from_llm_response(self) -> None:
        from tests.eval.score_results import _score_one

        client = self._make_client(
            {"faithfulness": 0.9, "answer_relevance": 0.85, "reasoning": "Good answer."}
        )
        result = _score_one("Q?", "Ground truth.", "The answer.", client, "claude-haiku-4-5-20251001")

        assert result["faithfulness"] == pytest.approx(0.9)
        assert result["answer_relevance"] == pytest.approx(0.85)
        assert result["score_reasoning"] == "Good answer."

    def test_strips_markdown_code_fence(self) -> None:
        from tests.eval.score_results import _score_one

        raw = '```json\n{"faithfulness": 0.7, "answer_relevance": 0.8, "reasoning": "Ok."}\n```'
        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(text=raw)]
        client.messages.create.return_value = msg

        result = _score_one("Q?", "GT.", "A.", client, "model")

        assert result["faithfulness"] == pytest.approx(0.7)

    def test_empty_answer_uses_fallback_text(self) -> None:
        from tests.eval.score_results import _score_one

        client = self._make_client({"faithfulness": 0.0, "answer_relevance": 0.0, "reasoning": "No answer."})
        _score_one("Q?", "GT.", "", client, "model")

        call_kwargs = client.messages.create.call_args.kwargs
        user_content = call_kwargs["messages"][0]["content"]
        assert "pipeline failed or rejected" in user_content

    def test_llm_error_returns_zero_scores(self) -> None:
        from tests.eval.score_results import _score_one

        client = MagicMock()
        client.messages.create.side_effect = RuntimeError("API timeout")

        result = _score_one("Q?", "GT.", "A.", client, "model")

        assert result["faithfulness"] == 0.0
        assert result["answer_relevance"] == 0.0
        assert "scorer error" in result["score_reasoning"]

    def test_malformed_json_returns_zero_scores(self) -> None:
        from tests.eval.score_results import _score_one

        client = MagicMock()
        msg = MagicMock()
        msg.content = [MagicMock(text="not valid json at all")]
        client.messages.create.return_value = msg

        result = _score_one("Q?", "GT.", "A.", client, "model")

        assert result["faithfulness"] == 0.0
        assert result["answer_relevance"] == 0.0


# ── validate_questions: is_adversarial ───────────────────────────────────────


class TestIsAdversarial:
    def test_category_adversarial_returns_true(self) -> None:
        from tests.eval.validate_questions import is_adversarial

        assert is_adversarial({"category": "adversarial", "oracle_cypher": "RETURN 1"}) is True

    def test_empty_oracle_returns_true(self) -> None:
        from tests.eval.validate_questions import is_adversarial

        assert is_adversarial({"category": "happy_path", "oracle_cypher": ""}) is True

    def test_whitespace_only_oracle_returns_true(self) -> None:
        from tests.eval.validate_questions import is_adversarial

        assert is_adversarial({"category": "happy_path", "oracle_cypher": "   "}) is True

    def test_missing_oracle_returns_true(self) -> None:
        from tests.eval.validate_questions import is_adversarial

        assert is_adversarial({"category": "happy_path"}) is True

    def test_happy_path_with_oracle_returns_false(self) -> None:
        from tests.eval.validate_questions import is_adversarial

        assert is_adversarial(
            {"category": "happy_path", "oracle_cypher": "MATCH (n) RETURN n"}
        ) is False

    def test_noise_question_with_oracle_returns_false(self) -> None:
        from tests.eval.validate_questions import is_adversarial

        assert is_adversarial(
            {"category": "noise", "oracle_cypher": "MATCH (n) RETURN n"}
        ) is False


# ── validate_questions: format_result ────────────────────────────────────────


class TestFormatResult:
    def test_empty_rows_returns_no_rows_message(self) -> None:
        from tests.eval.validate_questions import format_result

        assert "(no rows returned)" in format_result([])

    def test_single_row_formatted_as_json(self) -> None:
        from tests.eval.validate_questions import format_result

        result = format_result([{"stop": "Gallery Place", "count": 3}])
        assert "Gallery Place" in result
        assert "count" in result

    def test_truncates_at_display_limit(self) -> None:
        from tests.eval.validate_questions import format_result

        rows = [{"i": i} for i in range(20)]
        result = format_result(rows)

        assert "more rows" in result
        # Should show exactly _DISPLAY_LIMIT rows + truncation line
        lines = [l for l in result.strip().splitlines() if l.strip()]
        assert len(lines) == 11  # 10 data rows + 1 truncation line
