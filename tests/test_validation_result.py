# tests/test_validation_result.py
"""
Tests for src/common/validators/base.py — ValidationResult dataclass
"""

from src.common.validators.base import ValidationResult


class TestValidationResult:
    def test_passes_by_default(self):
        result = ValidationResult()
        assert result.passed is True

    def test_fail_sets_passed_false(self):
        result = ValidationResult()
        result.fail("something broke")
        assert result.passed is False

    def test_fail_accumulates_errors(self):
        result = ValidationResult()
        result.fail("error one")
        result.fail("error two")
        assert len(result.errors) == 2

    def test_warn_does_not_fail(self):
        result = ValidationResult()
        result.warn("heads up")
        assert result.passed is True
        assert len(result.warnings) == 1

    def test_note_does_not_fail(self):
        result = ValidationResult()
        result.note("fyi")
        assert result.passed is True
        assert len(result.info) == 1

    def test_summary_shows_all_levels(self):
        result = ValidationResult()
        result.fail("bad thing")
        result.warn("watch out")
        result.note("just so you know")
        summary = result.summary()
        assert "❌" in summary
        assert "⚠️" in summary
        assert "ℹ️" in summary

    def test_summary_all_passed_message(self):
        result = ValidationResult()
        assert "All checks passed" in result.summary()

    def test_multiple_fails_all_in_summary(self):
        result = ValidationResult()
        result.fail("first error")
        result.fail("second error")
        summary = result.summary()
        assert "first error" in summary
        assert "second error" in summary
