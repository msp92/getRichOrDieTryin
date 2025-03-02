from unittest.mock import patch, Mock
from scheduler.scheduler import (
    schedule_pipelines,
    PIPELINES,
    DAILY_SCHEDULE_DATES,
    run_job,
)


class TestScheduler:
    """Unit tests for the scheduler module."""

    @patch("scheduler.scheduler.schedule.every")
    def test_plans_pipelines(self, mock_schedule_every):
        """Test that the scheduler plans pipeline execution for all times and pipelines.

        Args:
            mock_run_job: Mocked run_job function to verify calls.
            mock_schedule_every: Mocked schedule.every to simulate scheduling.
        """
        mock_job = Mock()
        mock_schedule_every.return_value.day.at.return_value.do = mock_job

        schedule_pipelines(DAILY_SCHEDULE_DATES, PIPELINES)

        expected_calls = len(DAILY_SCHEDULE_DATES) * len(PIPELINES)
        assert (
            mock_job.call_count == expected_calls
        ), f"Expected {expected_calls} pipeline schedules, got {mock_job.call_count}"
        mock_job.assert_any_call(run_job, job_name="main")
