from data_pipeline.utils.progress import ProgressMonitor


class TestProgressMonitor:
    def test_should_return_progress_message_without_total(self):
        progress_monitor = ProgressMonitor(message_prefix='test:')
        progress_monitor.increment(10)
        assert str(progress_monitor) == 'test:10 (unknown total)'

    def test_should_return_progress_message_with_total(self):
        progress_monitor = ProgressMonitor(message_prefix='test:')
        progress_monitor.set_total(100)
        progress_monitor.increment(10)
        assert str(progress_monitor) == 'test:10 of 100 (10.0%)'

    def test_should_report_incomplete_if_total_is_known_and_current_is_less(self):
        progress_monitor = ProgressMonitor(total=100)
        progress_monitor.increment(10)
        assert progress_monitor.is_incomplete()

    def test_should_report_incomplete_false_if_current_equals_total(self):
        progress_monitor = ProgressMonitor(total=100)
        progress_monitor.increment(100)
        assert not progress_monitor.is_incomplete()

    def test_should_report_incomplete_false_if_total_is_unknown(self):
        progress_monitor = ProgressMonitor()
        progress_monitor.increment(100)
        assert not progress_monitor.is_incomplete()
