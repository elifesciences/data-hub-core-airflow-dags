from data_pipeline.scheduled_queries.cli import (
    main
)


class TestMain:
    def test_should_not_fail(self):
        main()
