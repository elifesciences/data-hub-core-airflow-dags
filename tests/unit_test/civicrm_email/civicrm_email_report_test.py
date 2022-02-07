from data_pipeline.civicrm_email_report.civicrm_email_report import (
    transform_email_report
)

MAIL_ID = 100

DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE = {
    "values": {
        str(MAIL_ID): {
            "Delivered": "135",
            "Bounces": "1",
            "Unsubscribers": 0,
            "Unique Clicks": "39",
            "Opened": "251",
            "clickthrough_rate": "30.30%",
            "opened_rate": "180.5%",
            "delivered_rate": "99%"
        }
    }
}


class TestTransformEmailReport():

    def test_should_extract_delivered_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["delivered"] == 135

    def test_should_extract_bounces_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["bounces"] == 1

    def test_should_extract_unsubscribers_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["unsubscribers"] == 0

    def test_should_extract_unique_clicks_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["unique_clicks"] == 39

    def test_should_extract_opened_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["opened"] == 251
    
    def test_should_extract_delivered_rate_as_float(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["delivered_rate"] == 99.0

    def test_should_extract_clickthrough_rate_as_float(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["clickthrough_rate"] == 30.30

    def test_should_extract_opened_rate_as_float(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["opened_rate"] == 180.5
