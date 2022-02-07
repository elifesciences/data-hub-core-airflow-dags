from data_pipeline.civicrm_email_report.civicrm_email_report import (
    transform_email_report
)

MAIL_ID = 100

DELIVERED_VALUE = "135"
BOUNCES_VALUE = "1"
UNSUBSCRIBERS_VALUE = 0
UNIQUE_CLICKS_VALUE = "10"
OPENED_VALUE = "200"
CLICKTHROUGH_RATE_VALUE = "30.30%"
OPENED_RATE_VALUE = "180.5%"
DELIVERED_RATE_VALUE = "99%"

DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE = {
    "values": {
        str(MAIL_ID): {
            "Delivered": DELIVERED_VALUE,
            "Bounces": BOUNCES_VALUE,
            "Unsubscribers": UNSUBSCRIBERS_VALUE,
            "Unique Clicks": UNIQUE_CLICKS_VALUE,
            "Opened": OPENED_VALUE,
            "clickthrough_rate": CLICKTHROUGH_RATE_VALUE,
            "opened_rate": OPENED_RATE_VALUE,
            "delivered_rate": DELIVERED_RATE_VALUE
        }
    }
}


class TestTransformEmailReport():

    def test_should_extract_delivered_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["delivered"] == int(DELIVERED_VALUE)

    def test_should_extract_bounces_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["bounces"] == int(BOUNCES_VALUE)

    def test_should_extract_unsubscribers_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["unsubscribers"] == int(UNSUBSCRIBERS_VALUE)

    def test_should_extract_unique_clicks_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["unique_clicks"] == int(UNIQUE_CLICKS_VALUE)

    def test_should_extract_opened_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["opened"] == int(OPENED_VALUE)

    def test_should_extract_delivered_rate_as_float(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["delivered_rate"] == float(DELIVERED_RATE_VALUE.replace("%", ""))

    def test_should_extract_clickthrough_rate_as_float(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["clickthrough_rate"] == float(CLICKTHROUGH_RATE_VALUE.replace("%", ""))

    def test_should_extract_opened_rate_as_float(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["opened_rate"] == float(OPENED_RATE_VALUE.replace("%", ""))
