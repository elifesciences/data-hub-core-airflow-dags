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

DISTINCT_UNIQUE_CLICKS_VALUE = "10"
DISTINCT_OPENED_VALUE = "50"

DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE = {
    "report_with_non_distinct_values": {
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
    },
    "report_with_distinct_values": {
        "values": {
            str(MAIL_ID): {
                "Unique Clicks": DISTINCT_UNIQUE_CLICKS_VALUE,
                "Opened": DISTINCT_OPENED_VALUE
            }
        }
    }
}


class TestTransformEmailReport():

    def test_shoul_mail_id_be_an_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["mail_id"] == MAIL_ID

    def test_should_extract_delivered_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["successful_deliveries"] == int(DELIVERED_VALUE)

    def test_should_extract_bounces_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["bounces"] == int(BOUNCES_VALUE)

    def test_should_extract_unsubscribe_requests_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["unsubscribe_requests"] == int(UNSUBSCRIBERS_VALUE)

    def test_should_extract_total_clicks_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["total_clicks"] == int(UNIQUE_CLICKS_VALUE)

    def test_should_extract_unique_clicks_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["unique_clicks"] == int(DISTINCT_UNIQUE_CLICKS_VALUE)

    def test_should_extract_total_opens_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["total_opens"] == int(OPENED_VALUE)

    def test_should_extract_unique_opens_as_integer(self):
        result = transform_email_report(
            DEFAULT_CIVICRM_EMAIL_REPORT_RESPONSE,
            MAIL_ID
        )
        assert result["unique_opens"] == int(DISTINCT_OPENED_VALUE)

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
