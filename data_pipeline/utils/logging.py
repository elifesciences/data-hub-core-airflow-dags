import logging
import re

import structlog
from structlog.typing import EventDict, WrappedLogger


def redact_secrets_from_log_value(value: str) -> str:
    return re.sub(r'pwd=[^&\s]+', 'pwd=***', value)


def redact_secrets_from_log_value_processor(
    logger: WrappedLogger,  # pylint: disable=unused-argument
    method_name: str,  # pylint: disable=unused-argument
    event_dict: EventDict
) -> EventDict:
    for key, value in list(event_dict.items()):
        if isinstance(value, str):
            event_dict[key] = redact_secrets_from_log_value(value)
    return event_dict


def configure_logging_with_redacted_secrets():
    root_logger = logging.getLogger()
    handler = root_logger.handlers[0]
    formatter = handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)

    existing_chain = list(formatter.foreign_pre_chain)
    existing_chain.append(redact_secrets_from_log_value_processor)

    new_formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=existing_chain,
        processors=formatter.processors,
        keep_exc_info=formatter.keep_exc_info,
        keep_stack_info=formatter.keep_stack_info,
    )

    handler.setFormatter(new_formatter)
