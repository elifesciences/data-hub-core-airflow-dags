import logging


LOGGER = logging.getLogger(__name__)


def main():
    LOGGER.info('Starting Scheduled Queries pipeline')
    LOGGER.info('Done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
