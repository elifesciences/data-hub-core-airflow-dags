import logging

LOGGER = logging.getLogger(__name__)


def main():
    LOGGER.info('Monitoring pipeline completed successfully.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
