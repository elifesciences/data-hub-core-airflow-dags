import os


TEST_AIRFLOW_HOME = os.path.join(
            os.path.dirname(__file__), '..', 'data_pipelinex', 'dagsx'

            )
TEST_ENV_VARS = {
    'AIRFLOW_HOMEr': TEST_AIRFLOW_HOME
}



def pytest_configure(config):
    """Configure and init envvars for airflow."""
    config.old_env = {}
    for key, value in TEST_ENV_VARS.items():
        config.old_env[key] = os.getenv(key)
        os.environ[key] = value


def pytest_unconfigure(config):
    """Restore envvars to old values."""
    for key, value in config.old_env.items():
        if value is None:
            del os.environ[key]
        else:
            os.environ[key] = value