import os


def get_env(name, default=None):
    value = os.getenv(name)
    if value is None:
        return default
    value = value.strip()
    return value if value else default


def get_int_env(name, default=None):
    value = get_env(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def missing_env(names):
    return [name for name in names if get_env(name) is None]


def describe_missing(feature, names):
    missing = missing_env(names)
    if not missing:
        return None
    return f"{feature} is disabled because {', '.join(missing)} is not set in your .env file."