import os
from dataclasses import fields, is_dataclass
from datetime import time, date, datetime
from typing import Any, get_origin


class ImproperlyConfigured(Exception):
    def __init__(self, variable_name: str, *args, **kwargs):
        self.variable_name = variable_name
        self.message = f"Set the {variable_name} environment variable."
        super().__init__(self.message, *args, **kwargs)


def getenv(var_name: str, cast_to=str) -> str:
    try:
        value = os.environ[var_name]
        return cast_to(value)
    except KeyError:
        raise ImproperlyConfigured(var_name)
    except ValueError:
        raise ValueError(f"The value {value} can't be cast to {cast_to}.")


def parse_time(time_string: str) -> time:
    # time format in config: 20.00.00?tz=3 or 20.00.00
    if "?tz=" in time_string:
        time_str, timezone = time_string.split("?tz=")
    else:
        time_str, timezone = time_string, 0
    _time = time(*map(int, time_str.split(".")), microsecond=0)
    return _time.replace(hour=(_time.hour - int(timezone)) % 24)


def parse_date(date_string: str) -> date:
    # date format in config: 2025.12.30

    return datetime.strptime(date_string, "%Y.%m.%d").date()


def create_empty_config(cls):
    """Рекурсивно создаёт экземпляр dataclass с None/пустыми значениями."""
    if not is_dataclass(cls):
        return None

    init_args = {}
    for field in fields(cls):
        if is_dataclass(field.type):
            value = create_empty_config(field.type)
        elif field.type in (int, str, float, bool, time, list, dict):
            value = None
        elif get_origin(field.type) is list:
            value = []
        else:
            value = None
        init_args[field.name] = value
    return cls(**init_args)


def fill_from_env(obj: Any, prefix: str = ""):
    """
    Рекурсивно заполняет поля объекта из переменных окружения.
    Имя переменной: {prefix}__{field_name} в верхнем регистре.
    """
    if not is_dataclass(obj) or isinstance(obj, type):
        return

    for field in fields(obj):
        value = getattr(obj, field.name)
        field_name_upper = field.name.upper()
        env_key = f"{prefix}__{field_name_upper}" if prefix else field_name_upper

        if is_dataclass(value) and not isinstance(value, type):
            fill_from_env(value, env_key)
        else:
            setattr(obj, field.name, parse_value(getenv(env_key), field.type))


def parse_value(value: str, target_type: type) -> Any:
    """Преобразует строку в нужный тип."""
    if target_type is int:
        return int(value)
    elif target_type is float:
        return float(value)
    elif target_type is bool:
        return value.lower() in ("1", "true", "yes", "on")
    elif target_type is str:
        return value
    elif target_type == list[int]:
        return [int(x.strip()) for x in value.split(";") if x.strip()]
    elif target_type == list[str]:
        return value.split(";")
    elif target_type == time:
        return parse_time(value)
    elif target_type == date:
        return parse_date(value)
    else:
        return value
