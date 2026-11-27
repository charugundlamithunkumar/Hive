"""Configuration: reads .env (git-ignored) plus environment overrides."""
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

OFFICIAL_FILES = ["NASA_access_log_Jul95.gz", "NASA_access_log_Aug95.gz"]  # explicit order


@dataclass
class Config:
    mysql_host: str
    mysql_port: int
    mysql_user: str
    mysql_password: str
    mysql_db: str
    hive_host: str = "127.0.0.1"
    hive_port: int = 10000
    hive_stage_dir: str = tempfile.gettempdir()
    hive_server_stage_dir: str = ""


def _read_env_file(path: Path) -> dict:
    values = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


def load_config(env_path=None) -> Config:
    values = _read_env_file(Path(env_path) if env_path else ROOT / ".env")
    for key in list(values):
        values[key] = os.environ.get(key, values[key])
    get = lambda k, d=None: values.get(k, os.environ.get(k, d))
    return Config(
        mysql_host=get("MYSQL_HOST", "127.0.0.1"),
        mysql_port=int(get("MYSQL_PORT", "3306")),
        mysql_user=get("MYSQL_USER", "etl"),
        mysql_password=get("MYSQL_PASSWORD", ""),
        mysql_db=get("MYSQL_DB", "nasa_etl"),
        hive_host=get("HIVE_HOST", "127.0.0.1"),
        hive_port=int(get("HIVE_PORT", "10000")),
        hive_stage_dir=get("HIVE_STAGE_DIR", tempfile.gettempdir()),
        hive_server_stage_dir=get("HIVE_SERVER_STAGE_DIR", ""),
    )


def official_files(data_dir=None):
    base = Path(data_dir) if data_dir else ROOT
    return [base / name for name in OFFICIAL_FILES]
