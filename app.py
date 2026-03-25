from flask import Flask, render_template, request, redirect, url_for, session, Response, flash, abort, jsonify
import requests
import os
import json
import logging
import math
import html
import base64
import smtplib
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from zoneinfo import ZoneInfo
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    import psycopg2
except Exception:
    psycopg2 = None

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "replace-this-with-your-secure-secret-key")
app.logger.setLevel(logging.INFO)
APP_ROOT = os.path.dirname(os.path.abspath(__file__))

START_ADDRESS = "서울특별시 종로구 율곡로2길 19"
RETURN_ADDRESS = "서울특별시 종로구 율곡로2길 19"

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "cpskqrhksfleks12#")
SETTINGS_FILE = "admin_settings.json"
GEOCODE_CACHE_FILE = "geocode_cache.json"
ROUTE_CACHE_FILE = "route_cache.json"
VEHICLE_LOG_OVERRIDES_FILE = "vehicle_log_overrides.json"
VEHICLE_LOG_BUNDLED_DB_FILE = "vehicle_log_source.db"
VEHICLE_LOG_REMOTE_CACHE_DB_FILE = "vehicle_log_remote_cache.db"
VEHICLE_LOG_DB_PATH_DEFAULT = r"C:\Users\MINSEOP\Desktop\개발\hyundai-api-test\app.db"
VEHICLE_LOG_REMOTE_SNAPSHOT_URL_DEFAULT = (
    os.getenv("VEHICLE_LOG_REMOTE_SNAPSHOT_URL")
    or "https://raw.githubusercontent.com/minseop-byeon/hyundai-log/main/vehicle_log_snapshot.db"
).strip()
try:
    VEHICLE_LOG_REMOTE_CACHE_TTL_SECONDS = max(
        60,
        int((os.getenv("VEHICLE_LOG_REMOTE_CACHE_TTL_SECONDS") or "300").strip() or "300"),
    )
except Exception:
    VEHICLE_LOG_REMOTE_CACHE_TTL_SECONDS = 300
VEHICLE_LOG_EDITABLE_FIELDS = (
    "passenger_name",
    "start_time",
    "end_time",
    "odometer_start",
    "odometer_end",
    "distance_km",
    "accident",
)

DEFAULT_TEAM_USERS = {"1조": []}
GUEST_TEAM_NAME = "게스트"
GUEST_USER_NAME = "게스트"
TMAP_DEFAULT_APP_KEY = os.getenv("TMAP_APP_KEY", "DBAKOdGMlm8X0TANyuGFI3GP7aMYWmb77v2JfnAA")
KAKAO_REST_API_KEY_DEFAULT = os.getenv("KAKAO_REST_API_KEY", "").strip()
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

DEFAULT_SETTINGS = {
    "mail": {
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 587,
        "smtp_user": "",
        "smtp_password": "",
        "mail_from": "",
        "default_recipient_email": "",
        "email_subject_template": "[경로결과] {team_no}({user_name}) - {trip_date}",
        "email_body_template": "경로 결과 PDF를 첨부합니다."
    },
    "api": {
        "client_id": "",
        "client_secret": "",
        "tmap_app_key": TMAP_DEFAULT_APP_KEY,
        "kakao_rest_api_key": KAKAO_REST_API_KEY_DEFAULT,
    },
    "user": {
        "start_name": "",
        "start_address": START_ADDRESS,
        "return_name": "",
        "return_address": RETURN_ADDRESS,
        "return_same_as_start": True,
        "team_users": DEFAULT_TEAM_USERS,
        "enable_guest_user": True
    },
    "restaurant": {
        "items": []
    },
    "parking": {
        "items": []
    },
    "vehicle_log": {
        "plate_numbers": {},
        "team_assignments": {},
        "main_drivers": {},
    },
    "admin": {
        "admin_password": ADMIN_PASSWORD
    }
}

DAY_START = 10 * 60
NO_LUNCH_IF_DONE_BY = 12 * 60
LUNCH_START_MIN = 11 * 60 + 30
LUNCH_START_MAX = 13 * 60 + 30
LUNCH_DURATION = 60
LUNCH_SKIP_IF_RETURN_BY = 13 * 60
LUNCH_SKIP_IF_DEPART_AFTER = 12 * 60 + 30
RETURN_LIMIT = 16 * 60 + 30

BEAM_WIDTH = 24
LOCAL_IMPROVE_ITER = 80
MAX_PARTIAL_CANDIDATES = 1200
ORDER_CANDIDATE_LIMIT = 12
APP_STATE_TABLE = "app_state"
ROUTE_CACHE_MEMORY = None
ROUTE_CACHE_DIRTY = False
USE_TRAFFIC_FOR_PLANNING = (os.getenv("USE_TRAFFIC_FOR_PLANNING", "0").strip() == "1")
PARKING_RESOLVED_CACHE_KEY = None
PARKING_RESOLVED_CACHE = []
HYUNDAI_COLLECT_INTERVAL_SECONDS = max(60, int((os.getenv("HYUNDAI_COLLECT_INTERVAL_SECONDS") or "300").strip() or "300"))
HYUNDAI_COLLECT_START_HOUR = max(0, min(23, int((os.getenv("HYUNDAI_COLLECT_START_HOUR") or "10").strip() or "10")))
HYUNDAI_COLLECT_END_HOUR = max(0, min(23, int((os.getenv("HYUNDAI_COLLECT_END_HOUR") or "18").strip() or "18")))
HYUNDAI_COLLECT_ALLOW_NON_WORKING_DAYS = (os.getenv("HYUNDAI_COLLECT_ALLOW_NON_WORKING_DAYS", "1").strip() == "1")
ENABLE_HYUNDAI_MILEAGE_COLLECTOR = (os.getenv("ENABLE_HYUNDAI_MILEAGE_COLLECTOR", "1").strip() == "1")
HYUNDAI_AUTH_BASE = (os.getenv("HYUNDAI_AUTH_BASE") or "").strip().rstrip("/")
HYUNDAI_DATA_BASE = (os.getenv("HYUNDAI_DATA_BASE") or "").strip().rstrip("/")
HYUNDAI_CLIENT_ID = (os.getenv("HYUNDAI_CLIENT_ID") or "").strip()
HYUNDAI_CLIENT_SECRET = (os.getenv("HYUNDAI_CLIENT_SECRET") or "").strip()
HYUNDAI_COLLECTOR_THREAD = None
HYUNDAI_COLLECTOR_STARTED = False


def load_json_file(path, default_value):
    if not os.path.exists(path):
        return default_value
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default_value


def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_database_url():
    return (os.getenv("DATABASE_URL") or DATABASE_URL or "").strip()


def has_database_storage():
    db_url = get_database_url()
    if not db_url:
        return False
    if "://" not in db_url:
        app.logger.warning("DATABASE_URL format is invalid; falling back to file storage.")
        return False
    if psycopg2 is None:
        app.logger.warning("psycopg2 is unavailable; falling back to file storage.")
        return False
    return True


def _remove_query_param(url, param_name):
    parts = urlsplit(url)
    query_pairs = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != param_name]
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query_pairs), parts.fragment))


def _database_url_candidates():
    if not has_database_storage():
        return []

    db_url = get_database_url()
    candidates = [db_url]

    # Render/Postgres providers sometimes inject options not supported by
    # psycopg2/libpq versions in runtime images. Try sanitized variants too.
    removable_params = (
        "channel_binding",
        "gssencmode",
        "target_session_attrs",
    )
    sanitized = db_url
    for param_name in removable_params:
        if f"{param_name}=" in sanitized:
            sanitized = _remove_query_param(sanitized, param_name)
    if sanitized != db_url:
        candidates.append(sanitized)

    if "sslmode=" not in sanitized:
        sep = "&" if "?" in sanitized else "?"
        candidates.append(f"{sanitized}{sep}sslmode=require")

    if "sslrootcert=" in sanitized:
        no_rootcert = _remove_query_param(sanitized, "sslrootcert")
        if no_rootcert != sanitized:
            candidates.append(no_rootcert)
            if "sslmode=" not in no_rootcert:
                sep = "&" if "?" in no_rootcert else "?"
                candidates.append(f"{no_rootcert}{sep}sslmode=require")

    # Preserve order while removing duplicates.
    deduped = []
    seen = set()
    for item in candidates:
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _connect_postgres():
    last_error = None
    for index, candidate in enumerate(_database_url_candidates()):
        try:
            conn = psycopg2.connect(candidate, connect_timeout=6)
            conn.autocommit = True
            if index > 0:
                app.logger.warning("Connected to Postgres after removing unsupported DATABASE_URL options.")
            return conn
        except Exception as exc:
            last_error = exc
    if last_error:
        app.logger.warning("Postgres connection attempts exhausted. last_error=%s", str(last_error))
        raise last_error
    raise RuntimeError("Database storage is not configured.")


def _ensure_postgres_state_table():
    with _connect_postgres() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {APP_STATE_TABLE} (
                    state_key TEXT PRIMARY KEY,
                    state_value JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )


def load_persistent_json(state_key, file_path, default_value):
    if has_database_storage():
        try:
            _ensure_postgres_state_table()
            with _connect_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT state_value FROM {APP_STATE_TABLE} WHERE state_key = %s",
                        (state_key,),
                    )
                    row = cur.fetchone()
            if row:
                value = row[0]
                if isinstance(value, str):
                    return json.loads(value)
                return value

            file_value = load_json_file(file_path, None)
            if file_value is not None:
                save_persistent_json(state_key, file_path, file_value)
                return file_value
            return default_value
        except Exception:
            app.logger.exception("Failed to load '%s' from Postgres. Falling back to file storage.", state_key)
            return load_json_file(file_path, default_value)

    return load_json_file(file_path, default_value)


def save_persistent_json(state_key, file_path, data):
    if has_database_storage():
        try:
            _ensure_postgres_state_table()
            with _connect_postgres() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        INSERT INTO {APP_STATE_TABLE} (state_key, state_value, updated_at)
                        VALUES (%s, %s::jsonb, NOW())
                        ON CONFLICT (state_key)
                        DO UPDATE SET
                            state_value = EXCLUDED.state_value,
                            updated_at = NOW()
                        """,
                        (state_key, json.dumps(data, ensure_ascii=False)),
                    )
            return
        except Exception:
            app.logger.exception("Failed to save '%s' to Postgres. Falling back to file storage.", state_key)

    save_json_file(file_path, data)


def initialize_storage():
    if not has_database_storage():
        app.logger.warning("Using file storage because DATABASE_URL Postgres storage is unavailable.")
        return
    try:
        _ensure_postgres_state_table()
        app.logger.info("Postgres storage is active.")
    except Exception:
        app.logger.exception("Postgres storage initialization failed. Falling back to file storage.")


def normalize_team_users(team_users):
    normalized = {}

    if isinstance(team_users, dict):
        for team_name, users in team_users.items():
            key = str(team_name).strip()
            if not key or key == GUEST_TEAM_NAME:
                continue
            if isinstance(users, list):
                clean_users = []
                for x in users:
                    v = str(x).strip()
                    if not v or v == GUEST_USER_NAME:
                        continue
                    clean_users.append(v)
                normalized[key] = clean_users

    if not normalized:
        normalized = {"1조": []}

    return normalized


def is_guest_enabled(settings):
    return bool((settings or {}).get("user", {}).get("enable_guest_user", True))


def get_effective_team_users(settings=None):
    settings = settings or load_settings()
    base = normalize_team_users((settings.get("user") or {}).get("team_users", {}))
    if is_guest_enabled(settings):
        effective = {GUEST_TEAM_NAME: [GUEST_USER_NAME]}
        effective.update(base)
        return effective
    return base


def is_valid_team_user_selection(team_users, team_no, user_name):
    team_no = (team_no or "").strip()
    user_name = (user_name or "").strip()
    if not team_no or not user_name:
        return False
    users = team_users.get(team_no)
    return isinstance(users, list) and user_name in users


def deep_copy_default_settings():
    return json.loads(json.dumps(DEFAULT_SETTINGS, ensure_ascii=False))


def migrate_legacy_settings(data):
    merged = deep_copy_default_settings()

    if not isinstance(data, dict):
        return merged

    has_new_structure = any(k in data for k in ["mail", "api", "user", "restaurant", "parking", "vehicle_log", "admin"])
    if has_new_structure:
        for section in ["mail", "api", "user", "restaurant", "parking", "vehicle_log", "admin"]:
            if isinstance(data.get(section), dict):
                merged[section].update(data[section])
    else:
        merged["api"]["client_id"] = str(data.get("client_id", "") or "").strip()
        merged["api"]["client_secret"] = str(data.get("client_secret", "") or "").strip()
        merged["mail"]["default_recipient_email"] = str(data.get("default_recipient_email", "") or "").strip()
        merged["mail"]["email_subject_template"] = str(
            data.get("email_subject_template", DEFAULT_SETTINGS["mail"]["email_subject_template"]) or ""
        ).strip()
        merged["mail"]["email_body_template"] = str(
            data.get("email_body_template", DEFAULT_SETTINGS["mail"]["email_body_template"]) or ""
        ).strip()
        merged["user"]["team_users"] = data.get("team_users", DEFAULT_TEAM_USERS)
        merged["user"]["start_name"] = ""
        merged["user"]["start_address"] = START_ADDRESS
        merged["user"]["return_name"] = ""
        merged["user"]["return_address"] = RETURN_ADDRESS
        merged["user"]["return_same_as_start"] = True

    merged["user"]["team_users"] = normalize_team_users(merged["user"].get("team_users", {}))
    merged["user"]["start_name"] = str(merged["user"].get("start_name", "") or "").strip()[:10]
    merged["user"]["return_name"] = str(merged["user"].get("return_name", "") or "").strip()[:10]
    if "enable_guest_user" not in merged["user"]:
        merged["user"]["enable_guest_user"] = True

    if not merged["user"].get("start_address"):
        merged["user"]["start_address"] = START_ADDRESS
    if not merged["user"].get("return_address"):
        merged["user"]["return_address"] = merged["user"].get("start_address", START_ADDRESS)
    if "return_same_as_start" not in merged["user"]:
        merged["user"]["return_same_as_start"] = True
    if not merged["api"].get("tmap_app_key"):
        merged["api"]["tmap_app_key"] = TMAP_DEFAULT_APP_KEY
    if "kakao_rest_api_key" not in merged["api"]:
        merged["api"]["kakao_rest_api_key"] = KAKAO_REST_API_KEY_DEFAULT
    if not merged["admin"].get("admin_password"):
        merged["admin"]["admin_password"] = ADMIN_PASSWORD
    restaurant_items = merged.get("restaurant", {}).get("items", [])
    normalized_restaurants = []
    if isinstance(restaurant_items, list):
        for item in restaurant_items:
            if not isinstance(item, dict):
                continue
            normalized_restaurants.append({
                "name": str(item.get("name", "") or "").strip(),
                "menu": str(item.get("menu", "") or "").strip(),
                "price": str(item.get("price", "") or "").strip(),
                "address": str(item.get("address", "") or "").strip(),
                "parking": str(item.get("parking", "") or "unknown").strip() if str(item.get("parking", "") or "").strip() in {"1", "0", "unknown"} else "unknown",
                "payment_card": bool(item.get("payment_card")),
                "payment_cash": bool(item.get("payment_cash")),
                "note": str(item.get("note", "") or "").strip(),
            })
    merged["restaurant"]["items"] = normalized_restaurants
    parking_items = merged.get("parking", {}).get("items", [])
    normalized_parking = []
    if isinstance(parking_items, list):
        for item in parking_items:
            if not isinstance(item, dict):
                continue
            normalized_parking.append({
                "name": str(item.get("name", "") or "").strip(),
                "address": str(item.get("address", "") or "").strip(),
            })
    merged["parking"]["items"] = normalized_parking
    vehicle_log = merged.get("vehicle_log", {})
    if not isinstance(vehicle_log, dict):
        vehicle_log = {}
    plate_numbers = vehicle_log.get("plate_numbers", {})
    if not isinstance(plate_numbers, dict):
        plate_numbers = {}
    team_assignments = vehicle_log.get("team_assignments", {})
    if not isinstance(team_assignments, dict):
        team_assignments = {}
    main_drivers = vehicle_log.get("main_drivers", {})
    if not isinstance(main_drivers, dict):
        main_drivers = {}
    merged["vehicle_log"] = {
        "plate_numbers": {
            str(car_id).strip(): str(plate_number).strip()
            for car_id, plate_number in plate_numbers.items()
            if str(car_id).strip()
        },
        "team_assignments": {
            str(car_id).strip(): str(team_name).strip()
            for car_id, team_name in team_assignments.items()
            if str(car_id).strip() and str(team_name).strip()
        },
        "main_drivers": {
            str(car_id).strip(): str(user_name).strip()
            for car_id, user_name in main_drivers.items()
            if str(car_id).strip() and str(user_name).strip()
        },
    }
    if not merged["mail"].get("smtp_host"):
        merged["mail"]["smtp_host"] = "smtp.gmail.com"
    if not merged["mail"].get("smtp_port"):
        merged["mail"]["smtp_port"] = 587

    try:
        merged["mail"]["smtp_port"] = int(merged["mail"].get("smtp_port", 587))
    except Exception:
        merged["mail"]["smtp_port"] = 587

    merged["user"]["return_same_as_start"] = bool(merged["user"].get("return_same_as_start"))
    return merged


def load_settings():
    data = load_persistent_json("settings", SETTINGS_FILE, None)
    if data is None:
        settings = deep_copy_default_settings()
        save_settings(settings)
        return settings

    return migrate_legacy_settings(data)


def save_settings(data):
    merged = migrate_legacy_settings(data)
    save_persistent_json("settings", SETTINGS_FILE, merged)


def get_start_address():
    settings = load_settings()
    return (settings.get("user", {}).get("start_address") or START_ADDRESS).strip() or START_ADDRESS


def get_return_address():
    settings = load_settings()
    user = settings.get("user", {})
    start_address = (user.get("start_address") or START_ADDRESS).strip() or START_ADDRESS
    if user.get("return_same_as_start"):
        return start_address
    return (user.get("return_address") or start_address).strip() or start_address


def get_start_name():
    settings = load_settings()
    value = (settings.get("user", {}).get("start_name") or "").strip()
    return value or "출발지"


def get_return_name():
    settings = load_settings()
    user = settings.get("user", {})
    start_name = (user.get("start_name") or "").strip()
    if user.get("return_same_as_start"):
        value = start_name
    else:
        value = (user.get("return_name") or "").strip()
    return value or "복귀지"


def get_mail_config():
    settings = load_settings()
    mail = settings.get("mail", {})
    return {
        "smtp_host": (mail.get("smtp_host") or "smtp.gmail.com").strip() or "smtp.gmail.com",
        "smtp_port": int(mail.get("smtp_port") or 587),
        "smtp_user": (mail.get("smtp_user") or "").strip(),
        "smtp_password": (mail.get("smtp_password") or "").strip(),
        "mail_from": (mail.get("mail_from") or "").strip(),
    }


def get_default_recipient_email():
    settings = load_settings()
    return (settings.get("mail", {}).get("default_recipient_email") or "").strip()


def get_vehicle_log_plate_numbers(settings=None):
    settings = settings or load_settings()
    plate_numbers = (settings.get("vehicle_log", {}) or {}).get("plate_numbers", {})
    return plate_numbers if isinstance(plate_numbers, dict) else {}


def get_vehicle_log_team_assignments(settings=None):
    settings = settings or load_settings()
    team_assignments = (settings.get("vehicle_log", {}) or {}).get("team_assignments", {})
    return team_assignments if isinstance(team_assignments, dict) else {}


def get_vehicle_log_main_drivers(settings=None):
    settings = settings or load_settings()
    main_drivers = (settings.get("vehicle_log", {}) or {}).get("main_drivers", {})
    return main_drivers if isinstance(main_drivers, dict) else {}


def update_vehicle_log_setting(settings, car_id, plate_number="", team_name="", main_driver=""):
    car_id = str(car_id or "").strip()
    if not car_id:
        raise ValueError("차량 ID가 올바르지 않습니다.")

    plate_number = str(plate_number or "").strip()
    team_name = str(team_name or "").strip()
    main_driver = str(main_driver or "").strip()

    team_users = normalize_team_users((settings.get("user") or {}).get("team_users", {}))
    plate_map = dict(get_vehicle_log_plate_numbers(settings))
    team_assignment_map = dict(get_vehicle_log_team_assignments(settings))
    main_driver_map = dict(get_vehicle_log_main_drivers(settings))

    if plate_number:
        plate_map[car_id] = plate_number
    else:
        plate_map.pop(car_id, None)

    if team_name:
        if team_name not in team_users:
            raise ValueError("배차는 등록된 조만 선택할 수 있습니다.")
        for assigned_car_id, assigned_team_name in team_assignment_map.items():
            if assigned_car_id == car_id:
                continue
            if str(assigned_team_name or "").strip() == team_name:
                raise ValueError("같은 조는 한 대의 차량에만 배차할 수 있습니다.")
        team_assignment_map[car_id] = team_name
    else:
        team_assignment_map.pop(car_id, None)

    effective_team_name = str(team_assignment_map.get(car_id) or "").strip()
    if main_driver:
        if not effective_team_name:
            raise ValueError("주 운전자를 선택하려면 먼저 배차를 지정해 주세요.")
        team_members = list(team_users.get(effective_team_name, []))
        if team_members and main_driver not in team_members:
            raise ValueError("주 운전자는 배차된 조의 인원 중에서만 선택할 수 있습니다.")
        main_driver_map[car_id] = main_driver
    else:
        main_driver_map.pop(car_id, None)

    settings["vehicle_log"]["plate_numbers"] = plate_map
    settings["vehicle_log"]["team_assignments"] = team_assignment_map
    settings["vehicle_log"]["main_drivers"] = main_driver_map

    return {
        "plate_number": plate_map.get(car_id, ""),
        "team_assignment": team_assignment_map.get(car_id, ""),
        "main_driver": main_driver_map.get(car_id, ""),
    }


def get_tmap_app_key():
    settings = load_settings()
    return (settings.get("api", {}).get("tmap_app_key") or TMAP_DEFAULT_APP_KEY).strip()


def get_kakao_rest_api_key():
    settings = load_settings()
    return (settings.get("api", {}).get("kakao_rest_api_key") or KAKAO_REST_API_KEY_DEFAULT).strip()


def get_admin_password():
    settings = load_settings()
    return (settings.get("admin", {}).get("admin_password") or ADMIN_PASSWORD).strip()


def get_api_headers():
    settings = load_settings()
    api = settings.get("api", {})
    return {
        "X-NCP-APIGW-API-KEY-ID": (api.get("client_id") or "").strip(),
        "X-NCP-APIGW-API-KEY": (api.get("client_secret") or "").strip()
    }


def resolve_kakao_address(query: str):
    query = (query or "").strip()
    if not query:
        return "", "주소가 비어 있습니다."

    api_key = get_kakao_rest_api_key()
    if not api_key:
        return "", "관리자 설정에서 Kakao REST API 키를 먼저 입력해 주세요."

    try:
        resp = requests.get(
            "https://dapi.kakao.com/v2/local/search/address.json",
            headers={"Authorization": f"KakaoAK {api_key}"},
            params={"query": query, "analyze_type": "similar"},
            timeout=10,
        )
    except requests.RequestException as e:
        return "", f"카카오 주소 검색 요청에 실패했습니다: {e}"

    if resp.status_code != 200:
        try:
            data = resp.json()
            msg = data.get("msg") or data.get("message") or resp.text
        except Exception:
            msg = resp.text
        return "", f"카카오 주소 검색 API 응답 오류입니다. {msg}".strip()

    try:
        data = resp.json()
    except Exception:
        return "", "카카오 주소 검색 API 응답을 해석하지 못했습니다."

    documents = data.get("documents") or []
    if not documents:
        return "", f"카카오 주소 검색 결과가 없습니다: {query}"

    first = documents[0] if isinstance(documents[0], dict) else {}
    road_address = first.get("road_address") or {}
    jibun_address = first.get("address") or {}
    normalized = (
        str(road_address.get("address_name") or "").strip()
        or str(jibun_address.get("address_name") or "").strip()
    )
    if not normalized:
        return "", f"카카오 주소 검색 결과를 해석하지 못했습니다: {query}"

    return normalized, ""


def get_vehicle_log_db_path():
    candidates = get_vehicle_log_db_candidates()
    selected_path, _source, _meta = select_vehicle_log_db(candidates)
    if selected_path:
        return selected_path
    return candidates[0] if candidates else ""


def get_vehicle_log_db_candidates():
    refresh_vehicle_log_remote_snapshot()
    candidates = []
    env_path = str(os.getenv("VEHICLE_LOG_DB_PATH") or "").strip()
    if env_path:
        candidates.append(env_path)
    remote_cache_path = get_vehicle_log_remote_cache_path()
    if remote_cache_path:
        candidates.append(remote_cache_path)
    candidates.append(os.path.join(APP_ROOT, VEHICLE_LOG_BUNDLED_DB_FILE))
    candidates.append(VEHICLE_LOG_DB_PATH_DEFAULT)

    unique_candidates = []
    seen = set()
    for item in candidates:
        path = str(item or "").strip()
        if not path:
            continue
        normalized_path = os.path.normpath(path)
        if normalized_path in seen:
            continue
        seen.add(normalized_path)
        unique_candidates.append(path)
    return unique_candidates


def _vehicle_log_source_label(path, env_path="", remote_cache_path="", bundled_path="", default_path=""):
    normalized = os.path.normpath(str(path or "").strip())
    if not normalized:
        return "missing"
    if env_path and normalized == env_path:
        return "env"
    if remote_cache_path and normalized == remote_cache_path:
        return "remote"
    if bundled_path and normalized == bundled_path:
        return "bundled"
    if default_path and normalized == default_path:
        return "default"
    return "other"


def inspect_vehicle_log_db(path):
    empty = {
        "path": path or "",
        "exists": bool(path and os.path.exists(path)),
        "valid": False,
        "latest_report_date": "",
        "latest_log_date": "",
        "report_count": 0,
        "log_count": 0,
    }
    if not empty["exists"]:
        return empty

    try:
        with sqlite3.connect(path) as conn:
            report_count = int(conn.execute("SELECT COUNT(*) FROM daily_reports").fetchone()[0] or 0)
            log_count = int(conn.execute("SELECT COUNT(*) FROM odometer_logs").fetchone()[0] or 0)
            latest_report_raw = conn.execute("SELECT MAX(drive_date) FROM daily_reports").fetchone()[0]
            latest_log_raw = conn.execute("SELECT MAX(log_date) FROM odometer_logs").fetchone()[0]

        return {
            "path": path,
            "exists": True,
            "valid": True,
            "latest_report_date": str(latest_report_raw or "").strip(),
            "latest_log_date": str(latest_log_raw or "").strip(),
            "report_count": report_count,
            "log_count": log_count,
        }
    except Exception:
        return empty


def select_vehicle_log_db(candidates):
    bundled_path = os.path.normpath(os.path.join(APP_ROOT, VEHICLE_LOG_BUNDLED_DB_FILE))
    remote_cache_path = os.path.normpath(get_vehicle_log_remote_cache_path()) if get_vehicle_log_remote_cache_path() else ""
    default_path = os.path.normpath(VEHICLE_LOG_DB_PATH_DEFAULT)
    env_path_raw = str(os.getenv("VEHICLE_LOG_DB_PATH") or "").strip()
    env_path = os.path.normpath(env_path_raw) if env_path_raw else ""

    existing = []
    for candidate in candidates:
        if not os.path.exists(candidate):
            continue
        source = _vehicle_log_source_label(
            candidate,
            env_path=env_path,
            remote_cache_path=remote_cache_path,
            bundled_path=bundled_path,
            default_path=default_path,
        )
        meta = inspect_vehicle_log_db(candidate)
        existing.append((candidate, source, meta))

    if not existing:
        return "", "missing", None

    dataful = []
    for candidate, source, meta in existing:
        if not meta or not meta.get("valid"):
            continue
        if int(meta.get("report_count") or 0) > 0 or int(meta.get("log_count") or 0) > 0:
            dataful.append((candidate, source, meta))

    if dataful:
        source_priority = {"env": 5, "remote": 4, "default": 3, "bundled": 2, "other": 1}

        def score(item):
            _path, src, meta = item
            latest = max(str(meta.get("latest_report_date") or ""), str(meta.get("latest_log_date") or ""))
            return (
                source_priority.get(src, 0),
                latest,
                int(meta.get("report_count") or 0) + int(meta.get("log_count") or 0),
            )

        best = max(dataful, key=score)
        return best

    return existing[0]


def get_vehicle_log_db_missing_message():
    searched = ", ".join(get_vehicle_log_db_candidates())
    return f"차량운행 DB 파일을 찾지 못했습니다. 검색 경로: {searched}"


def get_vehicle_log_db_status(today_value=None):
    today_value = today_value or date.today()
    candidates = get_vehicle_log_db_candidates()
    selected_path, selected_source, selected_meta = select_vehicle_log_db(candidates)

    status = {
        "path": selected_path or (candidates[0] if candidates else ""),
        "exists": bool(selected_path and os.path.exists(selected_path)),
        "source": selected_source,
        "latest_report_date": "",
        "latest_log_date": "",
        "report_count": 0,
        "log_count": 0,
        "warning_messages": [],
    }
    if not status["exists"]:
        status["warning_messages"].append(get_vehicle_log_db_missing_message())
        return status

    if not selected_meta or not selected_meta.get("valid"):
        selected_meta = inspect_vehicle_log_db(selected_path)

    if not selected_meta or not selected_meta.get("valid"):
        exc = "selected DB is invalid or unreadable"
        app.logger.warning("Failed to inspect vehicle log DB status: %s", exc)
        status["warning_messages"].append(f"李⑤웾?댄뻾 DB ?곹깭瑜?遺덈윭?ㅼ? 紐삵뻽?듬땲?? {exc}")
        return status

    def parse_date(value):
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None

    latest_report_date = parse_date(selected_meta.get("latest_report_date"))
    latest_log_date = parse_date(selected_meta.get("latest_log_date"))
    status["latest_report_date"] = latest_report_date.isoformat() if latest_report_date else ""
    status["latest_log_date"] = latest_log_date.isoformat() if latest_log_date else ""
    status["report_count"] = int(selected_meta.get("report_count") or 0)
    status["log_count"] = int(selected_meta.get("log_count") or 0)

    if status["source"] == "bundled":
        status["warning_messages"].append(
            "현재 운행이력은 앱에 포함된 번들 DB를 읽고 있습니다. 운영 DB 연결이 없으면 최신 이동이 반영되지 않을 수 있습니다."
        )

    if not latest_report_date and not latest_log_date:
        status["warning_messages"].append("운행이력 DB에 수집 데이터가 없습니다.")
        return status

    stale_reference = latest_report_date or latest_log_date
    if stale_reference:
        stale_days = (today_value - stale_reference).days
        if stale_days >= 1:
            status["warning_messages"].append(
                f"운행이력 DB 최신 일자가 {stale_reference.isoformat()} 로 {stale_days}일 지연되어 있습니다."
            )

    if latest_log_date and latest_report_date and latest_log_date > latest_report_date:
        status["warning_messages"].append(
            f"주행 로그 최신일({latest_log_date.isoformat()})이 운행이력 최신일({latest_report_date.isoformat()})보다 앞서 있습니다. 일일 리포트 생성 점검이 필요합니다."
        )

    return status


def get_vehicle_log_overrides():
    data = load_persistent_json("vehicle_log_overrides", VEHICLE_LOG_OVERRIDES_FILE, {})
    return data if isinstance(data, dict) else {}


def save_vehicle_log_overrides(data):
    save_persistent_json("vehicle_log_overrides", VEHICLE_LOG_OVERRIDES_FILE, data)


def normalize_vehicle_log_time(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    minute_value = str_to_minutes(raw)
    if minute_value is None:
        raise ValueError("시간은 HH:MM 형식으로 입력해 주세요.")
    return minutes_to_str(minute_value)


def normalize_vehicle_log_int(value, label):
    raw = str(value or "").strip().replace(",", "")
    if not raw:
        return ""
    try:
        return str(int(raw))
    except Exception as exc:
        raise ValueError(f"{label}은 숫자로 입력해 주세요.") from exc


def normalize_vehicle_log_accident(value):
    raw = str(value or "").strip()
    if raw not in {"", "없음", "있음"}:
        raise ValueError("사고유무는 없음 또는 있음으로 입력해 주세요.")
    return raw


def _vehicle_log_key(car_id, drive_date_text):
    return f"{car_id}|{drive_date_text}"


def calculate_vehicle_log_distance_km(odometer_start, odometer_end, fallback=None):
    try:
        if odometer_start is None or odometer_end is None:
            return fallback
        start_value = int(str(odometer_start).strip())
        end_value = int(str(odometer_end).strip())
        return end_value - start_value
    except Exception:
        return fallback


def _normalize_vehicle_log_payload(payload):
    normalized = {}
    normalized["passenger_name"] = str(payload.get("passenger_name") or "").strip() or None
    normalized["start_time"] = normalize_vehicle_log_time(payload.get("start_time"))
    normalized["end_time"] = normalize_vehicle_log_time(payload.get("end_time"))
    normalized["odometer_start"] = normalize_vehicle_log_int(payload.get("odometer_start"), "출발 km")
    normalized["odometer_end"] = normalize_vehicle_log_int(payload.get("odometer_end"), "도착 km")
    normalized["distance_km"] = normalize_vehicle_log_int(payload.get("distance_km"), "운행 거리")
    normalized["accident"] = normalize_vehicle_log_accident(payload.get("accident"))

    for field in ("start_time", "end_time", "odometer_start", "odometer_end", "distance_km"):
        if normalized[field] == "":
            normalized[field] = None
    if normalized["accident"] == "":
        normalized["accident"] = None
    normalized["distance_km"] = calculate_vehicle_log_distance_km(
        normalized.get("odometer_start"),
        normalized.get("odometer_end"),
        normalized.get("distance_km"),
    )
    return normalized


def get_vehicle_log_image_filename(vehicle):
    item = vehicle if isinstance(vehicle, dict) else {}
    vehicle_name = " ".join([
        str(item.get("label") or "").strip(),
        str(item.get("car_name") or "").strip(),
        str(item.get("car_nickname") or "").strip(),
        str(item.get("car_sellname") or "").strip(),
        str(item.get("car_type") or "").strip(),
        str(item.get("car_id") or "").strip(),
    ]).lower()
    if "ioniq 5" in vehicle_name or "ioniq5" in vehicle_name:
        return "ioniq5.png"
    if "ev6" in vehicle_name:
        return "EV6.png"
    if "casper" in vehicle_name:
        return "casper.png"
    return ""


def get_vehicle_log_default_main_driver(team_name, settings=None):
    settings = settings or load_settings()
    users = list(((settings.get("user") or {}).get("team_users", {}) or {}).get(team_name, []) or [])
    clean_users = sorted(str(user).strip() for user in users if str(user).strip())
    return clean_users[0] if clean_users else ""


def build_vehicle_log_passenger_summary(main_driver, team_members):
    members = [str(user).strip() for user in (team_members or []) if str(user).strip()]
    if not members:
        return ""
    driver_name = str(main_driver or "").strip() or members[0]
    others = max(0, len(members) - 1)
    if others:
        return f"{driver_name} 외 {others}명"
    return driver_name


def get_vehicle_log_remote_cache_path():
    if not VEHICLE_LOG_REMOTE_SNAPSHOT_URL_DEFAULT:
        return ""
    runtime_dir = str(os.getenv("VEHICLE_LOG_RUNTIME_DIR") or "").strip()
    if not runtime_dir:
        render_disk_root = str(os.getenv("RENDER_DISK_ROOT") or "").strip()
        if render_disk_root:
            runtime_dir = render_disk_root
        elif os.path.isdir("/var/data"):
            runtime_dir = "/var/data"
        else:
            runtime_dir = APP_ROOT
    try:
        os.makedirs(runtime_dir, exist_ok=True)
    except Exception:
        runtime_dir = APP_ROOT
    return os.path.join(runtime_dir, VEHICLE_LOG_REMOTE_CACHE_DB_FILE)


def refresh_vehicle_log_remote_snapshot(force=False):
    cache_path = get_vehicle_log_remote_cache_path()
    if not cache_path:
        return ""
    try:
        if (
            not force
            and os.path.exists(cache_path)
            and (time.time() - os.path.getmtime(cache_path)) < VEHICLE_LOG_REMOTE_CACHE_TTL_SECONDS
        ):
            return cache_path
    except Exception:
        pass

    try:
        response = requests.get(VEHICLE_LOG_REMOTE_SNAPSHOT_URL_DEFAULT, timeout=10)
        response.raise_for_status()
        payload = response.content
        if not payload:
            return cache_path if os.path.exists(cache_path) else ""

        existing_meta = inspect_vehicle_log_db(cache_path) if os.path.exists(cache_path) else None
        existing_rows = int((existing_meta or {}).get("report_count") or 0) + int((existing_meta or {}).get("log_count") or 0)

        temp_path = f"{cache_path}.tmp"
        with open(temp_path, "wb") as f:
            f.write(payload)

        incoming_meta = inspect_vehicle_log_db(temp_path)
        incoming_rows = int((incoming_meta or {}).get("report_count") or 0) + int((incoming_meta or {}).get("log_count") or 0)

        def _latest_meta_date(meta):
            report_date = str((meta or {}).get("latest_report_date") or "").strip()
            log_date = str((meta or {}).get("latest_log_date") or "").strip()
            latest = max(report_date, log_date)
            return latest

        existing_latest = _latest_meta_date(existing_meta)
        incoming_latest = _latest_meta_date(incoming_meta)

        # Never replace a dataful cache with an empty/older snapshot.
        if existing_rows > 0 and incoming_rows <= 0:
            try:
                os.remove(temp_path)
            except Exception:
                pass
            app.logger.warning(
                "Skipped remote snapshot replacement: incoming DB has no rows while existing cache has %s rows.",
                existing_rows,
            )
            return cache_path
        if existing_rows > incoming_rows:
            try:
                os.remove(temp_path)
            except Exception:
                pass
            app.logger.warning(
                "Skipped remote snapshot replacement: incoming DB rows(%s) < existing cache rows(%s).",
                incoming_rows,
                existing_rows,
            )
            return cache_path
        if existing_latest and incoming_latest and incoming_latest < existing_latest:
            try:
                os.remove(temp_path)
            except Exception:
                pass
            app.logger.warning(
                "Skipped remote snapshot replacement: incoming latest(%s) older than existing latest(%s).",
                incoming_latest,
                existing_latest,
            )
            return cache_path

        os.replace(temp_path, cache_path)
        return cache_path
    except Exception:
        app.logger.warning("Failed to refresh remote vehicle log snapshot.")
        return cache_path if os.path.exists(cache_path) else ""


def _ensure_vehicle_log_collection_db():
    cache_path = get_vehicle_log_remote_cache_path()
    if not cache_path:
        return ""
    if os.path.exists(cache_path):
        return cache_path

    bundled_path = os.path.join(APP_ROOT, VEHICLE_LOG_BUNDLED_DB_FILE)
    if os.path.exists(bundled_path):
        try:
            with open(bundled_path, "rb") as src, open(cache_path, "wb") as dst:
                dst.write(src.read())
            return cache_path
        except Exception:
            app.logger.warning("Failed to seed collector DB from bundled snapshot.")

    try:
        with sqlite3.connect(cache_path) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS token_store (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    access_token TEXT,
                    refresh_token TEXT,
                    expires_at TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS vehicle_store (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    car_id TEXT NOT NULL,
                    car_name TEXT,
                    car_nickname TEXT,
                    car_sellname TEXT,
                    car_type TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS odometer_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    car_id TEXT NOT NULL,
                    log_date TEXT NOT NULL,
                    log_time TEXT NOT NULL,
                    odometer_value INTEGER NOT NULL,
                    api_timestamp TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS daily_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    car_id TEXT NOT NULL,
                    drive_date TEXT NOT NULL,
                    start_time TEXT,
                    end_time TEXT,
                    odometer_start INTEGER,
                    odometer_end INTEGER,
                    distance_km INTEGER,
                    is_working_day INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS daily_manual_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    car_id TEXT NOT NULL,
                    drive_date TEXT NOT NULL,
                    passenger_name TEXT,
                    start_time TEXT,
                    end_time TEXT,
                    odometer_start INTEGER,
                    odometer_end INTEGER,
                    distance_km INTEGER,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
        return cache_path
    except Exception:
        app.logger.warning("Failed to initialize empty collector DB.")
        return ""


def _is_collect_window(local_now):
    if not HYUNDAI_COLLECT_ALLOW_NON_WORKING_DAYS and local_now.weekday() >= 5:
        return False
    if local_now.hour < HYUNDAI_COLLECT_START_HOUR or local_now.hour > HYUNDAI_COLLECT_END_HOUR:
        return False
    return True


def _parse_iso_datetime(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _hyundai_basic_auth_headers():
    if not HYUNDAI_CLIENT_ID or not HYUNDAI_CLIENT_SECRET:
        return None
    basic = base64.b64encode(f"{HYUNDAI_CLIENT_ID}:{HYUNDAI_CLIENT_SECRET}".encode("utf-8")).decode("utf-8")
    return {
        "Authorization": f"Basic {basic}",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def _refresh_access_token_in_db(conn, refresh_token):
    if not HYUNDAI_AUTH_BASE:
        return None
    headers = _hyundai_basic_auth_headers()
    if not headers:
        return None
    try:
        response = requests.post(
            f"{HYUNDAI_AUTH_BASE}/api/v1/user/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            headers=headers,
            timeout=20,
        )
        payload = response.json() if "application/json" in (response.headers.get("content-type") or "") else {}
        access_token = str(payload.get("access_token") or "").strip()
        if not access_token:
            return None
        new_refresh = str(payload.get("refresh_token") or "").strip() or refresh_token
        expires_in = int(payload.get("expires_in") or 3600)
        expires_at = (datetime.utcnow() + timedelta(seconds=max(expires_in - 60, 60))).isoformat(sep=" ")
        now_iso = datetime.utcnow().isoformat(sep=" ")
        conn.execute(
            "UPDATE token_store SET access_token=?, refresh_token=?, expires_at=?, updated_at=? WHERE id=1",
            (access_token, new_refresh, expires_at, now_iso),
        )
        conn.commit()
        return access_token
    except Exception:
        app.logger.warning("Hyundai token refresh failed.")
        return None


def _ensure_access_token(conn):
    row = conn.execute("SELECT id, access_token, refresh_token, expires_at FROM token_store ORDER BY id ASC LIMIT 1").fetchone()
    if not row:
        return None
    access_token = str(row[1] or "").strip()
    refresh_token = str(row[2] or "").strip()
    expires_at = _parse_iso_datetime(row[3])
    now_utc = datetime.utcnow()
    if access_token and expires_at and expires_at > now_utc:
        return access_token
    if not refresh_token:
        return None
    return _refresh_access_token_in_db(conn, refresh_token)


def _resolve_car_ids_for_collection(conn):
    car_ids = []
    preferred_many = str(os.getenv("HYUNDAI_CAR_IDS") or "").replace("\n", ",").split(",")
    for raw in preferred_many:
        v = str(raw or "").strip()
        if v and v not in car_ids:
            car_ids.append(v)
    preferred_one = str(os.getenv("HYUNDAI_CAR_ID") or "").strip()
    if preferred_one and preferred_one not in car_ids:
        car_ids.append(preferred_one)
    if car_ids:
        return car_ids

    rows = conn.execute("SELECT car_id FROM vehicle_store ORDER BY created_at ASC, id ASC").fetchall()
    return [str(item[0]).strip() for item in rows if str(item[0] or "").strip()]


def _derive_daily_report_fields_from_window(log_rows):
    if not log_rows:
        return None
    min_value = min(int(row["odometer_value"]) for row in log_rows)
    max_value = max(int(row["odometer_value"]) for row in log_rows)
    min_rows = [row for row in log_rows if int(row["odometer_value"]) == min_value]
    max_rows = [row for row in log_rows if int(row["odometer_value"]) == max_value]
    return {
        "start_time": str(min_rows[-1]["log_time"]) if min_rows else None,  # min value last maintained time
        "end_time": str(max_rows[0]["log_time"]) if max_rows else None,      # max value first started time
        "odometer_start": min_value,
        "odometer_end": max_value,
        "distance_km": max_value - min_value,
    }


def _upsert_daily_report_for_today(conn, car_id, target_date):
    rows = conn.execute(
        """
        SELECT log_time, odometer_value
        FROM odometer_logs
        WHERE car_id = ? AND log_date = ?
        ORDER BY log_time ASC
        """,
        (car_id, target_date),
    ).fetchall()

    window_rows = []
    for row in rows:
        raw = str(row["log_time"] or "").strip()
        hh = int(raw.split(":")[0]) if ":" in raw else -1
        if HYUNDAI_COLLECT_START_HOUR <= hh <= HYUNDAI_COLLECT_END_HOUR:
            window_rows.append(row)

    derived = _derive_daily_report_fields_from_window(window_rows)
    if not derived:
        return

    now_iso = datetime.utcnow().isoformat(sep=" ")
    existing = conn.execute(
        "SELECT id FROM daily_reports WHERE car_id = ? AND drive_date = ? LIMIT 1",
        (car_id, target_date),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE daily_reports
            SET start_time=?, end_time=?, odometer_start=?, odometer_end=?, distance_km=?, is_working_day=1, updated_at=?
            WHERE id=?
            """,
            (
                derived["start_time"],
                derived["end_time"],
                derived["odometer_start"],
                derived["odometer_end"],
                derived["distance_km"],
                now_iso,
                int(existing[0]),
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO daily_reports (
                car_id, drive_date, start_time, end_time, odometer_start, odometer_end, distance_km,
                is_working_day, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                car_id,
                target_date,
                derived["start_time"],
                derived["end_time"],
                derived["odometer_start"],
                derived["odometer_end"],
                derived["distance_km"],
                now_iso,
                now_iso,
            ),
        )
    conn.commit()


def _collect_odometer_once(db_path):
    if not HYUNDAI_DATA_BASE:
        return
    local_now = datetime.now()
    if not _is_collect_window(local_now):
        return

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        access_token = _ensure_access_token(conn)
        if not access_token:
            app.logger.warning("Hyundai collector: valid access token not available.")
            return

        car_ids = _resolve_car_ids_for_collection(conn)
        if not car_ids:
            app.logger.warning("Hyundai collector: no car_id configured.")
            return

        for car_id in car_ids:
            try:
                response = requests.get(
                    f"{HYUNDAI_DATA_BASE}/api/v1/car/status/{car_id}/odometer",
                    headers={"Authorization": f"Bearer {access_token}"},
                    timeout=20,
                )
                payload = response.json() if "application/json" in (response.headers.get("content-type") or "") else {}
                odometers = payload.get("odometers") or []
                if not odometers:
                    continue
                latest = odometers[0] if isinstance(odometers[0], dict) else {}
                value = latest.get("value")
                if value is None:
                    continue
                log_date = local_now.date().isoformat()
                log_time = local_now.strftime("%H:%M")
                api_timestamp = str(latest.get("timestamp") or "").strip()
                now_iso = datetime.utcnow().isoformat(sep=" ")

                existing = conn.execute(
                    """
                    SELECT id FROM odometer_logs
                    WHERE car_id=? AND log_date=? AND log_time=?
                    LIMIT 1
                    """,
                    (car_id, log_date, log_time),
                ).fetchone()
                if existing:
                    conn.execute(
                        "UPDATE odometer_logs SET odometer_value=?, api_timestamp=? WHERE id=?",
                        (int(value), api_timestamp, int(existing[0])),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO odometer_logs (car_id, log_date, log_time, odometer_value, api_timestamp, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (car_id, log_date, log_time, int(value), api_timestamp, now_iso),
                    )
                conn.commit()
                _upsert_daily_report_for_today(conn, car_id, log_date)
            except Exception:
                app.logger.warning("Hyundai collector: odometer collect failed for car_id=%s", car_id)


def _hyundai_collector_loop(db_path):
    while True:
        try:
            _collect_odometer_once(db_path)
        except Exception:
            app.logger.warning("Hyundai collector loop iteration failed.")
        time.sleep(HYUNDAI_COLLECT_INTERVAL_SECONDS)


def start_hyundai_mileage_collector():
    global HYUNDAI_COLLECTOR_THREAD, HYUNDAI_COLLECTOR_STARTED
    if HYUNDAI_COLLECTOR_STARTED:
        return
    HYUNDAI_COLLECTOR_STARTED = True

    if not ENABLE_HYUNDAI_MILEAGE_COLLECTOR:
        app.logger.info("Hyundai mileage collector is disabled by env.")
        return
    if not HYUNDAI_AUTH_BASE or not HYUNDAI_DATA_BASE or not HYUNDAI_CLIENT_ID or not HYUNDAI_CLIENT_SECRET:
        app.logger.warning("Hyundai mileage collector is not configured. Missing HYUNDAI_* envs.")
        return

    db_path = _ensure_vehicle_log_collection_db()
    if not db_path:
        app.logger.warning("Hyundai mileage collector could not initialize collection DB.")
        return

    HYUNDAI_COLLECTOR_THREAD = threading.Thread(
        target=_hyundai_collector_loop,
        args=(db_path,),
        name="hyundai-mileage-collector",
        daemon=True,
    )
    HYUNDAI_COLLECTOR_THREAD.start()
    app.logger.info(
        "Hyundai mileage collector started. interval=%ss window=%02d:00-%02d:59 db=%s",
        HYUNDAI_COLLECT_INTERVAL_SECONDS,
        HYUNDAI_COLLECT_START_HOUR,
        HYUNDAI_COLLECT_END_HOUR,
        db_path,
    )


def get_vehicle_log_vehicles():
    db_path = get_vehicle_log_db_path()
    settings = load_settings()
    plate_map = get_vehicle_log_plate_numbers(settings)
    team_assignment_map = get_vehicle_log_team_assignments(settings)
    main_driver_map = get_vehicle_log_main_drivers(settings)
    team_users_map = normalize_team_users((settings.get("user") or {}).get("team_users", {}))
    if not db_path:
        return [], get_vehicle_log_db_missing_message()
    if not os.path.exists(db_path):
        return [], get_vehicle_log_db_missing_message()

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            columns = {
                str(row["name"]).strip()
                for row in conn.execute("PRAGMA table_info(vehicle_store)").fetchall()
                if isinstance(row, sqlite3.Row)
            }
            plate_candidates = [
                "car_number",
                "car_no",
                "vehicle_number",
                "license_plate",
                "plate_number",
                "registration_number",
            ]
            plate_column = next((name for name in plate_candidates if name in columns), None)
            select_columns = ["car_id", "car_name", "car_nickname", "car_sellname", "car_type"]
            if plate_column:
                select_columns.append(plate_column)
            rows = conn.execute(
                f"""
                SELECT {", ".join(select_columns)}
                FROM vehicle_store
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()
    except Exception as exc:
        app.logger.exception("Failed to load vehicle list from vehicle log DB.")
        return [], f"차량 목록을 불러오지 못했습니다: {exc}"

    vehicles = []
    for row in rows:
        item = dict(row)
        item["label"] = (
            item.get("car_sellname")
            or item.get("car_nickname")
            or item.get("car_name")
            or item.get("car_type")
            or item.get("car_id")
            or ""
        )
        db_plate_number = str(item.get(plate_column) or "").strip() if 'plate_column' in locals() and plate_column else ""
        item["plate_number"] = str(plate_map.get(item.get("car_id")) or db_plate_number).strip()
        item["assigned_team"] = str(team_assignment_map.get(item.get("car_id")) or "").strip()
        item["team_members"] = list(team_users_map.get(item["assigned_team"], [])) if item["assigned_team"] else []
        saved_main_driver = str(main_driver_map.get(item.get("car_id")) or "").strip()
        if saved_main_driver and saved_main_driver in item["team_members"]:
            item["main_driver"] = saved_main_driver
        else:
            item["main_driver"] = get_vehicle_log_default_main_driver(item["assigned_team"], settings) if item["assigned_team"] else ""
        item["image_file"] = get_vehicle_log_image_filename(item)
        vehicles.append(item)

    return vehicles, ""


def build_vehicle_log_month_sections(rows, year_value):
    month_map = {month: [] for month in range(3, 11)}
    available_months = set()

    for row in rows:
        drive_date_text = str(row.get("drive_date") or "").strip()
        try:
            drive_date_value = date.fromisoformat(drive_date_text)
        except ValueError:
            continue
        if drive_date_value.year != year_value:
            continue
        if drive_date_value.month not in month_map:
            continue
        row["drive_date_obj"] = drive_date_value
        row["drive_date_label"] = f"{drive_date_value.month}월 {drive_date_value.day}일"
        month_map[drive_date_value.month].append(row)
        available_months.add(drive_date_value.month)

    month_sections = []
    for month in range(3, 11):
        month_rows = month_map.get(month, [])
        total_distance = 0
        for row in month_rows:
            value = row.get("distance_km_text") or row.get("distance_km") or ""
            if value in ("", None):
                continue
            try:
                total_distance += int(float(value))
            except Exception:
                continue
        month_sections.append({
            "month": month,
            "label": f"{month}월",
            "enabled": month in available_months,
            "rows": month_rows,
            "total_distance": total_distance,
        })
    return month_sections


def prepare_vehicle_log_display_rows(rows, vehicle):
    vehicle = vehicle if isinstance(vehicle, dict) else {}
    main_driver = str(vehicle.get("main_driver") or "").strip()
    team_members = list(vehicle.get("team_members") or [])
    default_passenger_name = build_vehicle_log_passenger_summary(main_driver, team_members)

    def format_km_text(value):
        raw = str(value or "").strip()
        if not raw:
            return "-"
        try:
            number_value = int(float(raw))
            return f"{number_value:,} km"
        except Exception:
            return f"{raw} km"

    prepared_rows = []
    for row in rows:
        item = dict(row)
        item["display_passenger_name"] = str(item.get("passenger_name") or "").strip() or default_passenger_name or "-"
        item["accident_display"] = str(item.get("accident_text") or "").strip() or "사고 없음"
        item["accident_is_yes"] = item["accident_display"] == "있음"
        item["odometer_start_display"] = format_km_text(item.get("odometer_start_text"))
        item["odometer_end_display"] = format_km_text(item.get("odometer_end_text"))
        distance_text = str(item.get("distance_km_text") or "").strip()
        if distance_text:
            try:
                item["distance_display"] = f"{int(float(distance_text)):,} km"
            except Exception:
                item["distance_display"] = f"{distance_text} km"
        else:
            item["distance_display"] = "0 km"
        item["original_passenger_name"] = str(item.get("passenger_name") or "").strip() or default_passenger_name
        item["original_start_time"] = str(item.get("start_time") or "").strip()
        item["original_end_time"] = str(item.get("end_time") or "").strip()
        item["original_odometer_start"] = str(item.get("odometer_start_text") or "").strip()
        item["original_odometer_end"] = str(item.get("odometer_end_text") or "").strip()
        item["original_distance_km"] = str(item.get("distance_km_text") or "").strip()
        item["original_accident"] = str(item.get("accident_text") or "").strip()
        prepared_rows.append(item)
    return prepared_rows


def get_vehicle_log_history(car_id, start_date_value, end_date_value):
    if not car_id:
        return [], "차량이 선택되지 않았습니다."

    db_path = get_vehicle_log_db_path()
    if not db_path or not os.path.exists(db_path):
        return [], get_vehicle_log_db_missing_message()

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            reports = conn.execute(
                """
                SELECT drive_date, start_time, end_time, odometer_start, odometer_end, distance_km
                FROM daily_reports
                WHERE car_id = ? AND drive_date >= ? AND drive_date <= ?
                ORDER BY drive_date DESC
                """,
                (car_id, start_date_value.isoformat(), end_date_value.isoformat()),
            ).fetchall()
            log_rows = conn.execute(
                """
                SELECT log_date, log_time, odometer_value
                FROM odometer_logs
                WHERE car_id = ? AND log_date >= ? AND log_date <= ?
                ORDER BY log_date ASC, log_time ASC
                """,
                (car_id, start_date_value.isoformat(), end_date_value.isoformat()),
            ).fetchall()
            manuals = conn.execute(
                """
                SELECT drive_date, passenger_name, start_time, end_time, odometer_start, odometer_end, distance_km
                FROM daily_manual_entries
                WHERE car_id = ? AND drive_date >= ? AND drive_date <= ?
                ORDER BY drive_date DESC
                """,
                (car_id, start_date_value.isoformat(), end_date_value.isoformat()),
            ).fetchall()
    except Exception as exc:
        app.logger.exception("Failed to load vehicle history from vehicle log DB.")
        return [], f"운행 이력을 불러오지 못했습니다: {exc}"

    report_map = {}
    for row in reports:
        item = dict(row)
        report_map[item["drive_date"]] = {
            "drive_date": item["drive_date"],
            "passenger_name": "",
            "start_time": item.get("start_time") or "",
            "end_time": item.get("end_time") or "",
            "odometer_start": item.get("odometer_start"),
            "odometer_end": item.get("odometer_end"),
            "distance_km": item.get("distance_km"),
            "accident": None,
            "source": "원본",
        }

    # Fallback: build day reports directly from odometer logs (10~18 window)
    # so same-day records are visible even before/without daily_reports upsert.
    logs_by_date = {}
    for row in log_rows:
        log_date_text = str(row["log_date"] or "").strip()
        log_time_text = str(row["log_time"] or "").strip()
        if not log_date_text or not log_time_text:
            continue
        try:
            hour_value = int(log_time_text.split(":", 1)[0])
        except Exception:
            continue
        if hour_value < HYUNDAI_COLLECT_START_HOUR or hour_value > HYUNDAI_COLLECT_END_HOUR:
            continue
        logs_by_date.setdefault(log_date_text, []).append(row)

    for log_date_text, day_rows in logs_by_date.items():
        derived = _derive_daily_report_fields_from_window(day_rows)
        if not derived:
            continue
        report_map[log_date_text] = {
            "drive_date": log_date_text,
            "passenger_name": "",
            "start_time": derived.get("start_time") or "",
            "end_time": derived.get("end_time") or "",
            "odometer_start": derived.get("odometer_start"),
            "odometer_end": derived.get("odometer_end"),
            "distance_km": derived.get("distance_km"),
            "accident": None,
            "source": "로그계산",
        }

    manual_map = {}
    for row in manuals:
        item = dict(row)
        manual_map[item["drive_date"]] = item

    overrides = get_vehicle_log_overrides()
    override_dates = set()
    for key in overrides.keys():
        if not isinstance(key, str) or not key.startswith(f"{car_id}|"):
            continue
        _, drive_date_text = key.split("|", 1)
        try:
            parsed = date.fromisoformat(drive_date_text)
        except ValueError:
            continue
        if start_date_value <= parsed <= end_date_value:
            override_dates.add(drive_date_text)

    all_dates = set(report_map.keys()) | set(manual_map.keys()) | override_dates
    rows = []
    for drive_date_text in sorted(all_dates, reverse=True):
        row = report_map.get(drive_date_text, {
            "drive_date": drive_date_text,
            "passenger_name": "",
            "start_time": "",
            "end_time": "",
            "odometer_start": None,
            "odometer_end": None,
            "distance_km": None,
            "accident": None,
            "source": "원본",
        })

        manual = manual_map.get(drive_date_text)
        if manual:
            row["passenger_name"] = manual.get("passenger_name") or row["passenger_name"]
            if manual.get("start_time") is not None:
                row["start_time"] = manual.get("start_time") or ""
            if manual.get("end_time") is not None:
                row["end_time"] = manual.get("end_time") or ""
            if manual.get("odometer_start") is not None:
                row["odometer_start"] = manual.get("odometer_start")
            if manual.get("odometer_end") is not None:
                row["odometer_end"] = manual.get("odometer_end")
            if manual.get("distance_km") is not None:
                row["distance_km"] = manual.get("distance_km")
            row["source"] = "수동보정"

        override_item = overrides.get(_vehicle_log_key(car_id, drive_date_text))
        if isinstance(override_item, dict):
            row["passenger_name"] = str(override_item.get("passenger_name") or "").strip()
            row["start_time"] = str(override_item.get("start_time") or "").strip()
            row["end_time"] = str(override_item.get("end_time") or "").strip()
            row["odometer_start"] = override_item.get("odometer_start")
            row["odometer_end"] = override_item.get("odometer_end")
            row["distance_km"] = override_item.get("distance_km")
            row["accident"] = override_item.get("accident")
            row["source"] = "현재앱 수정"

        row["distance_km"] = calculate_vehicle_log_distance_km(
            row.get("odometer_start"),
            row.get("odometer_end"),
            row.get("distance_km"),
        )
        row["odometer_start_text"] = "" if row["odometer_start"] is None else str(row["odometer_start"])
        row["odometer_end_text"] = "" if row["odometer_end"] is None else str(row["odometer_end"])
        row["distance_km_text"] = "" if row["distance_km"] is None else str(row["distance_km"])
        row["accident_text"] = "" if row.get("accident") is None else str(row.get("accident"))
        rows.append(row)

    return rows, ""


def parse_vehicle_log_date_arg(value, fallback):
    raw = str(value or "").strip()
    if not raw:
        return fallback
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return fallback


def parse_vehicle_log_form_rows(form):
    dates = form.getlist("drive_date")
    passenger_names = form.getlist("passenger_name")
    start_times = form.getlist("start_time")
    end_times = form.getlist("end_time")
    odometer_starts = form.getlist("odometer_start")
    odometer_ends = form.getlist("odometer_end")
    distance_values = form.getlist("distance_km")
    accident_values = form.getlist("accident")
    original_passenger_names = form.getlist("original_passenger_name")
    original_start_times = form.getlist("original_start_time")
    original_end_times = form.getlist("original_end_time")
    original_odometer_starts = form.getlist("original_odometer_start")
    original_odometer_ends = form.getlist("original_odometer_end")
    original_distance_values = form.getlist("original_distance_km")
    original_accident_values = form.getlist("original_accident")

    rows = []
    for idx, drive_date_text in enumerate(dates):
        current_payload = {
            "passenger_name": passenger_names[idx] if idx < len(passenger_names) else "",
            "start_time": start_times[idx] if idx < len(start_times) else "",
            "end_time": end_times[idx] if idx < len(end_times) else "",
            "odometer_start": odometer_starts[idx] if idx < len(odometer_starts) else "",
            "odometer_end": odometer_ends[idx] if idx < len(odometer_ends) else "",
            "distance_km": distance_values[idx] if idx < len(distance_values) else "",
            "accident": accident_values[idx] if idx < len(accident_values) else "",
        }
        original_payload = {
            "passenger_name": original_passenger_names[idx] if idx < len(original_passenger_names) else "",
            "start_time": original_start_times[idx] if idx < len(original_start_times) else "",
            "end_time": original_end_times[idx] if idx < len(original_end_times) else "",
            "odometer_start": original_odometer_starts[idx] if idx < len(original_odometer_starts) else "",
            "odometer_end": original_odometer_ends[idx] if idx < len(original_odometer_ends) else "",
            "distance_km": original_distance_values[idx] if idx < len(original_distance_values) else "",
            "accident": original_accident_values[idx] if idx < len(original_accident_values) else "",
        }
        rows.append({
            "drive_date": drive_date_text,
            "current": _normalize_vehicle_log_payload(current_payload),
            "original": _normalize_vehicle_log_payload(original_payload),
        })
    return rows


def save_vehicle_log_form_rows(car_id, rows):
    overrides = get_vehicle_log_overrides()
    changed_count = 0
    for row in rows:
        drive_date_text = str(row.get("drive_date") or "").strip()
        if not drive_date_text:
            continue
        key = _vehicle_log_key(car_id, drive_date_text)
        if (row.get("current") or {}) != (row.get("original") or {}):
            overrides[key] = row.get("current") or {}
            changed_count += 1
        else:
            overrides.pop(key, None)
    save_vehicle_log_overrides(overrides)
    return changed_count


def send_vehicle_history_email(recipient, vehicle_label, start_date_text, end_date_text, rows):
    recipient = str(recipient or "").strip()
    if not recipient:
        raise ValueError("이메일 수신자를 입력해 주세요.")

    mail_config = get_mail_config()
    smtp_host = mail_config.get("smtp_host") or ""
    smtp_port = int(mail_config.get("smtp_port") or 587)
    smtp_user = mail_config.get("smtp_user") or ""
    smtp_password = mail_config.get("smtp_password") or ""
    mail_from = mail_config.get("mail_from") or smtp_user
    if not smtp_host or not mail_from:
        raise ValueError("관리자 설정에서 SMTP 정보와 발신자 메일 주소를 먼저 설정해 주세요.")

    total_distance = 0
    text_lines = [
        f"차량: {vehicle_label}",
        f"조회기간: {start_date_text} ~ {end_date_text}",
        "",
    ]
    table_rows = []
    for row in rows:
        distance_text = row.get("distance_km_text") or ""
        if distance_text:
            try:
                total_distance += int(distance_text)
            except Exception:
                pass
        text_lines.append(
            " | ".join([
                row.get("drive_date") or "",
                row.get("passenger_name") or "",
                row.get("start_time") or "",
                row.get("end_time") or "",
                row.get("odometer_start_text") or "",
                row.get("odometer_end_text") or "",
                distance_text,
                row.get("source") or "",
            ])
        )
        table_rows.append(
            "<tr>"
            f"<td>{html.escape(row.get('drive_date') or '')}</td>"
            f"<td>{html.escape(row.get('passenger_name') or '')}</td>"
            f"<td>{html.escape(row.get('start_time') or '')}</td>"
            f"<td>{html.escape(row.get('end_time') or '')}</td>"
            f"<td>{html.escape(row.get('odometer_start_text') or '')}</td>"
            f"<td>{html.escape(row.get('odometer_end_text') or '')}</td>"
            f"<td>{html.escape(distance_text)}</td>"
            f"<td>{html.escape(row.get('source') or '')}</td>"
            "</tr>"
        )

    message = EmailMessage()
    message["Subject"] = f"[차량운행] {vehicle_label} / {start_date_text} ~ {end_date_text}"
    message["From"] = mail_from
    message["To"] = recipient
    message.set_content("\n".join(text_lines))
    message.add_alternative(
        f"""
        <html>
          <body style="font-family: Arial, sans-serif; color: #111827;">
            <h2>차량 운행 이력</h2>
            <p>차량: <strong>{html.escape(vehicle_label)}</strong></p>
            <p>조회기간: <strong>{html.escape(start_date_text)} ~ {html.escape(end_date_text)}</strong></p>
            <p>총 {len(rows)}건 / 합계 {total_distance}km</p>
            <table style="border-collapse: collapse; width: 100%; font-size: 13px;">
              <thead>
                <tr>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">일자</th>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">탑승자</th>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">출발</th>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">도착</th>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">출발km</th>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">도착km</th>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">거리</th>
                  <th style="border: 1px solid #d1d5db; padding: 8px; background: #f3f4f6;">출처</th>
                </tr>
              </thead>
              <tbody>{''.join(table_rows)}</tbody>
            </table>
          </body>
        </html>
        """,
        subtype="html",
    )

    if smtp_port == 465:
        server = smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=20)
    else:
        server = smtplib.SMTP(smtp_host, smtp_port, timeout=20)

    with server as smtp:
        smtp.ehlo()
        if smtp_port != 465:
            try:
                smtp.starttls()
                smtp.ehlo()
            except Exception:
                pass
        if smtp_user and smtp_password:
            smtp.login(smtp_user, smtp_password)
        smtp.send_message(message)


def is_mobile_request():
    ua = (request.headers.get("User-Agent") or "").lower()
    keywords = ["iphone", "android", "ipad", "mobile", "windows phone"]
    return any(k in ua for k in keywords)


def minutes_to_str(m: int) -> str:
    m = int(m)
    return f"{m // 60:02d}:{m % 60:02d}"


def str_to_minutes(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    if not s:
        return None
    if ':' not in s:
        try:
            return int(float(s))
        except Exception:
            return None
    parts = s.split(':')
    if len(parts) < 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except Exception:
        return None
    return hh * 60 + mm


def parse_appointment_minute(hour_str: str, minute_str: str):
    hour_str = (hour_str or "").strip()
    minute_str = (minute_str or "").strip()
    if hour_str == "" or minute_str == "":
        return None
    try:
        hh = int(hour_str)
        mm = int(minute_str)
    except Exception:
        return None
    return hh * 60 + mm


def shorten_sido_name(address: str):
    if address is None:
        return address
    s = str(address).strip()
    if not s:
        return s
    replacements = [
        ("제주특별자치도", "제주"),
        ("강원특별자치도", "강원"),
        ("전북특별자치도", "전북"),
        ("서울특별시", "서울"),
        ("부산광역시", "부산"),
        ("대구광역시", "대구"),
        ("인천광역시", "인천"),
        ("광주광역시", "광주"),
        ("대전광역시", "대전"),
        ("울산광역시", "울산"),
        ("세종특별자치시", "세종"),
        ("경기도", "경기"),
        ("강원도", "강원"),
        ("충청북도", "충북"),
        ("충청남도", "충남"),
        ("전라북도", "전북"),
        ("전라남도", "전남"),
        ("경상북도", "경북"),
        ("경상남도", "경남"),
        ("제주도", "제주"),
    ]
    for full, short in replacements:
        if s.startswith(full):
            return short + s[len(full):]
    return s


def get_geocode_cache():
    return load_persistent_json("geocode_cache", GEOCODE_CACHE_FILE, {})


def save_geocode_cache(cache):
    save_persistent_json("geocode_cache", GEOCODE_CACHE_FILE, cache)


def get_route_cache():
    global ROUTE_CACHE_MEMORY
    if ROUTE_CACHE_MEMORY is None:
        ROUTE_CACHE_MEMORY = load_persistent_json("route_cache", ROUTE_CACHE_FILE, {})
    return ROUTE_CACHE_MEMORY


def save_route_cache(cache=None, force=False):
    global ROUTE_CACHE_MEMORY, ROUTE_CACHE_DIRTY
    if cache is not None:
        ROUTE_CACHE_MEMORY = cache
        ROUTE_CACHE_DIRTY = True
    if force or ROUTE_CACHE_DIRTY:
        save_persistent_json("route_cache", ROUTE_CACHE_FILE, ROUTE_CACHE_MEMORY or {})
        ROUTE_CACHE_DIRTY = False


def cache_route_result(route_cache, cache_key, distance_m, duration_min, prediction_time):
    global ROUTE_CACHE_DIRTY
    route_cache[cache_key] = {
        "distance_m": int(distance_m),
        "duration_min": int(duration_min),
        "prediction_time": prediction_time,
        "updated_at": datetime.now().isoformat(),
    }
    ROUTE_CACHE_DIRTY = True


def extract_building_name(geocode_item):
    if not isinstance(geocode_item, dict):
        return ""

    for element in geocode_item.get("addressElements") or []:
        types = element.get("types") or element.get("type") or []
        if isinstance(types, str):
            types = [types]
        if "BUILDING_NAME" in types:
            name = str(element.get("longName") or element.get("shortName") or "").strip()
            if name:
                return name
    return ""


def format_display_address(address, building_name=""):
    address = (address or "").strip()
    building_name = (building_name or "").strip()
    if not address or not building_name:
        return address
    if building_name in address:
        return address
    return f"{address} ({building_name})"


def geocode_with_meta(address: str):
    query = (address or "").strip()
    coord, err = geocode(query)
    if not coord:
        return None, None, err

    cache = get_geocode_cache()
    item = cache.get(query) or {}
    building_name = (item.get("building_name") or "").strip()
    if building_name:
        return coord, {"building_name": building_name, "display_address": format_display_address(query, building_name)}, None

    url = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
    headers = get_api_headers()
    if headers["X-NCP-APIGW-API-KEY-ID"] and headers["X-NCP-APIGW-API-KEY"]:
        try:
            resp = requests.get(url, headers=headers, params={"query": query}, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                addresses = data.get("addresses") or []
                if addresses:
                    building_name = extract_building_name(addresses[0])
                    if building_name:
                        item.update({
                            "x": coord[0],
                            "y": coord[1],
                            "building_name": building_name,
                            "updated_at": datetime.now().isoformat(),
                        })
                        cache[query] = item
                        save_geocode_cache(cache)
        except Exception:
            pass

    return coord, {"building_name": building_name, "display_address": format_display_address(query, building_name)}, None


def geocode(address: str):
    url = "https://maps.apigw.ntruss.com/map-geocode/v2/geocode"
    query = (address or "").strip()
    params = {"query": query}
    headers = get_api_headers()

    if not query:
        return None, "주소가 비어 있습니다."

    if not headers["X-NCP-APIGW-API-KEY-ID"] or not headers["X-NCP-APIGW-API-KEY"]:
        return None, "관리자 설정에서 API 키가 입력되지 않았습니다."

    cache = get_geocode_cache()
    if query in cache:
        item = cache[query]
        try:
            return (float(item["x"]), float(item["y"])), None
        except Exception:
            pass

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
    except requests.RequestException as e:
        return None, f"지오코딩 요청 실패: {e}"

    if resp.status_code != 200:
        try:
            data = resp.json()
            msg = data.get("errorMessage") or data.get("message") or resp.text
        except Exception:
            msg = resp.text
        return None, f"지오코딩 HTTP {resp.status_code}: {msg}"

    try:
        data = resp.json()
    except Exception:
        return None, "지오코딩 응답이 JSON 형식이 아닙니다."

    addresses = data.get("addresses") or []
    if not addresses:
        return None, f"지오코딩 결과 없음: {query}"

    try:
        x = float(addresses[0]["x"])
        y = float(addresses[0]["y"])
        cache[query] = {"x": x, "y": y, "updated_at": datetime.now().isoformat()}
        save_geocode_cache(cache)
        return (x, y), None
    except Exception:
        return None, f"지오코딩 결과 파싱 실패: {query}"


def estimate_matrix_leg(start, goal):
    lon1, lat1 = start
    lon2, lat2 = goal
    rad = math.pi / 180.0
    d_lat = (lat2 - lat1) * rad
    d_lon = (lon2 - lon1) * rad
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat1 * rad) * math.cos(lat2 * rad) * (math.sin(d_lon / 2) ** 2)
    )
    surface_m = 6371000 * 2 * math.asin(min(1, math.sqrt(a)))
    road_m = max(500, int(surface_m * 1.28))
    duration_min = max(3, int(math.ceil((road_m / 1000) / 28 * 60)))
    return road_m, duration_min


def straight_distance_m(start, goal):
    lon1, lat1 = start
    lon2, lat2 = goal
    rad = math.pi / 180.0
    d_lat = (lat2 - lat1) * rad
    d_lon = (lon2 - lon1) * rad
    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat1 * rad) * math.cos(lat2 * rad) * (math.sin(d_lon / 2) ** 2)
    )
    return 6371000 * 2 * math.asin(min(1, math.sqrt(a)))


def estimate_walk_minutes(distance_m):
    if distance_m <= 0:
        return 1
    # average walking speed: 4.2km/h
    return max(1, int(math.ceil((distance_m / 1000.0) / 4.2 * 60)))


def estimate_parking_drive_walk_minutes(direct_distance_m):
    direct_distance_m = max(0.0, float(direct_distance_m or 0.0))
    if direct_distance_m <= 60:
        return 1, 1

    # Parking-to-visit legs are short local moves. Use lighter detour factors than full route estimates.
    drive_distance_m = max(direct_distance_m * 1.12, direct_distance_m + 20)
    walk_distance_m = max(direct_distance_m * 1.06, direct_distance_m)

    drive_min = max(1, int(math.ceil((drive_distance_m / 1000.0) / 18.0 * 60)))
    walk_min = max(1, int(math.ceil((walk_distance_m / 1000.0) / 4.5 * 60)))

    # For very short hops, avoid unrealistic "driving much slower than walking" output.
    if direct_distance_m < 300 and drive_min > walk_min:
        drive_min = max(1, walk_min)
    return drive_min, walk_min


def enrich_route_with_nearby_parking(route_view, visits, visit_coords, parking_items, radius_m=1000, max_items=5):
    global PARKING_RESOLVED_CACHE_KEY, PARKING_RESOLVED_CACHE
    if not isinstance(route_view, list):
        return route_view
    radius_m = max(100, int(radius_m or 1000))
    max_items = max(1, int(max_items or 5))

    # Always expose the key for template safety.
    for item in route_view:
        if isinstance(item, dict) and item.get("type") == "visit":
            item["nearby_parkings"] = []

    visit_coord_map = {}
    for idx, visit in enumerate(visits):
        if idx >= len(visit_coords):
            continue
        visit_id = visit.get("visit_id")
        if visit_id is None:
            continue
        visit_coord_map[visit_id] = visit_coords[idx]

    parking_pairs = []
    for item in (parking_items or [])[:200]:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        address = str(item.get("address") or "").strip()
        if name and address:
            parking_pairs.append((name, address))
    cache_key = tuple(parking_pairs)

    if cache_key == PARKING_RESOLVED_CACHE_KEY:
        resolved_parking = PARKING_RESOLVED_CACHE
    else:
        resolved_parking = []
        for name, address in parking_pairs:
            try:
                coord, _, _ = geocode_with_meta(address)
            except Exception:
                continue
            if not coord:
                continue
            resolved_parking.append({
                "name": name,
                "address": address,
                "coord": coord,
            })
        PARKING_RESOLVED_CACHE_KEY = cache_key
        PARKING_RESOLVED_CACHE = resolved_parking

    for item in route_view:
        if not isinstance(item, dict) or item.get("type") != "visit":
            continue

        visit_id = item.get("visit_id")
        visit_coord = visit_coord_map.get(visit_id)
        if not visit_coord:
            item["nearby_parkings"] = []
            continue

        nearby = []
        for parking in resolved_parking:
            try:
                direct_m = straight_distance_m(visit_coord, parking["coord"])
            except Exception:
                continue
            if direct_m > radius_m:
                continue

            drive_min, walk_min = estimate_parking_drive_walk_minutes(direct_m)
            nearby.append({
                "name": parking["name"],
                "address": parking["address"],
                "drive_min": int(drive_min),
                "walk_min": int(walk_min),
                "distance_m": int(round(direct_m)),
            })

        nearby.sort(key=lambda x: (x["distance_m"], x["drive_min"], x["name"]))
        item["nearby_parkings"] = nearby[:max_items]

    return route_view


def fallback_route_info(start, goal):
    return estimate_matrix_leg(start, goal)


def build_prediction_time(trip_date, departure_min, bucket_minutes=10):
    trip_date = (trip_date or "").strip()
    if not trip_date:
        return None
    try:
        base = datetime.strptime(trip_date, "%Y-%m-%d").replace(tzinfo=ZoneInfo("Asia/Seoul"))
    except Exception:
        return None

    rounded_minute = max(0, int(departure_min // bucket_minutes) * bucket_minutes)
    dt = base.replace(hour=0, minute=0, second=0, microsecond=0)
    dt = dt.replace(hour=rounded_minute // 60, minute=rounded_minute % 60)
    return dt.strftime("%Y-%m-%dT%H:%M:%S%z")


def get_route_info(start, goal, prediction_time=None, route_cache=None):
    start_key = f"{round(start[0], 6)},{round(start[1], 6)}"
    goal_key = f"{round(goal[0], 6)},{round(goal[1], 6)}"
    time_key = prediction_time or "realtime"
    cache_key = f"{start_key}|{goal_key}|{time_key}"

    route_cache = route_cache if route_cache is not None else get_route_cache()
    if cache_key in route_cache:
        item = route_cache[cache_key]
        return int(item["distance_m"]), int(item["duration_min"])

    tmap_app_key = get_tmap_app_key()
    if tmap_app_key and prediction_time:
        url = "https://apis.openapi.sk.com/tmap/routes/prediction?version=1&format=json"
        headers = {
            "appKey": tmap_app_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        payload = {
            "routesInfo": {
                "departure": {
                    "name": "출발지",
                    "lon": str(start[0]),
                    "lat": str(start[1]),
                    "type": "s",
                },
                "destination": {
                    "name": "도착지",
                    "lon": str(goal[0]),
                    "lat": str(goal[1]),
                    "type": "e",
                },
                "predictionType": "departure",
                "predictionTime": prediction_time,
                "searchOption": "00",
                "trafficInfo": "Y",
            }
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            if resp.status_code == 200:
                data = resp.json()
                features = data.get("features") or []
                properties = (features[0] or {}).get("properties") if features else {}
                if properties:
                    distance_m = int(properties.get("totalDistance", 99999999))
                    total_seconds = int(properties.get("totalTime", 999999))
                    duration_min = max(1, int(math.ceil(total_seconds / 60)))
                    cache_route_result(route_cache, cache_key, distance_m, duration_min, prediction_time)
                    return distance_m, duration_min
        except requests.RequestException:
            pass
        except Exception:
            pass

    url = "https://maps.apigw.ntruss.com/map-direction/v1/driving"
    headers = get_api_headers()
    if not headers["X-NCP-APIGW-API-KEY-ID"] or not headers["X-NCP-APIGW-API-KEY"]:
        return fallback_route_info(start, goal)

    params = {
        "start": f"{start[0]},{start[1]}",
        "goal": f"{goal[0]},{goal[1]}",
        "option": "trafast",
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
    except requests.RequestException:
        return fallback_route_info(start, goal)

    if resp.status_code != 200:
        return fallback_route_info(start, goal)

    try:
        data = resp.json()
        summary = data["route"]["trafast"][0]["summary"]
        distance_m = int(summary["distance"])
        duration_min = int(summary["duration"]) // 60000
        cache_route_result(route_cache, cache_key, distance_m, duration_min, prediction_time)
        return distance_m, duration_min
    except Exception:
        return fallback_route_info(start, goal)


def get_trip_meta():
    return {
        "work_type": session.get("work_type", "visit"),
        "user_name": session.get("user_name", ""),
        "team_no": session.get("team_no", ""),
        "trip_date": session.get("trip_date", "")
    }


def route_distance_with_return(order, dist_matrix):
    if not order:
        return 0
    total = dist_matrix[0][order[0]]
    for i in range(len(order) - 1):
        total += dist_matrix[order[i]][order[i + 1]]
    total += dist_matrix[order[-1]][0]
    return total


def nearest_neighbor_seed(visits, dist_matrix):
    n = len(visits)
    remaining = set(range(1, n + 1))
    current = 0
    order = []

    while remaining:
        nxt = min(remaining, key=lambda x: (dist_matrix[current][x], x))
        order.append(nxt)
        remaining.remove(nxt)
        current = nxt

    return order


def greedy_appointment_seed(visits, dist_matrix, time_matrix):
    remaining = set(range(1, len(visits) + 1))
    order = []
    current = 0
    current_time = DAY_START

    while remaining:
        scored = []
        for node in remaining:
            travel = time_matrix[current][node]
            arrival = current_time + travel
            visit = visits[node - 1]

            penalty = 0
            if visit["has_appointment"]:
                target = visit["appointment_minute"]
                if arrival > target:
                    penalty += (arrival - target) * 1000

            score = dist_matrix[current][node] + penalty
            scored.append((score, node))

        scored.sort()
        nxt = scored[0][1]
        visit = visits[nxt - 1]
        arrival = current_time + time_matrix[current][nxt]

        current_time = arrival + visit["service_time"]
        order.append(nxt)
        remaining.remove(nxt)
        current = nxt

    return order


def estimate_order_metrics(path, visits, dist_matrix, time_matrix):
    if not path:
        return {
            "appointment_violations": 0,
            "appointment_late_total": 0,
            "finish_time": DAY_START,
            "total_distance": 0,
            "locality_penalty": 0,
            "wait_total": 0,
        }

    current_time = DAY_START
    last = 0
    total_dist = 0
    locality_penalty = 0
    appointment_violations = 0
    appointment_late_total = 0
    wait_total = 0

    for idx, node in enumerate(path):
        travel = time_matrix[last][node]
        total_dist += dist_matrix[last][node]
        arrival = current_time + travel
        visit = visits[node - 1]

        if visit["has_appointment"]:
            target = visit["appointment_minute"]
            if arrival > target:
                appointment_violations += 1
                appointment_late_total += arrival - target

        current_time = arrival + visit["service_time"]

        if idx >= 1:
            locality_penalty += int(dist_matrix[path[idx - 1]][node] * 0.03)

        last = node

    finish_time = current_time + time_matrix[last][0]
    total_dist += dist_matrix[last][0]

    return {
        "appointment_violations": appointment_violations,
        "appointment_late_total": appointment_late_total,
        "finish_time": finish_time,
        "total_distance": total_dist,
        "locality_penalty": locality_penalty,
        "wait_total": wait_total,
    }


def partial_path_score(path, visits, dist_matrix, time_matrix):
    metrics = estimate_order_metrics(path, visits, dist_matrix, time_matrix)
    return (
        metrics["appointment_violations"] * 10**9
        + metrics["appointment_late_total"] * 10**6
        + metrics["finish_time"] * 10**3
        + metrics["total_distance"]
        + metrics["locality_penalty"]
        + metrics["wait_total"] * 5
    )


def beam_search_route_candidates(visits, dist_matrix, time_matrix, top_k=BEAM_WIDTH):
    n = len(visits)
    if n == 0:
        return [[]]

    seeds = []
    nn = nearest_neighbor_seed(visits, dist_matrix)
    ga = greedy_appointment_seed(visits, dist_matrix, time_matrix)
    seeds.append(nn)
    if ga != nn:
        seeds.append(ga)

    appointment_nodes = [i for i, v in enumerate(visits, start=1) if v["has_appointment"]]
    if appointment_nodes:
        earliest_appt_node = min(appointment_nodes, key=lambda x: visits[x - 1]["appointment_minute"])
        remaining = [x for x in range(1, n + 1) if x != earliest_appt_node]
        remaining.sort(key=lambda x: dist_matrix[earliest_appt_node][x])
        seeds.append([earliest_appt_node] + remaining)

    best_seed = min(seeds, key=lambda order: partial_path_score(order, visits, dist_matrix, time_matrix))

    first_candidates = best_seed[:min(BEAM_WIDTH, len(best_seed))]
    init_beams = []
    for node in first_candidates:
        init_beams.append(([node], partial_path_score([node], visits, dist_matrix, time_matrix)))
    init_beams.sort(key=lambda x: x[1])
    beams = init_beams[:BEAM_WIDTH]

    for _step in range(1, n):
        candidates = []

        for path, _score in beams:
            visited = set(path)
            last = path[-1]

            remaining = [x for x in range(1, n + 1) if x not in visited]
            remaining.sort(key=lambda x: dist_matrix[last][x])
            remaining = remaining[:min(len(remaining), 6)]

            for node in range(1, n + 1):
                if node not in visited and visits[node - 1]["has_appointment"] and node not in remaining:
                    remaining.append(node)

            for nxt in remaining:
                new_path = path + [nxt]
                score = partial_path_score(new_path, visits, dist_matrix, time_matrix)
                candidates.append((new_path, score))

        candidates.sort(key=lambda x: x[1])
        beams = candidates[:BEAM_WIDTH]

        if len(candidates) > MAX_PARTIAL_CANDIDATES:
            beams = beams[:BEAM_WIDTH]

    beams.sort(key=lambda x: x[1])
    unique_orders = []
    seen = set()
    for path, _score in beams:
        key = tuple(path)
        if key in seen:
            continue
        seen.add(key)
        unique_orders.append(path)

    if not unique_orders:
        unique_orders = [best_seed]

    return unique_orders[:max(1, int(top_k or 1))]


def beam_search_route(visits, dist_matrix, time_matrix):
    return beam_search_route_candidates(visits, dist_matrix, time_matrix, top_k=1)[0]


def two_opt(order, dist_matrix, time_matrix, visits, max_iter=LOCAL_IMPROVE_ITER):
    if len(order) <= 3:
        return order[:]

    def score(candidate):
        return partial_path_score(candidate, visits, dist_matrix, time_matrix) + route_distance_with_return(candidate, dist_matrix)

    best = order[:]
    best_score = score(best)
    improved = True
    iter_count = 0

    while improved and iter_count < max_iter:
        improved = False
        iter_count += 1

        for i in range(len(best) - 1):
            for j in range(i + 2, len(best) + 1):
                if j - i <= 1:
                    continue
                candidate = best[:i] + list(reversed(best[i:j])) + best[j:]
                cand_score = score(candidate)

                if cand_score < best_score:
                    best = candidate
                    best_score = cand_score
                    improved = True
                    break
            if improved:
                break

    return best


def relocate_improve(order, dist_matrix, time_matrix, visits, max_iter=LOCAL_IMPROVE_ITER):
    if len(order) <= 2:
        return order[:]

    def score(candidate):
        return partial_path_score(candidate, visits, dist_matrix, time_matrix) + route_distance_with_return(candidate, dist_matrix)

    best = order[:]
    best_score = score(best)
    improved = True
    iter_count = 0

    while improved and iter_count < max_iter:
        improved = False
        iter_count += 1

        for i in range(len(best)):
            node = best[i]
            reduced = best[:i] + best[i + 1:]

            for j in range(len(reduced) + 1):
                candidate = reduced[:j] + [node] + reduced[j:]
                cand_score = score(candidate)

                if cand_score < best_score:
                    best = candidate
                    best_score = cand_score
                    improved = True
                    break
            if improved:
                break

    return best


def or_opt_improve(order, dist_matrix, time_matrix, visits, max_iter=LOCAL_IMPROVE_ITER):
    if len(order) <= 3:
        return order[:]

    def score(candidate):
        return partial_path_score(candidate, visits, dist_matrix, time_matrix) + route_distance_with_return(candidate, dist_matrix)

    best = order[:]
    best_score = score(best)
    improved = True
    iter_count = 0

    while improved and iter_count < max_iter:
        improved = False
        iter_count += 1

        for seg_len in (1, 2, 3):
            if seg_len >= len(best):
                continue
            for i in range(len(best) - seg_len + 1):
                segment = best[i:i + seg_len]
                reduced = best[:i] + best[i + seg_len:]
                for j in range(len(reduced) + 1):
                    if j == i:
                        continue
                    candidate = reduced[:j] + segment + reduced[j:]
                    cand_score = score(candidate)
                    if cand_score < best_score:
                        best = candidate
                        best_score = cand_score
                        improved = True
                        break
                if improved:
                    break
            if improved:
                break

    return best


def optimize_route_from_order(base_order, visits, dist_matrix, time_matrix):
    order = list(base_order or [])
    order = two_opt(order, dist_matrix, time_matrix, visits)
    order = relocate_improve(order, dist_matrix, time_matrix, visits)
    order = or_opt_improve(order, dist_matrix, time_matrix, visits)
    order = two_opt(order, dist_matrix, time_matrix, visits)
    return order


def optimize_route(visits, dist_matrix, time_matrix):
    order = beam_search_route(visits, dist_matrix, time_matrix)
    return optimize_route_from_order(order, visits, dist_matrix, time_matrix)


def optimize_route_candidates(visits, dist_matrix, time_matrix, max_candidates=ORDER_CANDIDATE_LIMIT):
    n = len(visits)
    if n == 0:
        return [[]]

    max_candidates = max(1, int(max_candidates or 1))

    seeds = []
    nn = nearest_neighbor_seed(visits, dist_matrix)
    ga = greedy_appointment_seed(visits, dist_matrix, time_matrix)
    seeds.append(nn)
    if ga != nn:
        seeds.append(ga)

    appointment_nodes = [i for i, v in enumerate(visits, start=1) if v["has_appointment"]]
    if appointment_nodes:
        earliest_appt_node = min(
            appointment_nodes,
            key=lambda x: visits[x - 1]["appointment_minute"] if visits[x - 1]["appointment_minute"] is not None else (24 * 60),
        )
        remaining = [x for x in range(1, n + 1) if x != earliest_appt_node]
        remaining.sort(key=lambda x: dist_matrix[earliest_appt_node][x])
        seeds.append([earliest_appt_node] + remaining)

    beam_orders = beam_search_route_candidates(visits, dist_matrix, time_matrix, top_k=max(BEAM_WIDTH, max_candidates))
    seeds.extend(beam_orders)

    unique_seed_orders = []
    seen_seed = set()
    for order in seeds:
        key = tuple(order)
        if len(order) != n or key in seen_seed:
            continue
        seen_seed.add(key)
        unique_seed_orders.append(order)
        if len(unique_seed_orders) >= max_candidates * 2:
            break

    optimized_orders = []
    for base in unique_seed_orders:
        improved = optimize_route_from_order(base, visits, dist_matrix, time_matrix)
        optimized_orders.append(improved)
        optimized_orders.append(base)

    ranked = []
    seen_ranked = set()
    for order in optimized_orders:
        key = tuple(order)
        if len(order) != n or key in seen_ranked:
            continue
        seen_ranked.add(key)
        score = partial_path_score(order, visits, dist_matrix, time_matrix) + route_distance_with_return(order, dist_matrix)
        ranked.append((score, order))

    ranked.sort(key=lambda x: x[0])
    if not ranked:
        return [optimize_route(visits, dist_matrix, time_matrix)]

    return [order for _score, order in ranked[:max_candidates]]


def append_wait_block(route_view, start_min, end_min, name="대기"):
    if end_min <= start_min:
        return 0

    if route_view and route_view[-1].get("type") == "wait":
        prev = route_view[-1]
        prev_end_h, prev_end_m = map(int, prev["end_time"].split(":"))
        prev_end = prev_end_h * 60 + prev_end_m

        prev_start_h, prev_start_m = map(int, prev["arrival"].split(":"))
        prev_start = prev_start_h * 60 + prev_start_m

        if start_min <= prev_end:
            new_end = max(prev_end, end_min)
            prev["end_time"] = minutes_to_str(new_end)
            prev["service_time"] = new_end - prev_start
            return 0

    route_view.append({
        "type": "wait",
        "label": "W",
        "name": "대기" if "대기" in str(name) else (name or "대기"),
        "address": "",
        "arrival": minutes_to_str(start_min),
        "end_time": minutes_to_str(end_min),
        "service_time": end_min - start_min,
        "travel_km": None,
        "travel_min": None,
    })
    return 1


def append_lunch_block(route_view, start_min):
    route_view.append({
        "type": "lunch",
        "label": "L",
        "name": "점심",
        "address": "",
        "arrival": minutes_to_str(start_min),
        "end_time": minutes_to_str(start_min + LUNCH_DURATION),
        "service_time": LUNCH_DURATION,
        "travel_km": None,
        "travel_min": None,
    })


def add_visit_block(route_view, visit_no, visit, arrival_min, travel_m, travel_min):
    route_view.append({
        "type": "visit",
        "label": str(visit_no),
        "visit_id": visit.get("visit_id"),
        "name": visit["name"],
        "address": shorten_sido_name(visit.get("display_address") or visit["address"]),
        "arrival": minutes_to_str(arrival_min),
        "end_time": minutes_to_str(arrival_min + visit["service_time"]),
        "service_time": visit["service_time"],
        "travel_km": round(travel_m / 1000, 1),
        "travel_min": int(travel_min),
        "appointment_time": minutes_to_str(visit["appointment_minute"]) if visit["has_appointment"] else None,
    })


def add_return_block(route_view, arrival_min, travel_m, travel_min, return_address=None, return_name=None):
    route_view.append({
        "type": "return",
        "label": "R",
        "name": "복귀",
        "address": shorten_sido_name(return_address or get_return_address()),
        "arrival": minutes_to_str(arrival_min),
        "end_time": minutes_to_str(arrival_min),
        "service_time": 0,
        "travel_km": round(travel_m / 1000, 1),
        "travel_min": int(travel_min),
    })


def clone_route(route_view):
    return [dict(item) for item in route_view]


def count_intra_wait_blocks(route_view):
    penalty = 0
    for i in range(1, len(route_view) - 1):
        prev = route_view[i - 1]
        cur = route_view[i]
        nxt = route_view[i + 1]
        if prev["type"] == "visit" and cur["type"] == "wait" and nxt["type"] == "visit":
            penalty += 1
    return penalty


def maybe_insert_lunch(route_view, current_time, lunch_used, wait_label):
    options = []
    if lunch_used:
        return options

    if LUNCH_START_MIN <= current_time <= LUNCH_START_MAX:
        new_route = clone_route(route_view)
        append_lunch_block(new_route, current_time)
        options.append({
            "time_after_pre": current_time + LUNCH_DURATION,
            "lunch_used": True,
            "route": new_route,
            "wait_count": 0,
            "wait_total": 0
        })

    if current_time < LUNCH_START_MIN:
        new_route = clone_route(route_view)
        wc = append_wait_block(new_route, current_time, LUNCH_START_MIN, wait_label)
        append_lunch_block(new_route, LUNCH_START_MIN)
        options.append({
            "time_after_pre": LUNCH_START_MIN + LUNCH_DURATION,
            "lunch_used": True,
            "route": new_route,
            "wait_count": wc,
            "wait_total": LUNCH_START_MIN - current_time
        })

    options.sort(key=lambda x: (x["wait_total"], x["wait_count"], 0 if x["lunch_used"] else 1))
    return options


def best_depart_with_lunch(route_view, current_time, depart_time, lunch_used, wait_label):
    if depart_time < current_time:
        return [{
            "time_after_pre": current_time,
            "lunch_used": lunch_used,
            "route": clone_route(route_view),
            "wait_count": 0,
            "wait_total": 0
        }]

    candidates = []

    plain_route = clone_route(route_view)
    wc = append_wait_block(plain_route, current_time, depart_time, wait_label)
    candidates.append({
        "time_after_pre": depart_time,
        "lunch_used": lunch_used,
        "route": plain_route,
        "wait_count": wc,
        "wait_total": depart_time - current_time
    })

    if lunch_used:
        candidates.sort(key=lambda x: (x["wait_total"], x["wait_count"]))
        return candidates

    slack = depart_time - current_time

    if slack >= LUNCH_DURATION:
        lunch_start_a = max(current_time, LUNCH_START_MIN)
        if lunch_start_a <= LUNCH_START_MAX and lunch_start_a + LUNCH_DURATION <= depart_time:
            route_a = clone_route(route_view)
            wait_count_a = 0
            wait_total_a = 0

            if lunch_start_a > current_time:
                wait_count_a += append_wait_block(route_a, current_time, lunch_start_a, wait_label)
                wait_total_a += lunch_start_a - current_time

            append_lunch_block(route_a, lunch_start_a)
            lunch_end_a = lunch_start_a + LUNCH_DURATION

            if depart_time > lunch_end_a:
                wait_count_a += append_wait_block(route_a, lunch_end_a, depart_time, wait_label)
                wait_total_a += depart_time - lunch_end_a

            if lunch_start_a == current_time:
                candidates.append({
                    "time_after_pre": depart_time,
                    "lunch_used": True,
                    "route": route_a,
                    "wait_count": wait_count_a,
                    "wait_total": wait_total_a
                })

        lunch_start_b = depart_time - LUNCH_DURATION
        if LUNCH_START_MIN <= lunch_start_b <= LUNCH_START_MAX and lunch_start_b >= current_time:
            route_b = clone_route(route_view)
            wait_count_b = 0
            wait_total_b = 0

            if lunch_start_b > current_time:
                wait_count_b += append_wait_block(route_b, current_time, lunch_start_b, wait_label)
                wait_total_b += lunch_start_b - current_time

            append_lunch_block(route_b, lunch_start_b)

            candidates.append({
                "time_after_pre": depart_time,
                "lunch_used": True,
                "route": route_b,
                "wait_count": wait_count_b,
                "wait_total": wait_total_b
            })

    candidates.sort(key=lambda x: (x["wait_total"], x["wait_count"], 0 if x["lunch_used"] else 1))
    return candidates


def compress_route_view(route_view):
    if not route_view:
        return route_view

    compressed = []
    for item in route_view:
        if item.get("type") != "wait":
            compressed.append(dict(item))
            continue

        if compressed and compressed[-1].get("type") == "wait":
            prev = compressed[-1]

            prev_start_h, prev_start_m = map(int, prev["arrival"].split(":"))
            prev_end_h, prev_end_m = map(int, prev["end_time"].split(":"))
            cur_start_h, cur_start_m = map(int, item["arrival"].split(":"))
            cur_end_h, cur_end_m = map(int, item["end_time"].split(":"))

            prev_start = prev_start_h * 60 + prev_start_m
            prev_end = prev_end_h * 60 + prev_end_m
            cur_start = cur_start_h * 60 + cur_start_m
            cur_end = cur_end_h * 60 + cur_end_m

            if cur_start <= prev_end:
                new_end = max(prev_end, cur_end)
                prev["end_time"] = minutes_to_str(new_end)
                prev["service_time"] = new_end - prev_start
            else:
                compressed.append(dict(item))
        else:
            compressed.append(dict(item))

    return compressed


def normalize_pre_lunch_wait(route_view):
    if not route_view:
        return route_view

    normalized = [dict(item) for item in route_view]
    start_idx = next((i for i, item in enumerate(normalized) if item.get("type") == "start"), None)
    if start_idx is None:
        return normalized

    movable_wait = 0
    remove_indexes = []

    for idx in range(1, len(normalized) - 1):
        item = normalized[idx]
        if item.get("type") != "wait":
            continue
        prev_item = normalized[idx - 1]
        next_item = normalized[idx + 1]
        wait_start = str_to_minutes(item.get("arrival"))
        wait_end = str_to_minutes(item.get("end_time"))
        if wait_start >= LUNCH_START_MAX:
            continue
        if next_item.get("type") == "visit" and next_item.get("appointment_time"):
            continue
        if prev_item.get("type") in ("visit", "wait") and next_item.get("type") in ("visit", "lunch", "return"):
            movable_wait += max(0, wait_end - wait_start)
            remove_indexes.append(idx)

    if not movable_wait:
        return normalized

    normalized = [item for i, item in enumerate(normalized) if i not in remove_indexes]
    start_item = normalized[start_idx]
    start_time = str_to_minutes(start_item.get("arrival"))
    insert_wait = {
        "type": "wait",
        "label": "W",
        "name": "출발지 대기",
        "address": "",
        "arrival": minutes_to_str(start_time),
        "end_time": minutes_to_str(start_time + movable_wait),
        "service_time": movable_wait,
        "travel_km": None,
        "travel_min": None,
    }

    normalized.insert(start_idx + 1, insert_wait)
    return compress_route_view(normalized)


def is_lunch_required(departure_time, return_time):
    departure_time = int(departure_time)
    return_time = int(return_time)
    if return_time <= LUNCH_SKIP_IF_RETURN_BY:
        return False
    if departure_time >= LUNCH_SKIP_IF_DEPART_AFTER:
        return False
    return True


def build_departure_candidates(order, visits, time_matrix):
    candidates = {DAY_START}
    appointment_anchors = set()
    if not order:
        return [DAY_START]

    has_appointment = any(visits[node - 1]["has_appointment"] for node in order)
    if not has_appointment:
        return [DAY_START]

    cumulative_min = 0
    for idx, node in enumerate(order):
        prev_node = 0 if idx == 0 else order[idx - 1]
        cumulative_min += time_matrix[prev_node][node]
        visit = visits[node - 1]
        if visit["has_appointment"] and visit.get("appointment_minute") is not None:
            latest_departure = visit["appointment_minute"] - cumulative_min
            if latest_departure >= DAY_START:
                appointment_anchors.add(latest_departure)
            for offset in (0, -15, -30, -45, -60):
                shifted = latest_departure + offset
                if shifted >= DAY_START:
                    candidates.add(shifted)
        cumulative_min += visit["service_time"]

    total_route_min = cumulative_min + time_matrix[order[-1]][0]
    latest_by_return = RETURN_LIMIT - total_route_min
    for offset in (0, -15, -30):
        shifted = latest_by_return + offset
        if shifted >= DAY_START:
            candidates.add(shifted)

    if any(candidate >= LUNCH_SKIP_IF_DEPART_AFTER for candidate in candidates):
        candidates.add(LUNCH_SKIP_IF_DEPART_AFTER)

    bounded = {
        min(RETURN_LIMIT, max(DAY_START, int(candidate)))
        for candidate in candidates
    }
    ordered = sorted(bounded)
    if len(ordered) <= 4:
        return sorted(set(ordered) | appointment_anchors)

    reduced = []
    for candidate in reversed(ordered):
        if all(abs(candidate - existing) >= 15 for existing in reduced):
            reduced.append(candidate)
        if len(reduced) >= 4:
            break

    reduced.append(DAY_START)
    # Keep appointment-derived anchors so we can depart close to promised times
    # even after candidate list reduction.
    return sorted(set(reduced) | appointment_anchors)


def simulate_order(order, visits, time_matrix, distance_matrix, start_display_address=None, return_display_address=None, coords=None, trip_date=None, start_time=DAY_START, leg_cache=None, route_cache=None):
    best_result = None
    leg_cache = leg_cache if leg_cache is not None else {}
    route_cache = route_cache if route_cache is not None else get_route_cache()

    def resolve_leg(from_node, to_node, departure_min):
        cache_key = (from_node, to_node, int(departure_min))
        if cache_key in leg_cache:
            return leg_cache[cache_key]

        prediction_time = build_prediction_time(trip_date, departure_min)
        if USE_TRAFFIC_FOR_PLANNING and coords and prediction_time:
            resolved = get_route_info(coords[from_node], coords[to_node], prediction_time, route_cache=route_cache)
        else:
            resolved = (distance_matrix[from_node][to_node], time_matrix[from_node][to_node])

        leg_cache[cache_key] = resolved
        return resolved

    def consider_result(result):
        nonlocal best_result
        result["route_view"] = compress_route_view(result["route_view"])
        result["intra_wait_count"] = count_intra_wait_blocks(result["route_view"])
        result["locality_penalty"] = int(result["total_distance_m"] * 0.03) + (result["intra_wait_count"] * 500)
        result["departure_time"] = start_time
        result["lunch_required"] = is_lunch_required(start_time, result["return_time"])
        result["lunch_used"] = any(item.get("type") == "lunch" for item in result["route_view"])
        result["lunch_penalty"] = 0 if (not result["lunch_required"] and not result["lunch_used"]) else 1

        score = (
            result["appointment_violation_count"],
            result["appointment_late_total"],
            result["lunch_penalty"],
            -result["departure_time"],
            result["return_time"],
            result["total_distance_m"],
            result["locality_penalty"],
            result["wait_total"],
            result["wait_count"],
        )

        result["score"] = score
        if best_result is None or score < best_result["score"]:
            best_result = result

    def dfs(idx, last_node, current_time, lunch_used, route_view, total_distance_m, total_travel_min,
            wait_count, wait_total, appointment_violation_count, appointment_late_total, visit_no):
        if idx == len(order):
            end_route = clone_route(route_view)
            effective_end = current_time
            if order:
                dist_back_preview, travel_back_preview = resolve_leg(last_node, 0, effective_end)
            else:
                dist_back_preview = 0
                travel_back_preview = 0

            preview_return_time = effective_end + travel_back_preview
            lunch_optional = not is_lunch_required(start_time, preview_return_time)

            if not lunch_used and not lunch_optional:
                if current_time > LUNCH_START_MAX:
                    return

                added_wait = 0
                added_wait_total = 0

                if current_time < LUNCH_START_MIN:
                    added_wait = append_wait_block(end_route, current_time, LUNCH_START_MIN, "대기")
                    added_wait_total = LUNCH_START_MIN - current_time
                    lunch_start = LUNCH_START_MIN
                else:
                    lunch_start = current_time

                append_lunch_block(end_route, lunch_start)
                effective_end = lunch_start + LUNCH_DURATION
                wait_count += added_wait
                wait_total += added_wait_total

            if order:
                dist_back, travel_back = resolve_leg(last_node, 0, effective_end)
            else:
                dist_back = 0
                travel_back = 0

            return_time = effective_end + travel_back
            add_return_block(
                end_route,
                return_time,
                dist_back,
                travel_back,
                return_display_address,
                get_return_name(),
            )

            consider_result({
                "route_view": end_route,
                "return_time": return_time,
                "return_late": max(0, return_time - RETURN_LIMIT),
                "wait_count": wait_count,
                "wait_total": wait_total,
                "total_distance_m": total_distance_m + dist_back,
                "total_travel_min": total_travel_min + travel_back,
                "appointment_violation_count": appointment_violation_count,
                "appointment_late_total": appointment_late_total,
                "intra_wait_count": 0,
            })
            return

        node = order[idx]
        visit = visits[node - 1]
        travel_m, travel_min = resolve_leg(last_node, node, current_time)
        wait_label = "출발지 대기" if idx == 0 and last_node == 0 else "대기"

        pre_options = [{
            "time_after_pre": current_time,
            "lunch_used": lunch_used,
            "route": clone_route(route_view),
            "wait_count": 0,
            "wait_total": 0
        }]

        expanded = []
        for pre in pre_options:
            expanded.append(pre)
            expanded.extend(maybe_insert_lunch(
                pre["route"],
                pre["time_after_pre"],
                pre["lunch_used"],
                wait_label
            ))
        pre_options = expanded

        for pre in pre_options:
            depart_time = pre["time_after_pre"]
            arrival_time = depart_time + travel_min

            appt_violation = appointment_violation_count
            appt_late = appointment_late_total
            added_wait_count = 0
            added_wait_total = 0
            new_route = clone_route(pre["route"])

            if visit["has_appointment"]:
                target = visit["appointment_minute"]
                if target is not None and arrival_time > target:
                    # Appointment is a hard constraint: skip this branch immediately.
                    continue
                if target is not None and arrival_time < target:
                    # Keep movement first and waiting after arrival when both are adjacent.
                    # (travel -> wait is allowed, wait -> travel is not allowed)
                    added_wait_count = append_wait_block(new_route, arrival_time, target, wait_label)
                    added_wait_total = target - arrival_time
                    arrival_time = target

            add_visit_block(new_route, visit_no + 1, visit, arrival_time, travel_m, travel_min)
            new_time = arrival_time + visit["service_time"]

            dfs(
                idx + 1, node, new_time, pre["lunch_used"], new_route,
                total_distance_m + travel_m, total_travel_min + travel_min,
                wait_count + pre["wait_count"] + added_wait_count,
                wait_total + pre["wait_total"] + added_wait_total,
                appt_violation, appt_late, visit_no + 1
            )

    initial_route = [{
        "type": "start",
        "label": "S",
        "name": "출발",
        "address": shorten_sido_name(start_display_address or get_start_address()),
        "arrival": minutes_to_str(start_time),
        "end_time": minutes_to_str(start_time),
        "service_time": 0,
        "travel_km": None,
        "travel_min": None
    }]

    dfs(0, 0, start_time, False, initial_route, 0, 0, 0, 0, 0, 0, 0)
    return best_result


def choose_best_schedule(visits, distance_matrix, time_matrix, start_display_address=None, return_display_address=None, coords=None, trip_date=None):
    route_cache = get_route_cache()
    shared_leg_cache = {}
    if not visits:
        result = simulate_order([], visits, time_matrix, distance_matrix, start_display_address, return_display_address, coords, trip_date, leg_cache=shared_leg_cache, route_cache=route_cache)
        save_route_cache(force=True)
        return [], result

    best = None
    best_order = []
    candidate_orders = optimize_route_candidates(visits, distance_matrix, time_matrix, max_candidates=ORDER_CANDIDATE_LIMIT)
    if candidate_orders:
        best_order = candidate_orders[0]

    for order in candidate_orders:
        for start_time in build_departure_candidates(order, visits, time_matrix):
            candidate = simulate_order(
                order,
                visits,
                time_matrix,
                distance_matrix,
                start_display_address,
                return_display_address,
                coords,
                trip_date,
                start_time=start_time,
                leg_cache=shared_leg_cache,
                route_cache=route_cache,
            )
            if candidate is None:
                continue
            if best is None or candidate["score"] < best["score"]:
                best = candidate
                best_order = order

    if best is None:
        save_route_cache(force=True)
        return best_order, None

    save_route_cache(force=True)
    return best_order, best


def choose_shortest_distance_order(visits, distance_matrix):
    if not visits:
        return []

    order = nearest_neighbor_seed(visits, distance_matrix)
    best_order = order[:]
    best_distance = route_distance_with_return(order, distance_matrix)
    n = len(visits)

    min_outbound = []
    for node in range(0, n + 1):
        candidates = [distance_matrix[node][other] for other in range(0, n + 1) if other != node]
        min_outbound.append(min(candidates) if candidates else 0)

    def bound_distance(last_node, remaining, current_distance):
        estimate = current_distance
        if remaining:
            estimate += min(distance_matrix[last_node][node] for node in remaining)
            estimate += sum(min_outbound[node] for node in remaining)
        else:
            estimate += distance_matrix[last_node][0]
        return estimate

    def dfs(last_node, remaining, path, current_distance):
        nonlocal best_order, best_distance
        if not remaining:
            total = current_distance + distance_matrix[last_node][0]
            if total < best_distance:
                best_distance = total
                best_order = path[:]
            return

        if bound_distance(last_node, remaining, current_distance) >= best_distance:
            return

        for nxt in sorted(remaining, key=lambda node: (distance_matrix[last_node][node], node)):
            path.append(nxt)
            dfs(
                nxt,
                remaining - {nxt},
                path,
                current_distance + distance_matrix[last_node][nxt],
            )
            path.pop()

    dfs(0, set(range(1, n + 1)), [], 0)
    return best_order


def build_phone_route_result(order, visits, distance_matrix, start_display_address=None, return_display_address=None):
    route_view = [{
        "type": "start",
        "label": "S",
        "name": get_start_name(),
        "address": shorten_sido_name(start_display_address or get_start_address()),
        "travel_km": None,
    }]

    total_distance_m = 0
    prev = 0
    for idx, node in enumerate(order, start=1):
        visit = visits[node - 1]
        travel_m = int(distance_matrix[prev][node])
        total_distance_m += travel_m
        route_view.append({
            "type": "visit",
            "label": str(idx),
            "visit_id": visit.get("visit_id"),
            "name": visit["name"],
            "address": shorten_sido_name(visit.get("display_address") or visit["address"]),
            "travel_km": round(travel_m / 1000, 1),
        })
        prev = node

    return_distance_m = int(distance_matrix[prev][0]) if order else 0
    total_distance_m += return_distance_m
    route_view.append({
        "type": "return",
        "label": "F",
        "name": get_return_name(),
        "address": shorten_sido_name(return_display_address or get_return_address()),
        "travel_km": round(return_distance_m / 1000, 1) if order else None,
    })

    return {
        "route_view": route_view,
        "total_distance_m": total_distance_m,
        "departure_time": DAY_START,
        "return_time": DAY_START,
    }


def choose_group_sizes(total_count, preferred_size, min_size=5, max_size=10):
    total_count = int(total_count)
    preferred_size = int(preferred_size)
    if total_count <= 0:
        return []
    if total_count < min_size:
        return [total_count]

    min_groups = math.ceil(total_count / max_size)
    max_groups = max(1, total_count // min_size)
    if min_groups > max_groups:
        return [total_count]

    best_sizes = None
    best_score = None
    for group_count in range(min_groups, max_groups + 1):
        base = total_count // group_count
        remainder = total_count % group_count
        sizes = [base + (1 if i < remainder else 0) for i in range(group_count)]
        if min(sizes) < min_size or max(sizes) > max_size:
            continue
        score = (
            sum(abs(size - preferred_size) for size in sizes),
            max(sizes) - min(sizes),
            abs(group_count - round(total_count / preferred_size)),
        )
        if best_score is None or score < best_score:
            best_score = score
            best_sizes = sizes

    return best_sizes or [total_count]


def build_phone_groups(visits, order, dist_matrix, preferred_size, start_display_address=None, return_display_address=None):
    sizes = choose_group_sizes(len(order), preferred_size)
    groups = []
    cursor = 0
    start_address = shorten_sido_name(start_display_address or get_start_address())
    return_address = shorten_sido_name(return_display_address or get_return_address())

    for group_no, size in enumerate(sizes, start=1):
        nodes = order[cursor:cursor + size]
        cursor += size
        items = [{
            "type": "start",
            "label": "S",
            "name": get_start_name(),
            "address": start_address,
        }]

        prev = 0
        total_distance_m = 0
        for idx, node in enumerate(nodes, start=1):
            visit = visits[node - 1]
            total_distance_m += int(dist_matrix[prev][node])
            items.append({
                "type": "visit",
                "label": str(idx),
                "name": visit["name"],
                "address": shorten_sido_name(visit.get("display_address") or visit["address"]),
            })
            prev = node

        if nodes:
            total_distance_m += int(dist_matrix[prev][0])

        items.append({
            "type": "return",
            "label": "F",
            "name": get_return_name(),
            "address": return_address,
        })

        groups.append({
            "group_no": group_no,
            "count": len(nodes),
            "distance_km": round(total_distance_m / 1000, 2),
            "items": items,
        })

    return groups


initialize_storage()
start_hyundai_mileage_collector()


@app.route("/vehicle-log", methods=["GET", "POST"])
def vehicle_log_page():
    vehicles, db_message = get_vehicle_log_vehicles()
    vehicle_map = {item.get("car_id"): item for item in vehicles}
    settings = load_settings()
    team_users_map = normalize_team_users((settings.get("user") or {}).get("team_users", {}))
    today_value = date.today()
    db_status = get_vehicle_log_db_status(today_value)
    default_start = date(today_value.year, 3, 1)
    default_end = date(today_value.year, 10, 31)
    default_month = today_value.month if 3 <= today_value.month <= 10 else 3

    if request.method == "POST":
        selected_car_id = (request.form.get("car_id") or "").strip()
        selected_month_raw = str(request.form.get("month") or "").strip()
        try:
            selected_month = int(selected_month_raw) if selected_month_raw else default_month
        except Exception:
            selected_month = default_month
        if selected_month < 3 or selected_month > 10:
            selected_month = default_month

        if not selected_car_id:
            flash("차량을 먼저 선택해 주세요.")
        else:
            try:
                rows_from_form = parse_vehicle_log_form_rows(request.form)
                changed_count = save_vehicle_log_form_rows(selected_car_id, rows_from_form)
                if changed_count:
                    flash(f"{changed_count}건의 운행 이력 수정값을 저장했습니다.")
                else:
                    flash("변경된 운행 이력이 없어 저장 대상이 없었습니다.")
            except Exception as exc:
                flash(str(exc))

        return redirect(url_for(
            "vehicle_log_page",
            car_id=selected_car_id,
            month=selected_month,
        ))

    selected_car_id = (request.args.get("car_id") or "").strip()
    if not selected_car_id:
        return render_template(
            "vehicle_log.html",
            vehicles=vehicles,
            selected_car_id="",
            selected_vehicle={},
            month_sections=[],
            selected_month=default_month,
            selected_month_data={"rows": [], "total_distance": 0},
            total_distance=0,
            total_distance_all=0,
            db_message=db_message,
            history_error="",
            main_driver="",
            row_count=0,
            default_recipient_email=get_default_recipient_email(),
            db_path=get_vehicle_log_db_path(),
            db_status=db_status,
            vehicle_team_names=list(team_users_map.keys()),
            vehicle_team_users=team_users_map,
        )

    rows = []
    history_error = ""
    if selected_car_id:
        rows, history_error = get_vehicle_log_history(selected_car_id, default_start, default_end)

    selected_vehicle = vehicle_map.get(selected_car_id) or {}
    rows = prepare_vehicle_log_display_rows(rows, selected_vehicle)
    month_sections = build_vehicle_log_month_sections(rows, today_value.year)

    selected_month_raw = str(request.args.get("month") or "").strip()
    try:
        selected_month = int(selected_month_raw) if selected_month_raw else default_month
    except Exception:
        selected_month = default_month
    if selected_month < 3 or selected_month > 10:
        selected_month = default_month

    month_map = {item["month"]: item for item in month_sections}
    selected_month_data = month_map.get(selected_month) or {
        "month": selected_month,
        "label": f"{selected_month}월",
        "enabled": False,
        "rows": [],
        "total_distance": 0,
    }
    enabled_months = [item["month"] for item in month_sections if item.get("enabled")]
    prev_month = next((month for month in reversed(enabled_months) if month < selected_month), None)
    next_month = next((month for month in enabled_months if month > selected_month), None)
    visible_months = []
    for month in range(selected_month - 2, selected_month + 3):
        if month < 1 or month > 12:
            continue
        month_info = month_map.get(month) or {}
        visible_months.append({
            "month": month,
            "label": f"{month}월",
            "enabled": bool(month_info.get("enabled")),
            "active": month == selected_month,
        })
    total_distance_all = 0
    for item in month_sections:
        if item["month"] <= today_value.month:
            total_distance_all += int(item.get("total_distance") or 0)

    return render_template(
        "vehicle_log.html",
        vehicles=vehicles,
        selected_car_id=selected_car_id,
        selected_vehicle=selected_vehicle,
        month_sections=month_sections,
        selected_month=selected_month,
        selected_month_data=selected_month_data,
        prev_month=prev_month,
        next_month=next_month,
        visible_months=visible_months,
        db_message=db_message,
        history_error=history_error,
        default_recipient_email=get_default_recipient_email(),
        total_distance=selected_month_data.get("total_distance", 0),
        total_distance_all=total_distance_all,
        main_driver=selected_vehicle.get("main_driver") or "",
        row_count=len(selected_month_data.get("rows", [])),
        db_path=get_vehicle_log_db_path(),
        db_status=db_status,
        vehicle_team_names=list(team_users_map.keys()),
        vehicle_team_users=team_users_map,
    )


@app.route("/", methods=["GET", "POST"])
def start():
    settings = load_settings()
    team_users = get_effective_team_users(settings)
    team_options = list(team_users.keys())

    if request.method == "POST":
        work_type = request.form.get("work_type", "visit").strip()
        user_name = request.form.get("user_name", "").strip()
        team_no = request.form.get("team_no", "").strip()
        trip_date = request.form.get("trip_date", "").strip()

        if work_type not in {"visit", "phone"}:
            return render_template(
                "start.html",
                work_type="",
                user_name="",
                team_no="",
                trip_date="",
                team_options=team_options,
                team_users=team_users
            )

        if work_type == "visit" and (
            not user_name
            or not team_no
            or not trip_date
            or not is_valid_team_user_selection(team_users, team_no, user_name)
        ):
            return render_template(
                "start.html",
                work_type=work_type,
                user_name="",
                team_no="",
                trip_date="",
                team_options=team_options,
                team_users=team_users
            )

        session["work_type"] = work_type
        session["user_name"] = user_name
        session["team_no"] = team_no
        session["trip_date"] = trip_date
        session.pop("last_result_payload", None)
        session.pop("last_grouping_payload", None)
        if work_type == "phone":
            return redirect(url_for("phone_menu"))
        return redirect(url_for("planner"))

    session.pop("work_type", None)
    session.pop("user_name", None)
    session.pop("team_no", None)
    session.pop("trip_date", None)
    session.pop("last_result_payload", None)
    session.pop("last_grouping_payload", None)

    return render_template(
        "start.html",
        work_type="",
        user_name="",
        team_no="",
        trip_date="",
        team_options=team_options,
        team_users=team_users
    )


@app.route("/reset")
def reset():
    session.clear()
    return redirect(url_for("start"))


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if is_mobile_request():
        return render_template("admin_login.html", mobile_blocked=True)

    if session.get("is_admin"):
        return redirect(url_for("admin_settings_page"))

    if request.method == "POST":
        password = (request.form.get("password") or "").strip()
        if password == get_admin_password():
            session["is_admin"] = True
            return redirect(url_for("admin_settings_page"))
        flash("비밀번호가 올바르지 않습니다.")
    return render_template("admin_login.html", mobile_blocked=False)


@app.route("/admin/logout")
def admin_logout():
    session.pop("is_admin", None)
    return redirect(url_for("start"))


@app.route("/admin/settings", methods=["GET"])
def admin_settings_page():
    if not session.get("is_admin"):
        return redirect(url_for("admin_login"))
    if is_mobile_request():
        return render_template("admin_login.html", mobile_blocked=True)
    settings = load_settings()
    vehicle_log_vehicles, _ = get_vehicle_log_vehicles()
    return render_template("admin_settings.html", settings=settings, vehicle_log_vehicles=vehicle_log_vehicles)


@app.route("/admin/settings/verify-password", methods=["POST"])
def verify_admin_password():
    if not session.get("is_admin"):
        return jsonify({"success": False, "message": "관리자 로그인이 필요합니다."}), 401

    if is_mobile_request():
        return jsonify({"success": False, "message": "모바일에서는 관리자 설정을 변경할 수 없습니다."}), 403

    current_password = (request.form.get("current_admin_password") or "").strip()
    if not current_password:
        return jsonify({"success": False, "message": "현재 관리자 비밀번호를 입력해 주세요."}), 400

    if current_password != get_admin_password():
        return jsonify({"success": False, "message": "현재 관리자 비밀번호가 올바르지 않습니다."}), 400

    return jsonify({"success": True, "message": "비밀번호가 확인되었습니다."})


@app.route("/admin/settings/search-parking-place", methods=["GET"])
def admin_search_parking_place():
    if not session.get("is_admin"):
        return jsonify({"success": False, "message": "관리자 로그인이 필요합니다."}), 401

    if is_mobile_request():
        return jsonify({"success": False, "message": "모바일에서는 관리자 설정을 변경할 수 없습니다."}), 403

    query = (request.args.get("query") or "").strip()
    if len(query) < 2:
        return jsonify({"success": False, "message": "검색어를 2자 이상 입력해 주세요."}), 400

    tmap_app_key = get_tmap_app_key()
    if not tmap_app_key:
        return jsonify({"success": False, "message": "API 설정에서 TMAP App Key를 먼저 입력해 주세요."}), 400

    try:
        response = requests.get(
            "https://apis.openapi.sk.com/tmap/pois",
            headers={
                "Accept": "application/json",
                "appKey": tmap_app_key,
            },
            params={
                "version": "1",
                "query": query,
                "searchKeyword": query,
                "searchType": "all",
                "page": 1,
                "count": 8,
                "resCoordType": "WGS84GEO",
                "reqCoordType": "WGS84GEO",
                "multiPoint": "N",
                "searchtypCd": "A",
                "poiGroupYn": "N",
            },
            timeout=10,
        )
    except Exception:
        return jsonify({"success": False, "message": "TMAP 장소 검색 API 호출 중 오류가 발생했습니다."}), 502

    if response.status_code != 200:
        detail = ""
        try:
            detail = (response.json() or {}).get("errorMessage") or ""
        except Exception:
            detail = ""
        return jsonify({"success": False, "message": f"TMAP 장소 검색 API 응답 오류입니다. {detail}".strip()}), 502

    try:
        payload = response.json() or {}
    except Exception:
        return jsonify({"success": False, "message": "TMAP 장소 검색 API 응답을 해석하지 못했습니다."}), 502

    poi_list = (((payload.get("searchPoiInfo") or {}).get("pois") or {}).get("poi") or [])
    if isinstance(poi_list, dict):
        poi_list = [poi_list]

    items = []
    for raw in poi_list:
        if not isinstance(raw, dict):
            continue

        name = str(raw.get("name") or "").strip()
        jibun_parts = [
            str(raw.get("upperAddrName") or "").strip(),
            str(raw.get("middleAddrName") or "").strip(),
            str(raw.get("lowerAddrName") or "").strip(),
        ]
        jibun_base = " ".join([x for x in jibun_parts if x])
        first_no = str(raw.get("firstNo") or "").strip()
        second_no = str(raw.get("secondNo") or "").strip()
        jibun_no = ""
        if first_no:
            # 부지번이 0이면 '-0'은 표기하지 않음 (예: 845-0 -> 845)
            jibun_no = first_no if (not second_no or second_no == "0") else f"{first_no}-{second_no}"
        jibun_address = " ".join([x for x in [jibun_base, jibun_no] if x]).strip()

        address = jibun_address
        if not name or not jibun_address:
            continue

        items.append({
            "name": name,
            "address": address,
            "road_address": "",
            "jibun_address": jibun_address,
            "display_address": jibun_address,
            "category": str(raw.get("upperBizName") or "").strip(),
        })

    return jsonify({"success": True, "items": items})


@app.route("/admin/settings/save-section", methods=["POST"])
def save_admin_settings_section():
    if not session.get("is_admin"):
        return jsonify({"success": False, "message": "관리자 로그인이 필요합니다."}), 401

    if is_mobile_request():
        return jsonify({"success": False, "message": "모바일에서는 관리자 설정을 변경할 수 없습니다."}), 403

    settings = load_settings()
    section = (request.form.get("section") or "").strip()

    try:
        if section == "mail":
            settings["mail"]["smtp_host"] = (request.form.get("smtp_host") or "smtp.gmail.com").strip() or "smtp.gmail.com"

            smtp_port_raw = (request.form.get("smtp_port") or "587").strip()
            try:
                settings["mail"]["smtp_port"] = int(smtp_port_raw)
            except Exception:
                return jsonify({"success": False, "message": "SMTP 포트는 숫자로 입력해 주세요."})

            settings["mail"]["smtp_user"] = (request.form.get("smtp_user") or "").strip()
            settings["mail"]["smtp_password"] = (request.form.get("smtp_password") or "").strip()
            settings["mail"]["mail_from"] = (request.form.get("mail_from") or "").strip()
            settings["mail"]["default_recipient_email"] = (request.form.get("default_recipient_email") or "").strip()
            settings["mail"]["email_subject_template"] = (request.form.get("email_subject_template") or DEFAULT_SETTINGS["mail"]["email_subject_template"]).strip()
            settings["mail"]["email_body_template"] = (request.form.get("email_body_template") or DEFAULT_SETTINGS["mail"]["email_body_template"]).strip()

        elif section == "api":
            settings["api"]["client_id"] = (request.form.get("client_id") or "").strip()
            settings["api"]["client_secret"] = (request.form.get("client_secret") or "").strip()
            settings["api"]["tmap_app_key"] = (request.form.get("tmap_app_key") or TMAP_DEFAULT_APP_KEY).strip()
            settings["api"]["kakao_rest_api_key"] = (request.form.get("kakao_rest_api_key") or "").strip()

        elif section == "user":
            start_name = (request.form.get("start_name") or "").strip()
            start_address = (request.form.get("start_address") or "").strip()
            return_name = (request.form.get("return_name") or "").strip()
            return_address = (request.form.get("return_address") or "").strip()
            return_same_as_start = (request.form.get("return_same_as_start") or "").strip() == "1"

            if len(start_name) > 10:
                return jsonify({"success": False, "message": "출발지명은 최대 10자까지 입력할 수 있습니다."})
            if len(return_name) > 10:
                return jsonify({"success": False, "message": "복귀지명은 최대 10자까지 입력할 수 있습니다."})

            settings["user"]["start_name"] = start_name
            settings["user"]["start_address"] = start_address or START_ADDRESS
            settings["user"]["return_same_as_start"] = return_same_as_start
            settings["user"]["return_name"] = settings["user"]["start_name"] if return_same_as_start else return_name
            settings["user"]["return_address"] = settings["user"]["start_address"] if return_same_as_start else (return_address or settings["user"]["start_address"])
            settings["user"]["enable_guest_user"] = (request.form.get("enable_guest_user") or "1") == "1"

            team_names = request.form.getlist("team_name")
            team_user_blocks = request.form.getlist("team_users_block")

            team_users = {}
            for idx, raw_name in enumerate(team_names):
                team_name = (raw_name or "").strip()
                if not team_name:
                    return jsonify({"success": False, "message": "조 이름은 필수 입력 항목입니다."})
                if team_name == GUEST_TEAM_NAME:
                    return jsonify({"success": False, "message": "게스트는 카드 목록에서 수정할 수 없습니다."})

                raw_users = team_user_blocks[idx] if idx < len(team_user_blocks) else ""
                users = [x.strip() for x in raw_users.splitlines() if x.strip() and x.strip() != GUEST_USER_NAME]
                if not users:
                    return jsonify({"success": False, "message": "사용자 목록은 필수 입력 항목입니다."})
                team_users[team_name] = users

            if not team_users:
                return jsonify({"success": False, "message": "최소 1개의 카드를 유지해 주세요."})

            settings["user"]["team_users"] = normalize_team_users(team_users)
            vehicle_log_car_ids = request.form.getlist("vehicle_log_car_id")
            vehicle_log_plate_numbers = request.form.getlist("vehicle_log_plate_number")
            vehicle_log_team_assignments = request.form.getlist("vehicle_log_team_assignment")
            vehicle_log_main_drivers = request.form.getlist("vehicle_log_main_driver")
            plate_map = {}
            team_assignment_map = {}
            main_driver_map = {}
            assigned_teams = set()
            for idx, raw_car_id in enumerate(vehicle_log_car_ids):
                car_id = str(raw_car_id or "").strip()
                if not car_id:
                    continue
                plate_number = str(vehicle_log_plate_numbers[idx] if idx < len(vehicle_log_plate_numbers) else "").strip()
                if plate_number:
                    plate_map[car_id] = plate_number
                team_name = str(vehicle_log_team_assignments[idx] if idx < len(vehicle_log_team_assignments) else "").strip()
                if team_name:
                    if team_name not in settings["user"]["team_users"]:
                        return jsonify({"success": False, "message": "배차에는 등록된 조만 선택할 수 있습니다."})
                    if team_name in assigned_teams:
                        return jsonify({"success": False, "message": "같은 조는 한 대의 차량에만 배차할 수 있습니다."})
                    assigned_teams.add(team_name)
                    team_assignment_map[car_id] = team_name
                main_driver = str(vehicle_log_main_drivers[idx] if idx < len(vehicle_log_main_drivers) else "").strip()
                if main_driver:
                    effective_team_name = team_name or str(team_assignment_map.get(car_id) or "").strip()
                    team_members = list(settings["user"]["team_users"].get(effective_team_name, [])) if effective_team_name else []
                    if team_members and main_driver not in team_members:
                        return jsonify({"success": False, "message": "주 운전자는 배차된 조 인원 중에서만 선택할 수 있습니다."})
                    main_driver_map[car_id] = main_driver
            settings["vehicle_log"]["plate_numbers"] = plate_map
            settings["vehicle_log"]["team_assignments"] = team_assignment_map
            settings["vehicle_log"]["main_drivers"] = main_driver_map

        elif section == "restaurant":
            names = request.form.getlist("restaurant_name")
            menus = request.form.getlist("restaurant_menu")
            prices = request.form.getlist("restaurant_price")
            addresses = request.form.getlist("restaurant_address")
            parkings = request.form.getlist("restaurant_parking")
            payment_cards = request.form.getlist("restaurant_payment_card")
            payment_cashes = request.form.getlist("restaurant_payment_cash")
            notes = request.form.getlist("restaurant_note")

            items = []
            for idx, raw_name in enumerate(names):
                name = (raw_name or "").strip()
                menu = (menus[idx] if idx < len(menus) else "").strip()
                price = (prices[idx] if idx < len(prices) else "").strip()
                address = (addresses[idx] if idx < len(addresses) else "").strip()
                raw_parking = (parkings[idx] if idx < len(parkings) else "unknown").strip()
                parking = raw_parking if raw_parking in {"1", "0", "unknown"} else "unknown"
                payment_card = (payment_cards[idx] if idx < len(payment_cards) else "0") == "1"
                payment_cash = (payment_cashes[idx] if idx < len(payment_cashes) else "0") == "1"
                note = (notes[idx] if idx < len(notes) else "").strip()

                if not any([name, menu, price, address]):
                    continue
                if not name or not menu or not price or not address:
                    return jsonify({"success": False, "message": "식당명, 메뉴, 가격, 주소는 모두 입력해 주세요."})

                items.append({
                    "name": name,
                    "menu": menu,
                    "price": price,
                    "address": address,
                    "parking": parking,
                    "payment_card": payment_card,
                    "payment_cash": payment_cash,
                    "note": note,
                })

            settings["restaurant"]["items"] = items

        elif section == "parking":
            names = request.form.getlist("parking_name")
            addresses = request.form.getlist("parking_address")

            items = []
            for idx, raw_name in enumerate(names):
                name = (raw_name or "").strip()
                address = (addresses[idx] if idx < len(addresses) else "").strip()

                if not any([name, address]):
                    continue
                if not name or not address:
                    return jsonify({"success": False, "message": "주차장명과 주소를 모두 입력해 주세요."})

                items.append({
                    "name": name,
                    "address": address,
                })

            settings["parking"]["items"] = items

        elif section == "admin":
            current_password = (request.form.get("current_admin_password") or "").strip()
            new_password = (request.form.get("new_admin_password") or "").strip()
            confirm_password = (request.form.get("confirm_admin_password") or "").strip()

            saved_password = get_admin_password()

            if not current_password:
                return jsonify({"success": False, "message": "현재 관리자 비밀번호를 입력해 주세요."})

            if current_password != saved_password:
                return jsonify({"success": False, "message": "현재 관리자 비밀번호가 올바르지 않습니다."})

            if not new_password:
                return jsonify({"success": False, "message": "새 관리자 비밀번호를 입력해 주세요."})

            if new_password != confirm_password:
                return jsonify({"success": False, "message": "새 관리자 비밀번호와 확인 값이 일치하지 않습니다."})

            settings["admin"]["admin_password"] = new_password

        else:
            return jsonify({"success": False, "message": "잘못된 설정 항목입니다."}), 400

        save_settings(settings)
        return jsonify({"success": True, "message": "변경사항이 저장되었습니다."})

    except Exception as e:
        return jsonify({"success": False, "message": f"저장 중 오류가 발생했습니다: {e}"}), 500


@app.route("/vehicle-log/save-vehicle-settings", methods=["POST"])
def vehicle_log_save_vehicle_settings():
    settings = load_settings()
    car_id = (request.form.get("car_id") or "").strip()
    try:
        payload = update_vehicle_log_setting(
            settings,
            car_id=car_id,
            plate_number=request.form.get("plate_number"),
            team_name=request.form.get("team_assignment"),
            main_driver=request.form.get("main_driver"),
        )
        save_settings(settings)
        return jsonify({"success": True, "message": "차량정보를 저장했습니다.", "data": payload})
    except Exception as exc:
        return jsonify({"success": False, "message": str(exc)}), 400


@app.route("/planner/resolve-qr-items", methods=["POST"])
def planner_resolve_qr_items():
    payload = request.get_json(silent=True) or {}
    items = payload.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify({"success": False, "message": "QR 항목이 없습니다."}), 400

    if not get_kakao_rest_api_key():
        return jsonify({"success": False, "message": "API 설정에서 Kakao REST API 키를 먼저 입력해 주세요."}), 400

    resolved_items = []
    for raw_item in items[:15]:
        if not isinstance(raw_item, dict):
            continue

        name = str(raw_item.get("name") or "").strip()
        address = str(raw_item.get("address") or "").strip()
        if not name and not address:
            continue

        resolved_address, err = resolve_kakao_address(address)
        err = err or "카카오 주소 검색에 실패했습니다."
        if resolved_address:
            resolved_items.append({
                "name": name,
                "address": "",
                "display_address": resolved_address,
                "ok": True,
                "message": "",
            })
        else:
            resolved_items.append({
                "name": name,
                "address": "",
                "display_address": "",
                "ok": False,
                "message": err or "주소 확인에 실패했습니다.",
            })

    return jsonify({"success": True, "items": resolved_items})


@app.route("/phone/menu", methods=["GET"])
def phone_menu():
    trip_meta = get_trip_meta()
    if trip_meta.get("work_type") != "phone":
        return redirect(url_for("start"))
    return render_template("phone_menu.html")


@app.route("/phone/grouping", methods=["GET", "POST"])
def phone_grouping():
    trip_meta = get_trip_meta()
    if trip_meta.get("work_type") != "phone":
        return redirect(url_for("start"))

    if request.method == "POST":
        names = request.form.getlist("name")
        addresses = request.form.getlist("address")
        preferred_size_raw = (request.form.get("group_size") or "10").strip()
        try:
            preferred_size = int(preferred_size_raw)
        except Exception:
            preferred_size = 10
        preferred_size = max(5, min(10, preferred_size))

        visits = []
        for idx in range(len(addresses)):
            name = names[idx].strip() if idx < len(names) else ""
            address = addresses[idx].strip() if idx < len(addresses) else ""
            if not name or not address:
                continue
            visits.append({
                "visit_id": idx,
                "name": name,
                "address": address,
            })

        if not visits:
            return render_template("phone_grouping.html", warning_message="최소 1개의 주소를 입력해 주세요.", group_size=preferred_size)

        if len(visits) > 40:
            return render_template("phone_grouping.html", warning_message="방문지 그룹화는 최대 40개까지 가능합니다.", group_size=preferred_size)

        start_coord, start_meta, start_err = geocode_with_meta(get_start_address())
        if not start_coord:
            return render_template("phone_grouping.html", warning_message=f"출발지 좌표를 불러오지 못했습니다. ({start_err})", group_size=preferred_size)

        start_display_address = (start_meta or {}).get("display_address") or get_start_address()
        _, return_meta, _ = geocode_with_meta(get_return_address())
        return_display_address = (return_meta or {}).get("display_address") or get_return_address()

        coords = [start_coord]
        failed_addresses = []
        for visit in visits:
            coord, meta, err = geocode_with_meta(visit["address"])
            coords.append(coord)
            if coord is None:
                failed_addresses.append(f"{visit['name']} / {visit['address']} / {err}")
            else:
                visit["display_address"] = (meta or {}).get("display_address") or visit["address"]

        if failed_addresses:
            return render_template(
                "phone_grouping.html",
                warning_message="일부 주소의 좌표를 불러오지 못했습니다. " + " | ".join(failed_addresses[:3]),
                group_size=preferred_size,
            )

        size = len(coords)
        dist_matrix = [[0] * size for _ in range(size)]
        for i in range(size):
            for j in range(i + 1, size):
                d, _ = estimate_matrix_leg(coords[i], coords[j])
                dist_matrix[i][j] = d
                dist_matrix[j][i] = d

        order = choose_shortest_distance_order(visits, dist_matrix)
        groups = build_phone_groups(
            visits,
            order,
            dist_matrix,
            preferred_size,
            start_display_address=start_display_address,
            return_display_address=return_display_address,
        )

        payload = {
            "groups": groups,
            "total_count": len(visits),
            "group_size": preferred_size,
        }
        session["last_grouping_payload"] = payload
        return redirect(url_for("phone_grouping_result"))

    return render_template("phone_grouping.html", warning_message="", group_size=10)


@app.route("/phone/grouping/result", methods=["GET"])
def phone_grouping_result():
    trip_meta = get_trip_meta()
    payload = session.get("last_grouping_payload")
    if trip_meta.get("work_type") != "phone" or not payload:
        return redirect(url_for("phone_grouping"))
    return render_template("phone_grouping_result.html", **payload)


@app.route("/planner", methods=["GET", "POST"])
def planner():
    trip_meta = get_trip_meta()

    work_type = trip_meta["work_type"] if trip_meta["work_type"] in {"visit", "phone"} else "visit"
    if work_type == "visit":
        if not trip_meta["user_name"] or not trip_meta["team_no"] or not trip_meta["trip_date"]:
            return redirect(url_for("start"))
    elif not trip_meta["work_type"]:
        return redirect(url_for("start"))

    if request.method == "POST":
        names = request.form.getlist("name")
        addresses = request.form.getlist("address")
        service_times_raw = request.form.getlist("service_time")
        has_appointment_flags = request.form.getlist("has_appointment_flag")
        visit_hours = request.form.getlist("visit_hour")
        visit_minutes = request.form.getlist("visit_minute")

        visits = []
        for i in range(len(addresses)):
            name = names[i].strip() if i < len(names) else ""
            address = addresses[i].strip() if i < len(addresses) else ""

            if not name or not address:
                continue

            try:
                default_service = 5 if work_type == "visit" else 0
                service_time = int(service_times_raw[i]) if i < len(service_times_raw) else default_service
            except Exception:
                service_time = 5 if work_type == "visit" else 0

            has_appt = (
                work_type == "visit"
                and i < len(has_appointment_flags)
                and str(has_appointment_flags[i]).strip() == "1"
            )

            appointment_minute = None
            if has_appt:
                hour = visit_hours[i] if i < len(visit_hours) else ""
                minute = visit_minutes[i] if i < len(visit_minutes) else ""
                appointment_minute = parse_appointment_minute(hour, minute)

            visits.append({
                "visit_id": i,
                "name": name,
                "address": address,
                "service_time": service_time,
                "has_appointment": has_appt,
                "appointment_minute": appointment_minute
            })

        if not visits:
            payload = {
                "route": [],
                "total_count": 0,
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": "",
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        start_coord, start_meta, start_err = geocode_with_meta(get_start_address())
        if not start_coord:
            payload = {
                "route": [],
                "total_count": 0,
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": f"출발지 좌표를 불러오지 못했습니다. ({start_err})",
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        start_display_address = (start_meta or {}).get("display_address") or get_start_address()
        _, return_meta, _ = geocode_with_meta(get_return_address())
        return_display_address = (return_meta or {}).get("display_address") or get_return_address()

        coords = [start_coord]
        failed_addresses = []

        for visit in visits:
            coord, meta, err = geocode_with_meta(visit["address"])
            coords.append(coord)
            if coord is None:
                failed_addresses.append(f"{visit['name']} / {visit['address']} / {err}")
            else:
                visit["display_address"] = (meta or {}).get("display_address") or visit["address"]

        if failed_addresses:
            payload = {
                "route": [],
                "total_count": len(visits),
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": "일부 주소의 좌표를 불러오지 못했습니다. " + " | ".join(failed_addresses[:3]),
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        size = len(coords)
        dist_matrix = [[0] * size for _ in range(size)]
        time_matrix = [[0] * size for _ in range(size)]

        for i in range(size):
            for j in range(i + 1, size):
                d, t = estimate_matrix_leg(coords[i], coords[j])
                dist_matrix[i][j] = d
                time_matrix[i][j] = t
                dist_matrix[j][i] = d
                time_matrix[j][i] = t

        if work_type == "phone":
            order = choose_shortest_distance_order(visits, dist_matrix)
            best = build_phone_route_result(
                order,
                visits,
                dist_matrix,
                start_display_address=start_display_address,
                return_display_address=return_display_address,
            )
        else:
            _, best = choose_best_schedule(
                visits,
                dist_matrix,
                time_matrix,
                start_display_address=start_display_address,
                return_display_address=return_display_address,
                coords=coords,
                trip_date=trip_meta["trip_date"],
            )

        if best is None:
            has_any_appointment = any(v.get("has_appointment") for v in visits)
            fail_message = (
                "지정한 약속시간을 모두 만족하는 경로를 찾지 못했습니다. 약속시간, 방문 순서, 방문 소요시간을 조정해 주세요."
                if has_any_appointment
                else "조건에 맞는 경로를 계산하지 못했습니다."
            )
            payload = {
                "route": [],
                "total_count": len(visits),
                "total_distance": "--",
                "total_time": "--",
                "end_time": "--:--",
                "warning_message": fail_message,
                "team_no": trip_meta["team_no"],
                "user_name": trip_meta["user_name"],
                "trip_date": trip_meta["trip_date"]
            }
            session["last_result_payload"] = payload
            return redirect(url_for("result_page"))

        warning_message = ""
        return_time_minute = int(best["return_time"])
        caution_start = 16 * 60 + 20
        caution_end = 16 * 60 + 30
        if return_time_minute >= caution_start:
            hh = return_time_minute // 60
            mm = return_time_minute % 60
            if return_time_minute <= caution_end:
                warning_message = (
                    f"예상 복귀시간이 {hh}시 {mm}분입니다.\n"
                    "복귀가 늦지 않도록 주의해 주세요."
                )
            else:
                warning_message = (
                    f"예상 복귀시간이 {hh}시 {mm}분입니다.\n"
                    "방문 일정을 조정하여 주세요."
                )

        try:
            parking_items = (load_settings().get("parking", {}) or {}).get("items", [])
            best["route_view"] = enrich_route_with_nearby_parking(
                best.get("route_view", []),
                visits,
                coords[1:],
                parking_items,
                radius_m=1000,
                max_items=5,
            )
        except Exception:
            app.logger.exception("Failed to enrich nearby parking data; serving route without parking info.")

        total_time_min = int(best["return_time"] - best.get("departure_time", DAY_START))
        total_distance_km = round(best["total_distance_m"] / 1000, 2)

        payload = {
            "route": best["route_view"],
            "total_count": len(visits),
            "total_distance": total_distance_km,
            "total_time": total_time_min,
            "end_time": minutes_to_str(best["return_time"]),
            "warning_message": warning_message,
            "team_no": trip_meta["team_no"],
            "user_name": trip_meta["user_name"],
            "trip_date": trip_meta["trip_date"]
        }
        session["last_result_payload"] = payload

        return redirect(url_for("result_page"))

    return render_template(
        "index.html",
        team_no=trip_meta["team_no"],
        user_name=trip_meta["user_name"],
        trip_date=trip_meta["trip_date"],
        work_type=work_type,
    )


@app.route("/result", methods=["GET"])
def result_page():
    trip_meta = get_trip_meta()
    payload = session.get("last_result_payload")
    work_type = trip_meta["work_type"] if trip_meta["work_type"] in {"visit", "phone"} else "visit"
    missing_visit_meta = not trip_meta["user_name"] or not trip_meta["team_no"] or not trip_meta["trip_date"]
    if ((work_type == "visit" and missing_visit_meta) or not payload):
        return redirect(url_for("start"))
    template_name = "result_phone.html" if work_type == "phone" else "result.html"
    return render_template(template_name, **payload, tmap_app_key=get_tmap_app_key(), force_mobile=False)


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True}), 200


@app.route("/vehicle-log/debug", methods=["GET"])
def vehicle_log_debug():
    db_path = get_vehicle_log_db_path()
    if not db_path or not os.path.exists(db_path):
        return jsonify({
            "ok": False,
            "error": get_vehicle_log_db_missing_message(),
            "db_path": db_path,
        }), 404

    selected_car_id = str(request.args.get("car_id") or "").strip()
    date_arg = str(request.args.get("date") or "").strip()
    try:
        target_date = date.fromisoformat(date_arg) if date_arg else date.today()
    except ValueError:
        target_date = date.today()
    target_date_text = target_date.isoformat()

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row

            if not selected_car_id:
                picked = conn.execute(
                    "SELECT car_id FROM vehicle_store ORDER BY created_at ASC, id ASC LIMIT 1"
                ).fetchone()
                selected_car_id = str(picked[0] or "").strip() if picked else ""

            overall_reports = conn.execute(
                "SELECT MIN(drive_date), MAX(drive_date), COUNT(*) FROM daily_reports"
            ).fetchone()
            overall_logs = conn.execute(
                "SELECT MIN(log_date), MAX(log_date), COUNT(*) FROM odometer_logs"
            ).fetchone()

            car_report_range = conn.execute(
                "SELECT MIN(drive_date), MAX(drive_date), COUNT(*) FROM daily_reports WHERE car_id = ?",
                (selected_car_id,),
            ).fetchone()
            car_log_range = conn.execute(
                "SELECT MIN(log_date), MAX(log_date), COUNT(*) FROM odometer_logs WHERE car_id = ?",
                (selected_car_id,),
            ).fetchone()
            token_row = conn.execute(
                "SELECT access_token, refresh_token, expires_at, updated_at FROM token_store ORDER BY id ASC LIMIT 1"
            ).fetchone()
            vehicle_count_row = conn.execute("SELECT COUNT(*) FROM vehicle_store").fetchone()

            report_row = conn.execute(
                """
                SELECT drive_date, start_time, end_time, odometer_start, odometer_end, distance_km
                FROM daily_reports
                WHERE car_id = ? AND drive_date = ?
                LIMIT 1
                """,
                (selected_car_id, target_date_text),
            ).fetchone()

            day_logs = conn.execute(
                """
                SELECT log_time, odometer_value, api_timestamp
                FROM odometer_logs
                WHERE car_id = ? AND log_date = ?
                ORDER BY log_time ASC
                """,
                (selected_car_id, target_date_text),
            ).fetchall()

            window_logs = []
            for row in day_logs:
                log_time_text = str(row["log_time"] or "").strip()
                try:
                    hour_value = int(log_time_text.split(":", 1)[0])
                except Exception:
                    continue
                if HYUNDAI_COLLECT_START_HOUR <= hour_value <= HYUNDAI_COLLECT_END_HOUR:
                    window_logs.append(row)

            derived = _derive_daily_report_fields_from_window(window_logs)

            latest_logs = conn.execute(
                """
                SELECT log_date, log_time, odometer_value
                FROM odometer_logs
                WHERE car_id = ?
                ORDER BY log_date DESC, log_time DESC
                LIMIT 20
                """,
                (selected_car_id,),
            ).fetchall()

            return jsonify({
                "ok": True,
                "db_path": db_path,
                "selected_car_id": selected_car_id,
                "target_date": target_date_text,
                "window_hours": [HYUNDAI_COLLECT_START_HOUR, HYUNDAI_COLLECT_END_HOUR],
                "overall": {
                    "report_min_date": str(overall_reports[0] or "") if overall_reports else "",
                    "report_max_date": str(overall_reports[1] or "") if overall_reports else "",
                    "report_count": int(overall_reports[2] or 0) if overall_reports else 0,
                    "log_min_date": str(overall_logs[0] or "") if overall_logs else "",
                    "log_max_date": str(overall_logs[1] or "") if overall_logs else "",
                    "log_count": int(overall_logs[2] or 0) if overall_logs else 0,
                },
                "car_range": {
                    "report_min_date": str(car_report_range[0] or "") if car_report_range else "",
                    "report_max_date": str(car_report_range[1] or "") if car_report_range else "",
                    "report_count": int(car_report_range[2] or 0) if car_report_range else 0,
                    "log_min_date": str(car_log_range[0] or "") if car_log_range else "",
                    "log_max_date": str(car_log_range[1] or "") if car_log_range else "",
                    "log_count": int(car_log_range[2] or 0) if car_log_range else 0,
                },
                "collector": {
                    "enabled": ENABLE_HYUNDAI_MILEAGE_COLLECTOR,
                    "interval_seconds": HYUNDAI_COLLECT_INTERVAL_SECONDS,
                    "window_hours": [HYUNDAI_COLLECT_START_HOUR, HYUNDAI_COLLECT_END_HOUR],
                    "auth_base_set": bool(HYUNDAI_AUTH_BASE),
                    "data_base_set": bool(HYUNDAI_DATA_BASE),
                    "client_id_set": bool(HYUNDAI_CLIENT_ID),
                    "client_secret_set": bool(HYUNDAI_CLIENT_SECRET),
                },
                "token_store": {
                    "has_row": bool(token_row),
                    "has_access_token": bool(str(token_row[0] or "").strip()) if token_row else False,
                    "has_refresh_token": bool(str(token_row[1] or "").strip()) if token_row else False,
                    "expires_at": str(token_row[2] or "") if token_row else "",
                    "updated_at": str(token_row[3] or "") if token_row else "",
                },
                "vehicle_store_count": int(vehicle_count_row[0] or 0) if vehicle_count_row else 0,
                "daily_report_row": dict(report_row) if report_row else None,
                "target_day_log_count": len(day_logs),
                "target_day_window_log_count": len(window_logs),
                "target_day_window_derived": derived,
                "latest_logs": [dict(row) for row in latest_logs],
            })
    except Exception as exc:
        app.logger.exception("Failed to build vehicle-log debug payload.")
        return jsonify({
            "ok": False,
            "error": str(exc),
            "db_path": db_path,
            "selected_car_id": selected_car_id,
            "target_date": target_date_text,
        }), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
