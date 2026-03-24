from typing import Any

import pymysql

from boxer.core import settings as s


def _create_db_connection(timeout_sec: int | None = None) -> Any:
    actual_timeout = max(1, timeout_sec if timeout_sec is not None else s.DB_QUERY_TIMEOUT_SEC)
    connection = pymysql.connect(
        host=s.DB_HOST,
        port=s.DB_PORT,
        user=s.DB_USERNAME,
        password=s.DB_PASSWORD,
        database=s.DB_DATABASE,
        connect_timeout=actual_timeout,
        read_timeout=actual_timeout,
        write_timeout=actual_timeout,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with connection.cursor() as cursor:
            # 앱 레벨 2차 안전장치: 세션 기본 트랜잭션을 read-only로 강제
            cursor.execute("SET SESSION TRANSACTION READ ONLY")
    except pymysql.MySQLError as exc:
        connection.close()
        raise RuntimeError("DB 세션을 read-only로 설정하지 못했어") from exc
    return connection


def _validate_readonly_sql(raw_sql: str) -> str:
    sql = (raw_sql or "").strip()
    if not sql:
        return s.DEFAULT_DB_QUERY

    if len(sql) > max(1, s.DB_QUERY_MAX_SQL_CHARS):
        raise ValueError(f"SQL 길이는 최대 {s.DB_QUERY_MAX_SQL_CHARS}자까지 허용해")

    if sql.endswith(";"):
        sql = sql[:-1].strip()
    if ";" in sql:
        raise ValueError("한 번에 한 쿼리만 실행할 수 있어")

    # 주석 문법은 우회 경로가 될 수 있어 차단
    if any(token in sql for token in ("--", "/*", "*/", "#")):
        raise ValueError("SQL 주석 문법은 허용하지 않아")

    lowered = sql.lower()
    if not s.DB_READONLY_SQL_HEAD_PATTERN.match(lowered):
        raise ValueError("읽기 전용 쿼리(SELECT/SHOW/DESCRIBE/EXPLAIN/WITH)만 허용해")
    if s.DB_FORBIDDEN_SQL_PATTERN.search(lowered):
        raise ValueError("쓰기/변경 쿼리는 허용하지 않아")
    if s.DB_FORBIDDEN_SQL_FRAGMENT_PATTERN.search(lowered):
        raise ValueError("파일 입출력/적재 쿼리는 허용하지 않아")
    if s.DB_LOCKING_READ_PATTERN.search(lowered):
        raise ValueError("잠금 조회(SELECT ... FOR UPDATE)는 허용하지 않아")

    return sql


def _query_db(sql: str) -> dict[str, Any]:
    rows_limit = max(1, min(200, s.DB_QUERY_MAX_ROWS))
    timeout_sec = max(1, s.DB_QUERY_TIMEOUT_SEC)
    connection = _create_db_connection(timeout_sec)

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchmany(rows_limit)
            rowcount = cursor.rowcount
    finally:
        connection.close()

    return {
        "rows": rows,
        "rowcount": rowcount,
    }
