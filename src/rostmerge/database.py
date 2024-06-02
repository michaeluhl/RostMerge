import sqlite3
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Any

ROSTER_SCHEMA = """
CREATE TABLE IF NOT EXISTS roster (
    last TEXT,
    first TEXT,
    dob DATE,
    gender TEXT,
    usatf_id INTEGER DEFAULT NULL,
    is_coach BOOLEAN DEFAULT FALSE,
    update_time TIMESTAMP,
    UNIQUE(last, first)
);
"""


USATF_SCHEMA = """
CREATE TABLE IF NOT EXISTS usatf (
    usatf_id INTEGER UNIQUE NOT NULL,
    last TEXT,
    first TEXT,
    dob DATE,
    gender TEXT,
    valid BOOLEAN DEFAULT FALSE,
    age_verified BOOLEAN DEFAULT FALSE,
    update_time TIMESTAMP
)
"""


class MatchResults:

    def __init__(self,
                 exact: Sequence[Sequence[Any]],
                 partial: Sequence[Sequence[Any]]) -> None:
        self.exact = exact
        self.partial = partial


# Adapt datetime.date to ISO 8601 date.
sqlite3.register_adapter(date, lambda v: v.isoformat())
# Adapt datetime.datetime to Unix timestamp.
sqlite3.register_adapter(datetime, lambda v: int(v.timestamp()))
# Convert ISO 8601 date to datetime.date object.
sqlite3.register_converter('date', lambda v: date.fromisoformat(v.decode()))
# Convert Unix epoch timestamp to datetime.datetime object.
sqlite3.register_converter('timestamp', lambda v: datetime.fromtimestamp(int(v), tz=UTC))


def prepare_db(filename: str) -> sqlite3.Connection:
    db = sqlite3.connect(
        filename,
        detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES
    )
    with db:
        db.executescript(ROSTER_SCHEMA)
        db.executescript(USATF_SCHEMA)
    return db


def get_joint_runner_data(db: sqlite3.Connection) -> Sequence[Sequence[Any]]:
    return db.execute('SELECT t.last, t.first, t.dob, u.valid, t.usatf_id, u.age_verified, '
                      't.last = u.last, t.first = u.first, t.dob = u.dob, t.gender = u.gender '
                      'FROM roster t LEFT JOIN usatf u '
                      'ON t.usatf_id = u.usatf_id '
                      'WHERE t.is_coach = FALSE '
                      'ORDER BY t.last, t.first').fetchall()


def ts_clear_coaches(db: sqlite3.Connection) -> None:
    with db:
        db.execute('UPDATE roster SET is_coach = FALSE')


def ts_clear_matches(db: sqlite3.Connection) -> None:
    with db:
        db.execute('UPDATE roster '
                   'SET usatf_id = NULL')


def ts_delete_missing(db: sqlite3.Connection,
                      rows: Sequence[Any]) -> None:
    with db:
        db.executemany(
            'DELETE FROM roster '
            'WHERE last = ? AND first = ?',
            [(r[0], r[1]) for r in rows]
        )


def ts_get_roster(db: sqlite3.Connection,
                  *,
                  exclude_coaches: bool,
                  exclude_matched: bool,
                  ordered: bool=True) -> Sequence[Any]:
    criteria = []
    if exclude_coaches:
        criteria.append('is_coach = FALSE')
    if exclude_matched:
        criteria.append('usatf_id IS NULL')
    where_clause = ' WHERE ' + ' AND '.join(criteria) if criteria else ''
    order_clause = ' ORDER BY last, first' if ordered else ''
    print('SELECT * '
          'FROM roster' +
          where_clause +
          order_clause)
    return db.execute('SELECT * '
                      'FROM roster' +
                      where_clause +
                      order_clause).fetchall()


def ts_insert_data(db: sqlite3.Connection,
                   rows: Sequence[dict[str, Any]],
                   *,
                   return_missing_records: bool,
                   timestamp: datetime | None = None
                   ) -> Sequence[str] | None:
    timestamp = timestamp if timestamp else datetime.now(tz=UTC)
    for r in rows:
        r['update_time'] = timestamp

    with db:
        db.executemany(
            'INSERT OR REPLACE INTO roster(last, first, dob, gender, update_time) '
            'VALUES (:last, :first, :dob, :gender, :update_time)',
            rows
        )

    if return_missing_records:
        with db:
            db.executescript(
                'CREATE TEMPORARY TABLE namecheck (last TEXT, first TEXT, UNIQUE(last, first))'
            )
            db.executemany(
                'INSERT INTO namecheck(last, first) VALUES (:last, :first)',
                rows
            )
        missing = db.execute(
            'SELECT r.last, r.first '
            'FROM roster r '
            'LEFT JOIN namecheck n '
            'ON r.last = n.last AND r.first = n.first '
            'WHERE n.last IS NULL AND n.first IS NULL'
        ).fetchall()
        return missing
    return None


def ts_set_coach(db: sqlite3.Connection,
                 last: str,
                 first: str,
                 *,
                 is_coach: bool) -> None:
    with db:
        db.execute('UPDATE roster '
                   'SET is_coach = ? '
                   'WHERE last = ? AND first = ?',
                   (is_coach, last, first))


def ts_set_usatf_id(db: sqlite3.Connection,
                    last: str,
                    first: str,
                    usatf_id: int) -> None:
    with db:
        db.execute('UPDATE roster '
                   'SET usatf_id = ? '
                   'WHERE last = ? AND first = ?',
                   (usatf_id, last, first))


def usatf_insert_data(db: sqlite3.Connection,
                      rows: Sequence[dict[str, Any]],
                      timestamp: datetime | None
                      ) -> None:
    timestamp = timestamp if timestamp else datetime.now(tz=UTC)
    for r in rows:
        r['update_time'] = timestamp

    with db:
        db.executemany(
            'INSERT OR REPLACE INTO usatf'
            '(usatf_id, last, first, dob, gender, valid, age_verified, update_time) '
            'VALUES (:usatf_id, :last, :first, :dob, :gender, :valid, :age_verified, :update_time)',
            rows
        )


def usatf_find_match(db: sqlite3.Connection,
                     runner: Sequence[Any]) -> MatchResults:
    last, first, dob, gender, *_ = runner
    exact = db.execute('SELECT * '
                       'FROM usatf '
                       'WHERE last = ? AND first = ? AND dob = ? AND gender = ?',
                       (last, first, dob, gender)).fetchall()
    partial = set()
    partial.update(tuple(r) for r in db.execute('SELECT * FROM usatf WHERE last = ?', (last, )))
    partial.update(tuple(r) for r in db.execute('SELECT * FROM usatf WHERE first = ?', (first, )))
    partial.update(tuple(r) for r in db.execute('SELECT * FROM usatf WHERE dob = ?', (dob, )))
    return MatchResults(exact, list(partial))
