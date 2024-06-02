import csv
from datetime import date
from typing import Any, Callable, Dict, Sequence


def to_title(name: str) -> str:
    name = name.strip()
    return name.title() if name.islower() or name.isupper() else name


TS_KEYS = {
    'Last': to_title,
    'First': to_title,
    'Birthdate': lambda v: date.fromisoformat(v) if v else date(1900, 1, 1),
    'Gender': to_title,

}

USATF_KEYS = {
    'Last Name': to_title,
    'First Name': to_title,
    'Date of Birth': date.fromisoformat,
    'Sex': to_title,
    'Individual Membership Status': lambda v: v.strip() == 'Current',
    'Individual Membership Memb No.': lambda v: int(v.strip()) if v else None,
    'Date of Birth Verification Status': lambda v: v.strip() == 'Current'
}


KEY_MAP = {
    'Last': 'last',
    'Last Name': 'last',
    'First': 'first',
    'First Name': 'first',
    'Birthdate': 'dob',
    'Date of Birth': 'dob',
    'Gender': 'gender',
    'Sex': 'gender',
    'Individual Membership Status': 'valid',
    'Individual Membership Memb No.': 'usatf_id',
    'Date of Birth Verification Status': 'age_verified'
}


def prepare_record(raw_data: Dict[str, str], norm_dict: Dict[str, Callable]) -> Dict[str, Any]:
    return {KEY_MAP[k]: f(raw_data[k]) for k, f in norm_dict.items()}


def read_csv_data(filename: str, norm_dict: Dict[str, Callable]) -> Sequence[Dict[str, Any]]:
    with open(filename, 'rt') as csv_file:
        reader = csv.DictReader(csv_file)
        return [prepare_record(r, norm_dict) for r in reader]
