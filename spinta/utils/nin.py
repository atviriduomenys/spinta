"""National identification number."""

import datetime
import random


def _get_nin_lt_checksum(code: str) -> int:
    numbers = list(map(int, code))
    factors = [1, 2, 3, 4, 5, 6, 7, 8, 9, 1]
    checksum = sum(x * i for x, i in zip(numbers, factors)) % 11
    if checksum == 10:
        factors = [3, 4, 5, 6, 7, 8, 9, 1, 2, 3]
        checksum = sum(x * i for x, i in zip(numbers, factors)) % 10
    return checksum


def generate_person_code(
    *,
    dob: datetime.datetime = None,
    gender: int = None,  # 0 - male, 1 - female
    num: int = None,  # between 1 and 999, including both ends
) -> str:
    if dob is None:
        now = datetime.datetime.now()
        dob = random.randint(
            int((now - datetime.timedelta(days=365) * 105).timestamp()),
            int(now.timestamp()),
        )
        dob = datetime.date.fromtimestamp(dob)

    if dob.year < 1801:
        raise ValueError("dob year must be at least 1801")

    if gender is None:
        gender = random.randint(0, 1)
    century = dob.year // 100 - 18
    cender = century * 2 + gender + 1

    if num is None:
        num = random.randint(1, 999)

    code = f'{cender}{dob:%y%m%d}{num:03}'
    checksum = _get_nin_lt_checksum(code)

    return f'{code}{checksum}'


def is_nin_lt(code: str) -> bool:
    """Check if given code is a Lithuanian national identification number."""
    code = str(code)

    if len(code) != 11:
        return False

    if not code.isdigit():
        return False

    if code[0] == '9':
        # This is an exceptional case and might be a valid person code.
        return True

    year = int(code[1:3])
    month = int(code[3:5])
    day = int(code[5:7])
    if year and month and day:
        # year, month ant day can be 0 if person does not remember they birth
        # date.
        century, gender = divmod(int(code[0]), 2)
        year = (17 + century) * 100 + year
        try:
            dob = datetime.datetime(year, month, day)
        except ValueError:
            # Invalid birth date.
            return False

        if dob > datetime.datetime.now():
            # Person can not be born in future.
            return False

    if int(code[-1]) != _get_nin_lt_checksum(code[:-1]):
        # Invalid checksum.
        return False

    return True

