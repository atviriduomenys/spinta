import base64
import hashlib
import hmac
import math
import os
import random


def _bencode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip('=')


def _bdecode(value: str) -> bytes:
    value = value.ljust(math.ceil(len(value) / 4) * 4, '=')
    return base64.urlsafe_b64decode(value)


def crypt(password, iterations=None, salt=None):
    if iterations is None:
        iterations = random.randint(100_000, 999_999)
    if salt is None:
        salt = os.urandom(16)
    elif isinstance(salt, str):
        salt = salt.encode()
    if isinstance(password, str):
        password = password.encode()
    hash = hashlib.pbkdf2_hmac('sha256', password, salt, iterations)
    hash = _bencode(hash)
    salt = _bencode(salt)
    return f'pbkdf2$sha256${iterations}${salt}${hash}'


def verify(password, hash):
    if isinstance(password, str):
        password = password.encode()

    hasher, *args = hash.split('$')
    if hasher == 'pbkdf2':
        hash_name, iterations, salt, hash = args
        iterations = int(iterations)
        salt = _bdecode(salt)
        given = _bdecode(hash)
        repro = hashlib.pbkdf2_hmac(hash_name, password, salt, iterations)
    else:
        raise Exception(f"Unknown hasher {hasher!r}.")

    return hmac.compare_digest(given, repro)


def gensecret(size=32):
    n = math.floor(math.log(64, 256) * size) + 1
    return _bencode(os.urandom(n))[:size]
