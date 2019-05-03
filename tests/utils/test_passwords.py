from spinta.utils import passwords


def test_passwords():
    secret = 'joWgziYLap3eKDL6Gk2SnkJoyz0F8ukB'
    hash = passwords.crypt(secret, iterations=2, salt='salt')
    assert hash == 'pbkdf2$sha256$2$c2FsdA$dQ6drDKRG9hThvJNXG6gp-iSxpt0AIDju2Xy_2A-Ezc'
    assert passwords.verify(secret, hash)


def test_gesecret(mocker):
    mocker.patch('os.urandom', lambda n: b'\x0F' * n)
    assert passwords.gensecret(9) == 'Dw8PDw8PD'
