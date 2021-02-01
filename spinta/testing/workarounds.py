from spinta.core.config import RawConfig


def fix_s3_backend_issue(rc: RawConfig) -> RawConfig:
    # S3 backend is hardcoded globally and requires a fixture.
    # This rc override deletes S3 backend from config.
    return rc.fork({
        'backends': {
            'default': {
                'type': 'memory',
            }
        },
    })
