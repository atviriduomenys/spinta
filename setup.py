from pathlib import Path
from setuptools import setup, find_packages


def read_requirements(filename):
    with open(filename) as f:
        return [req for req in (req.partition('#')[0].strip() for req in f) if req]


setup(
    name='spinta',
    description='Data store.',
    long_description=Path('README.rst').read_text(),
    url='https://gitlab.com/atviriduomenys/spinta',
    author='Mantas Zimnickas',
    author_email='sirexas@gmail.com',
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    license='MIT',
    packages=find_packages(),
    package_data={'spinta': ['manifest/*.yml', 'templates/*.html']},
    install_requires=read_requirements('requirements.in'),
    entry_points={
        'console_scripts': [
            'spinta = spinta.cli:main',
        ]
    },
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.7',
        'Topic :: Database',
        'Topic :: Database :: Database Engines/Servers',
        'Topic :: Database :: Front-Ends',
    ],
)
