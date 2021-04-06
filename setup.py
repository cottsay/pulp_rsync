#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name="pulp-rsync",
    version="0.0.1",
    description="Sync Pulp distribution content using rsync",
    license="GPLv2+",
    author="Scott K Logan",
    author_email="logans@cottsay.net",
    url="https://github.com/cottsay/pulp_rsync",
    python_requires=">=3.7",
    install_requires=[
        "Django~=2.2.19",
        "pulpcore>=3.8",
    ],
    include_package_data=True,
    packages=find_packages(),
    classifiers=(
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        "Operating System :: POSIX :: Linux",
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ),
    entry_points={
        "console_scripts": [
            "pulp-rsync = pulp_rsync.app.__main__:main",
        ],
        "pulpcore.plugin": [
            "pulp_rsync = pulp_rsync:default_app_config",
        ],
    },
)
