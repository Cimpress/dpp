from setuptools import setup, find_packages


setup(
    name="dpp",
    url="https://github.com/Cimpress/dpp",
    author="Clockwork Squad",
    version="0.1",
    packages=find_packages(
        exclude=["*.tests", "*.tests.*", "tests.*", "tests"]
    ),
    install_requires=["requests>=2.23.0", "pycryptodomex", "boto3"],
    extras_require={"dev": ["pytest", "pytest-pep8", "pytest-cov"]},
    setup_requires=["wheel"],
)
