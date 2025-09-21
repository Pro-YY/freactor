import re
import setuptools


# read __version__ from __init__
with open("freactor/__init__.py", "r") as f:
    content = f.read()
    version = re.search(r'__version__ = "([^"]+)"', content).group(1)


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


setuptools.setup(
    name="freactor",
    version=version,
    author="Brooke Yang",
    author_email="brookeyang@vip.qq.com",
    description="lightweight flow-control framework (sync + async support)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Pro-YY/freactor",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
        "Framework :: AsyncIO",
    ],
    python_requires=">=3.8",
)
