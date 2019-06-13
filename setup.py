import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="freactor",
    version="0.0.3",
    author="Brooke Yang",
    author_email="brookeyang@vip.qq.com",
    description="lightweight flow-control framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Pro-YY/freactor",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
        "Operating System :: POSIX :: Linux",
        "Topic :: Software Development :: Libraries :: Application Frameworks",
    ],
)
