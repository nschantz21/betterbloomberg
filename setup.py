import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="betterbloomberg",
    version="0.0.5",
    description="A simple wrapper for the Bloomberg Python API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nschantz21/betterbloomberg",
    author="Nick Schantz",
    author_email="nschantz21@gmail.com",
    license="GNU",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    python_requires=">=3.7",
)
