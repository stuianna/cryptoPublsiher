import setuptools
exec(open('cryptopublisher/_version.py').read())

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cryptopublisher",
    scripts=['bin/crypto_rawPricePublisher'],
    version=__version__,
    author="stuianna",
    author_email="stuian@protonmail.com",
    description="Publisher for multiple cryptocurrency datasources",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/stuianna/cryptoPublisher",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3", "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3.6',
    install_requires=[
        'db-ops',
        'pandas',
        'numpy',
        'appdirs',
        'config-checker',
        'CMCLogger'
    ],
)
