# Overview

This repository contains features and functionality related to the BTrDB and the PingThings platform - it exists to keep the bindings library clean, organized, and heavily focused on core database interactions.  As such this package will contain Python code to interact with other PingThings platform features, contain industry specific code, etc.

## Installation

To quickly get started using the latest available versions you can use `pip` to install from PyPi.

    $ pip install btrdbextras


## Tests

This project includes a suite of automated tests based upon [pytest](https://docs.pytest.org/en/latest/).  For your convenience, a `Makefile` has been provided with a target for evaluating the test suite.  Use the following command to run the tests.

    $ make test

Aside from basic unit tests, the test suite is configured to use [pytest-flake8](https://github.com/tholo/pytest-flake8) for linting and style checking as well as [coverage](https://coverage.readthedocs.io) for measuring test coverage.

Note that the test suite has additional dependencies that must be installed for them to successfully run: `pip install -r tests/requirements.txt`.

## Releases

This codebase uses github actions to control the release process.  To create a new release of the software, run `release.sh` with arguments for the new version as shown below.  Make sure you are in the master branch when running this script.

```
./release.sh 5 11 4
```

This will tag and push the current commit and github actions will run the test suite, build the package, and push it to [PyPi](https://pypi.org/project/btrdbextras/).  If any issues are encountered with the automated tests, the build will fail and you will have a tag with no corresponding release.

After a release is created, you can manually edit the release description through github.

## Documentation

The project documentation is written in reStructuredText and is built using Sphinx, which also includes the docstring documentation from the `btrdb` Python package. For your convenience, the `Makefile` includes a target for building the documentation:

    $ make html

This will build the HTML documentation locally in `docs/build`, which can be viewed using `open docs/build/index.html`. Other formats (PDF, epub, etc) can be built using `docs/Makefile`. The documentation is automatically built on every GitHub release and hosted on [Read The Docs](https://btrdbextras.readthedocs.io/en/latest/).

Note that the documentation also requires Sphix and other dependencies to successfully build: `pip install -r docs/requirements.txt`.

## Versioning

This codebases uses a form of [Semantic Versioning](http://semver.org/) to structure version numbers.  In general, the major version number will track with the BTrDB codebase to transparently maintain version compatibility.  Planned features between major versions will increment the minor version while any special releases (bug fixes, etc.) will increment the patch number.