# Shell to use with Make
SHELL := /bin/bash

# Set important Paths
PROJECT := btrdbextras
LOCALPATH := $(CURDIR)/$(PROJECT)

# Sphinx configuration
SPHINXOPTS    	=
SPHINXBUILD   	= sphinx-build
SPHINXBUILDDIR  = docs/build
SPHINXSOURCEDIR = docs/source

# Export targets not associated with files
.PHONY: test grpc

# Clean build files
clean:
	find . -name "*.pyc" -print0 | xargs -0 rm -rf
	find . -name "__pycache__" -print0 | xargs -0 rm -rf
	find . -name ".DS_Store" -print0 | xargs -0 rm -rf
	-rm -rf docs/build
	-rm -rf htmlcov
	-rm -rf .pytest_cache
	-rm -rf .coverage
	-rm -rf build
	-rm -rf dist
	-rm -rf $(PROJECT).egg-info
	-rm -rf .eggs
	-rm -rf site
	-rm -rf docs/build
	-rm -rf platform-builds meta.yaml

# Generate new grpc code
grpc:
	@echo Generating files:
	python -m grpc_tools.protoc -I btrdbextras/eventproc/protobuff --python_out=btrdbextras/eventproc/protobuff --grpc_python_out=btrdbextras/eventproc/protobuff btrdbextras/eventproc/protobuff/api.proto
	@echo
	@echo Fixing import statements:
	sed -i'.bak' 's/api_pb2 as api__pb2/btrdbextras.eventproc.protobuff.api_pb2 as api__pb2/' btrdbextras/eventproc/protobuff/api_pb2_grpc.py


# Targets for testing
test:
	python setup.py test

# Build the universal wheel and source distribution
build:
	python setup.py sdist bdist_wheel

# Install the package from source
install:
	python setup.py install

# Deploy to PyPI
deploy:
	# python setup.py register
	twine upload dist/* --verbose

# Build html version of docs
html:
	$(SPHINXBUILD) -b html $(SPHINXOPTS) $(SPHINXSOURCEDIR) $(SPHINXBUILDDIR)