SHELL := /bin/bash
RELEASE_VER ?= ${ESGYN_PRODUCT_VER}
RELEASE_TYPE = QianBase

ADDON_INSTALLER_TARNAME = $(shell echo ${RELEASE_TYPE}_addon_installer-${RELEASE_VER}-${OS_TYPE}${OS_MAJOR}.tar.gz)

all: pkg-addon-installer

create-dir:
	mkdir -p ../../distribution

pkg-addon-installer: create-dir
	tar czf ../../distribution/${ADDON_INSTALLER_TARNAME} ../addon-installer
