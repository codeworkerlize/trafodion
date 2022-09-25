# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2019 Esgyn Corporation
#
# @@@ END COPYRIGHT @@@
include $(TRAF_HOME)/macros.gmk

GO ?= go
TOOLSDIR ?= /opt/home/tools
TARGET_DIR=$(BINEXPDIR)
FLAGS = -ldflags -w -o $(TARGET_DIR)/mds -mod=vendor
VENDOR_DIR = $(TOOLSDIR)/mds/vendor
all: current 

pre:
	@if [ -d vendor ]; then \
        echo "Using local vendor dir"; \
	elif [ -d $(VENDOR_DIR) ]; then \
        echo "Copying vendor dir from TOOLSDIR"; \
        cp -rf $(VENDOR_DIR) . ; \
    else \
        echo "Using go mod to download vendor dependencies ..."; \
        GOPROXY=https://goproxy.io $(GO) mod vendor; \
    fi

# compile using current runtime env
current: pre
	$(GO) build $(FLAGS)

# cross compile MacOS x86 version
darwin-x86: pre
	GOOS=darwin GOARCH=amd64 $(GO) build $(FLAGS)

# cross compile Linux x86 version
linux-x86: pre
	GOOS=linux GOARCH=amd64	$(GO) build $(FLAGS)

# cross compile Linux ARM version
linux-arm: pre
	GOOS=linux GOARCH=arm64	$(GO) build $(FLAGS)
clean:
	rm -rf $(TARGET_DIR)/mds 
	rm -rf ./vendor/
