# Copyright 2011 by Christoph Hack. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=godbm

GOFILES=\
	godbm.go\

include $(GOROOT)/src/Make.pkg

format:
	gofmt -w godbm.go
	gofmt -w godbm_test.go
