// Copyright 2011 by Christoph Hack. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package godbm

import (
	"testing"
)

func TestSetAndGet(t *testing.T) {
	db, err := Create("test.db", 5)
	if err != nil {
		t.Fatalf("%s\n", err)
	}
	data := map[string]string{
		"foo":       "bar",
		"key":       "value",
		"user/1102": "id=1102&username=steve&data=3",
	}

	for k, v := range data {
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			t.Fatalf("%s\n", err)
		}
	}

	for k, v := range data {
		if data, err := db.Get([]byte(k)); err != nil {
			t.Fatalf("%s\n", err)
		} else if string(data) != v {
			t.Fatalf("%s != %s\n", string(data), v)
		}
	}
}
