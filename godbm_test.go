// Copyright 2011 by Christoph Hack. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package godbm

import (
	"testing"
	"fmt"
)

// Simple testcase for setting and getting some values
func TestSetAndGet(t *testing.T) {
	db, err := Create("test.db", 6)
	if err != nil {
		t.Fatalf("%s\n", err)
	}
	data := map[string]string{
		"foo":       "bar",
		"key":       "value",
		"user/1102": "id=1102&username=steve&data=3",
		"empty":     "",
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
			t.Fatalf("\"%s\" != \"%s\"\n", string(data), v)
		}
	}
}

// This testcase stores 2^4 values in a hash map with 2^2 slots to
// test the collision handling.
func TestCollision(t *testing.T) {
	db, err := Create("test.db", 2)
	if err != nil {
		t.Fatalf("%s\n", err)
	}
	for i := 0; i < 1<<4; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		val := []byte(fmt.Sprintf("value%02d", i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("%s\n", err)
		}
	}
	for i := 0; i < 1<<4; i++ {
		key := []byte(fmt.Sprintf("key%02d", i))
		val := []byte(fmt.Sprintf("value%02d", i))
		if data, err := db.Get(key); err != nil {
			t.Fatalf("%s\n", err)
		} else if string(data) != string(val) {
			t.Fatalf("\"%s\" != \"%s\"\n", string(data), string(val))
		}
	}
}
