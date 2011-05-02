// Copyright 2011 by Christoph Hack. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
The godbm package provides a native DBM like database similar to Berkley DB,
QDBM or Kyota Cabinet.

This lightweight embedded database can only used by one process at a time, but
that's not a problem because Go programs are quite good a concurrency and if
you want to access the database from different hosts, you can provide a service
using the RPC package. The advantage of this approach is that you are not bound
to an external DBMS and that the simple DBM provided by this package is
extremely fast.

Therefore godbm is the ideal solution if you want to build things like a
persistent cache, a session store or when you need to find a way to persist
mails in your own MDA!

Attention: The godbm package is currently work in progress and the file format
is likely to change in further versions. So do not use it for sensitive data
yet!
*/

package godbm

import (
	"os"
	"encoding/binary"
	"crypto/md5"
	"sync"
)

// The HashDB provides a persistent hash table with the usual O(1)
// characteristics.
type HashDB struct {
	nbuckets uint32   // 2^nbuckets buckets
	buckets  []int64  // bucket array
	file     *os.File // associated file
	mu       sync.RWMutex
}

type record struct {
	offset     int64
	key, value []byte
}

// Create a new hash database with 2^nbuckets available slots
func Create(path string, nbuckets uint32) (db *HashDB, err os.Error) {
	var file *os.File
	if file, err = os.Create(path); err != nil {
		return
	}
	db = &HashDB{
		buckets:  make([]int64, 1<<nbuckets),
		nbuckets: nbuckets,
		file:     file,
	}
	db.writeBuckets()
	return
}

// Write the bucket array to the file
func (db *HashDB) writeBuckets() {
	// TODO(tux21b): Consider using mmap for the bucket array
	db.file.Seek(0, 0)
	binary.Write(db.file, binary.BigEndian, db.nbuckets)
	binary.Write(db.file, binary.BigEndian, db.buckets)
}

// Store a (key, value) pair in the database
func (db *HashDB) Set(key, value []byte) (err os.Error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	offset, err := db.file.Seek(0, 2)
	db.buckets[db.bucket(key)] = offset
	db.writeBuckets()
	err = db.writeRecord(&record{
		offset: offset,
		key:    key,
		value:  value,
	})
	// TODO(tux21b): Sync in specified intervals and just block here
	db.file.Sync()
	return
}

// Retrieve a (key, value) pair from the database
func (db *HashDB) Get(key []byte) (value []byte, err os.Error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	offset := db.buckets[db.bucket(key)]
	if offset == 0 {
		return nil, nil
	}
	rec := &record{offset: offset}
	err = db.readRecord(rec)
	value = rec.value
	return
}

// Calculate the bucket ID for a given key
func (db *HashDB) bucket(key []byte) (bucket_id uint64) {
	// TODO(tux21b): Consider using a faster, non-secure hash here (MurMur?)
	hash := md5.New()
	hash.Write(key)
	sum := hash.Sum()
	for i := uint(0); i < 8; i++ {
		bucket_id |= uint64(sum[i] << (8 * i))
	}
	bucket_id &= ((1 << 64) - 1) >> (64 - db.nbuckets)
	return
}

// Write a record to the file
func (db *HashDB) writeRecord(rec *record) (err os.Error) {
	db.file.Seek(rec.offset, 0)
	err = binary.Write(db.file, binary.BigEndian, uint32(len(rec.key)))
	if err != nil {
		return
	}
	err = binary.Write(db.file, binary.BigEndian, uint32(len(rec.value)))
	if err != nil {
		return
	}
	if _, err = db.file.Write(rec.key); err != nil {
		return
	}
	if _, err = db.file.Write(rec.value); err != nil {
		return
	}
	return
}

// Read a record from the file
func (db *HashDB) readRecord(rec *record) (err os.Error) {
	if _, err = db.file.Seek(rec.offset, 0); err != nil {
		return
	}
	var keyl, valuel uint32
	if err = binary.Read(db.file, binary.BigEndian, &keyl); err != nil {
		return
	}
	if err = binary.Read(db.file, binary.BigEndian, &valuel); err != nil {
		return
	}
	rec.key = make([]byte, keyl)
	if _, err = db.file.Read(rec.key); err != nil {
		return
	}
	rec.value = make([]byte, valuel)
	if _, err = db.file.Read(rec.value); err != nil {
		return
	}
	return
}
