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
	"bytes"
)

// The HashDB provides a persistent hash table with the usual O(1)
// characteristics.
type HashDB struct {
	nbuckets uint32   // 2^nbuckets buckets
	buckets  []uint64 // bucket array
	file     *os.File // associated file
	mu       sync.RWMutex
}

type record struct {
	offset      uint64 // absolute offset of this record
	size        uint32 // size (in bytes), including padding
	left, right uint64 // absolute offset of the left and right node
	key, value  []byte // key and value
}

// Create a new hash database with 2^nbuckets available slots
func Create(path string, nbuckets uint32) (db *HashDB, err os.Error) {
	var file *os.File
	if file, err = os.Create(path); err != nil {
		return
	}
	db = &HashDB{
		buckets:  make([]uint64, 1<<nbuckets),
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
	if err != nil {
		return err
	}
	bucket_id := db.bucket(key)
	if db.buckets[bucket_id] == 0 {
		db.buckets[bucket_id] = uint64(offset)
		db.writeBuckets()
	} else {
		// hash collision
		other := &record{offset: db.buckets[bucket_id]}
		cmp, err := db.binSearch(key, other)
		if err != nil {
			return err
		}
		switch {
		case cmp == 0:
			// TODO(tux21b): Updates
		case cmp < 0:
			other.left = uint64(offset)
			db.writeRecord(other)
		case cmp > 0:
			other.right = uint64(offset)
			db.writeRecord(other)
		}
	}

	err = db.writeRecord(&record{
		offset: uint64(offset),
		size:   nextPowerTwo(uint32(28 + len(key) + len(value))),
		key:    key,
		value:  value,
	})
	// TODO(tux21b): Sync in specified intervals and just block here
	db.file.Sync()
	return
}

// Perform a binary search to find a specific record. If no matching
// record was found, then rec is set to the parent record (useful for
// inserting).
func (db *HashDB) binSearch(key []byte, rec *record) (int, os.Error) {
	if err := db.readRecord(rec); err != nil {
		return 0, err
	}
	cmp := bytes.Compare(key, rec.key)
	switch {
	case cmp < 0 && rec.left != 0:
		rec.offset = rec.left
		return db.binSearch(key, rec)
	case cmp > 0 && rec.right != 0:
		rec.offset = rec.right
		return db.binSearch(key, rec)
	}
	return cmp, nil
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
	cmp, err := db.binSearch(key, rec)
	if err != nil || cmp != 0 {
		return nil, err
	}
	value = rec.value
	return
}

// Calculate the bucket ID for a given key. This ID is always between 0
// and 2^nbuckets - 1 inclusive.
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

// Write a record to the file.
func (db *HashDB) writeRecord(rec *record) (err os.Error) {
	buffer := bytes.NewBuffer(make([]byte, rec.size)[:0])
	binary.Write(buffer, binary.BigEndian, uint32(rec.size))
	binary.Write(buffer, binary.BigEndian, uint64(rec.left))
	binary.Write(buffer, binary.BigEndian, uint64(rec.right))
	binary.Write(buffer, binary.BigEndian, uint32(len(rec.key)))
	binary.Write(buffer, binary.BigEndian, uint32(len(rec.value)))
	buffer.Write(rec.key)
	buffer.Write(rec.value)
	_, err = db.file.WriteAt(buffer.Bytes()[:rec.size], int64(rec.offset))
	return
}

// Read a record from the file
func (db *HashDB) readRecord(rec *record) (err os.Error) {
	header := make([]byte, 28)
	if _, err = db.file.ReadAt(header, int64(rec.offset)); err != nil {
		return
	}
	var keyl, vall uint32
	buffer := bytes.NewBuffer(header)
	binary.Read(buffer, binary.BigEndian, &rec.size)
	binary.Read(buffer, binary.BigEndian, &rec.left)
	binary.Read(buffer, binary.BigEndian, &rec.right)
	binary.Read(buffer, binary.BigEndian, &keyl)
	binary.Read(buffer, binary.BigEndian, &vall)

	data := make([]byte, keyl+vall)
	_, err = db.file.ReadAt(data, int64(rec.offset)+int64(len(header)))
	if err != nil {
		return
	}
	rec.key = data[:keyl]
	rec.value = data[keyl : keyl+vall]
	return
}

// Calculates the next power of two
func nextPowerTwo(x uint32) uint32 {
	if x == 0 {
		return 1
	}
	x--
	for i := uint32(1); i < 4*32; i <<= 1 {
		x |= x >> i
	}
	return x + 1
}
