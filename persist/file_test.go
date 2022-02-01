package persist

//	MIT License
//
//	Copyright (c) Microsoft Corporation. All rights reserved.
//
//	Permission is hereby granted, free of charge, to any person obtaining a copy
//	of this software and associated documentation files (the "Software"), to deal
//	in the Software without restriction, including without limitation the rights
//	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//	copies of the Software, and to permit persons to whom the Software is
//	furnished to do so, subject to the following conditions:
//
//	The above copyright notice and this permission notice shall be included in all
//	copies or substantial portions of the Software.
//
//	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//	SOFTWARE

import (
	"errors"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	letterRunes = []rune("abcdefghijklmnopqrstuvwxyz123456789")
)

func init() {
	rand.Seed(time.Now().Unix())
}

func TestFilePersister_Read(t *testing.T) {
	namespace := "namespace"
	name := "name"
	group := "$Default"
	partitionID := "0"
	dir := path.Join(os.TempDir(), RandomName("read", 4))
	persister, err := NewFilePersister(dir)
	assert.Nil(t, err)
	ckp, err := persister.Read(namespace, name, group, partitionID)
	assert.Nil(t, err)
	assert.Equal(t, NewCheckpointFromStartOfStream(), ckp)
}

func TestFilePersister_ReadEmpty(t *testing.T) {
	namespace := "namespace"
	name := "name"
	group := "$Default"
	partitionID := "0"
	dir := path.Join(os.TempDir(), RandomName("read", 4))
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(dir, os.ModePerm)
		assert.Nil(t, err)
	}
	partitionFile := filepath.Join(dir, "hello.json")
	if _, err := os.Stat(partitionFile); err == nil {
		err := os.Remove(partitionFile)
		assert.Nil(t, err)
	}
	fakeJson := []byte("hello\nworld\n")
	err := os.WriteFile(partitionFile, fakeJson, 0644)
	assert.Nil(t, err)

	persister, err := NewFilePersister(dir)
	assert.Nil(t, err)
	ckp, err := persister.Read(namespace, name, group, partitionID)
	assert.Nil(t, err)
	assert.Equal(t, NewCheckpointFromStartOfStream(), ckp)
}

func TestFilePersister_Write(t *testing.T) {
	namespace := "namespace"
	name := "name"
	group := "$Default"
	partitionID := "0"
	dir := path.Join(os.TempDir(), RandomName("write", 4))
	persister, err := NewFilePersister(dir)
	assert.Nil(t, err)
	ckp := NewCheckpoint("120", 22, time.Now())
	err = persister.Write(namespace, name, group, partitionID, ckp)
	assert.Nil(t, err)
	ckp2, err := persister.Read(namespace, name, group, partitionID)
	assert.Nil(t, err)
	assert.Equal(t, ckp.Offset, ckp2.Offset)
	assert.Equal(t, ckp.SequenceNumber, ckp2.SequenceNumber)
}

// RandomName generates a random Event Hub name
func RandomName(prefix string, length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return prefix + "-" + string(b)
}