package index_test

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/hannahhoward/go-storethehash/store/index"
	"github.com/hannahhoward/go-storethehash/store/primary/inmemory"
	"github.com/hannahhoward/go-storethehash/store/types"
	"github.com/stretchr/testify/require"
)

func TestFirstNonCommonByte(t *testing.T) {
	require.Equal(t, index.FirstNonCommonByte([]byte{0}, []byte{1}), 0)
	require.Equal(t, index.FirstNonCommonByte([]byte{0}, []byte{0}), 1)
	require.Equal(t, index.FirstNonCommonByte([]byte{0, 1, 2, 3}, []byte{0}), 1)
	require.Equal(t, index.FirstNonCommonByte([]byte{0}, []byte{0, 1, 2, 3}), 1)
	require.Equal(t, index.FirstNonCommonByte([]byte{0, 1, 2}, []byte{0, 1, 2, 3}), 3)
	require.Equal(t, index.FirstNonCommonByte([]byte{0, 1, 2, 3}, []byte{0, 1, 2}), 3)
	require.Equal(t, index.FirstNonCommonByte([]byte{3, 2, 1, 0}, []byte{0, 1, 2}), 0)
	require.Equal(t, index.FirstNonCommonByte([]byte{0, 1, 1, 0}, []byte{0, 1, 2}), 2)
	require.Equal(t,
		index.FirstNonCommonByte([]byte{180, 9, 113, 0}, []byte{180, 0, 113, 0}),
		1,
	)
}

func assertHeader(t *testing.T, indexPath string, bucketsBits uint8) {
	indexData, err := ioutil.ReadFile(indexPath)
	require.NoError(t, err)
	headerSize := binary.LittleEndian.Uint32(indexData)
	require.Equal(t, headerSize, uint32(2))
	headerData := indexData[len(indexData)-int(headerSize):]
	header := index.FromBytes(headerData)
	require.Equal(t, header.Version, index.IndexVersion)
	require.Equal(t, header.BucketsBits, bucketsBits)
}

// Asserts that given two keys that on the first insert the key is trimmed to a single byte and on
// the second insert they are trimmed to the minimal distinguishable prefix
func assertCommonPrefixTrimmed(t *testing.T, key1 []byte, key2 []byte, expectedKeyLength int) {
	const bucketBits uint8 = 24
	primaryStorage := inmemory.NewInmemory([][2][]byte{{key1, {0x20}}, {key2, {0x30}}})
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	i, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)
	err = i.Put(key1, types.Block{Offset: 0, Size: 1})
	require.NoError(t, err)
	_, err = i.Flush()
	require.NoError(t, err)
	err = i.Sync()
	require.NoError(t, err)
	err = i.Put(key2, types.Block{Offset: 1, Size: 1})
	require.NoError(t, err)
	_, err = i.Flush()
	require.NoError(t, err)
	err = i.Sync()
	require.NoError(t, err)

	// Skip header
	file, err := os.Open(indexPath)
	require.NoError(t, err)
	_, bytesRead, err := index.ReadHeader(file)
	require.NoError(t, err)
	iter := index.NewIndexIter(file, bytesRead)

	// The record list is append only, hence the first record list only contains the first insert
	{
		data, _, err, done := iter.Next()
		require.NoError(t, err)
		require.False(t, done)
		recordlist := index.NewRecordList(data)
		recordIter := recordlist.Iter()
		var keyLengths []int
		for !recordIter.Done() {
			record := recordIter.Next()
			keyLengths = append(keyLengths, len(record.Key))
		}
		require.Equal(t, keyLengths, []int{1}, "Single key has the expected length of 1")
	}

	// The second block contains both keys
	{
		data, _, err, done := iter.Next()
		require.NoError(t, err)
		require.False(t, done)
		recordlist := index.NewRecordList(data)
		recordIter := recordlist.Iter()
		var keyLengths []int
		for !recordIter.Done() {
			record := recordIter.Next()
			keyLengths = append(keyLengths, len(record.Key))
		}
		require.Equal(t,
			keyLengths,
			[]int{expectedKeyLength, expectedKeyLength},
			"All keys are trimmed to their minimal distringuishable prefix",
		)
	}
}

// This test is about making sure that inserts into an empty bucket result in a key that is trimmed
// to a single byte.

func TestIndexPutSingleKey(t *testing.T) {
	const bucketBits uint8 = 24
	primaryStorage := inmemory.NewInmemory([][2][]byte{})
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	i, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)
	err = i.Put([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, types.Block{Offset: 222, Size: 10})
	require.NoError(t, err)
	_, err = i.Flush()
	require.NoError(t, err)
	err = i.Sync()
	require.NoError(t, err)

	// Skip header
	file, err := os.Open(indexPath)
	require.NoError(t, err)
	_, bytesRead, err := index.ReadHeader(file)
	require.NoError(t, err)
	iter := index.NewIndexIter(file, bytesRead)
	data, _, err, done := iter.Next()
	require.NoError(t, err)
	require.False(t, done)
	recordlist := index.NewRecordList(data)
	recordIter := recordlist.Iter()
	require.False(t, recordIter.Done())
	record := recordIter.Next()
	require.Equal(t,
		len(record.Key),
		1,
		"Key is trimmed to one byteas it's the only key in the record list",
	)
}

// This test is about making sure that a new key that doesn't share any prefix with other keys
// within the same bucket is trimmed to a single byte.
func TestIndexPutDistinctKey(t *testing.T) {
	const bucketBits uint8 = 24
	primaryStorage := inmemory.NewInmemory([][2][]byte{})
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	i, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)
	err = i.Put([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, types.Block{Offset: 222, Size: 10})
	require.NoError(t, err)
	err = i.Put([]byte{1, 2, 3, 55, 5, 6, 7, 8, 9, 10}, types.Block{Offset: 333, Size: 10})
	require.NoError(t, err)
	_, err = i.Flush()
	require.NoError(t, err)
	err = i.Sync()
	require.NoError(t, err)

	// Skip header
	file, err := os.Open(indexPath)
	require.NoError(t, err)
	_, bytesRead, err := index.ReadHeader(file)
	require.NoError(t, err)
	iter := index.NewIndexIter(file, bytesRead)

	// The record list is append only, hence the first record list only contains the first insert
	var data []byte
	for {
		next, _, err, done := iter.Next()
		require.NoError(t, err)
		if done {
			break
		}
		data = next
	}
	recordlist := index.NewRecordList(data)
	recordIter := recordlist.Iter()
	var keys [][]byte
	for !recordIter.Done() {
		record := recordIter.Next()
		keys = append(keys, record.Key)
	}
	require.Equal(t, keys, [][]byte{{4}, {55}}, "All keys are trimmed to a single byte")
}
func TestCorrectCacheReading(t *testing.T) {
	const bucketBits uint8 = 24
	primaryStorage := inmemory.NewInmemory([][2][]byte{})
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	i, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)
	// put key in, then flush the cache
	err = i.Put([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, types.Block{Offset: 222, Size: 10})
	require.NoError(t, err)
	_, err = i.Flush()
	require.NoError(t, err)
	// now put two keys in the same bucket
	err = i.Put([]byte{1, 2, 3, 55, 5, 6, 7, 8, 9, 10}, types.Block{Offset: 333, Size: 10})
	require.NoError(t, err)
	err = i.Put([]byte{1, 2, 3, 88, 5, 6, 7, 8, 9, 10}, types.Block{Offset: 500, Size: 10})
	require.NoError(t, err)

	block, found, err := i.Get([]byte{1, 2, 3, 55, 5, 6, 7, 8, 9, 10})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, types.Block{Offset: 333, Size: 10}, block)
}

// This test is about making sure that a key is trimmed correctly if it shares a prefix with the
// previous key

func TestIndexPutPrevKeyCommonPrefix(t *testing.T) {
	key1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	key2 := []byte{1, 2, 3, 4, 5, 6, 9, 9, 9, 9}
	assertCommonPrefixTrimmed(t, key1, key2, 4)
}

// This test is about making sure that a key is trimmed correctly if it shares a prefix with the
// next key
func TestIndexPutNextKeyCommonPrefix(t *testing.T) {
	key1 := []byte{1, 2, 3, 4, 5, 6, 9, 9, 9, 9}
	key2 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	assertCommonPrefixTrimmed(t, key1, key2, 4)
}

// This test is about making sure that a key is trimmed correctly if it shares a prefix with the
// previous and the next key, where the common prefix with the next key is longer.
func TestIndexPutPrevAndNextKeyCommonPrefix(t *testing.T) {
	key1 := []byte{1, 2, 3, 4, 5, 6, 9, 9, 9, 9}
	key2 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	key3 := []byte{1, 2, 3, 4, 5, 6, 9, 8, 8, 8}

	const bucketBits uint8 = 24
	primaryStorage := inmemory.NewInmemory([][2][]byte{
		{key1, {0x10}},
		{key2, {0x20}},
		{key3, {0x30}},
	})
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	i, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)
	err = i.Put(key1, types.Block{Offset: 0, Size: 1})
	require.NoError(t, err)
	err = i.Put(key2, types.Block{Offset: 1, Size: 1})
	require.NoError(t, err)
	err = i.Put(key3, types.Block{Offset: 1, Size: 1})
	require.NoError(t, err)
	_, err = i.Flush()
	require.NoError(t, err)
	err = i.Sync()
	require.NoError(t, err)

	// Skip header
	file, err := os.Open(indexPath)
	require.NoError(t, err)
	_, bytesRead, err := index.ReadHeader(file)
	require.NoError(t, err)
	iter := index.NewIndexIter(file, bytesRead)

	var data []byte
	for {
		next, _, err, done := iter.Next()
		require.NoError(t, err)
		if done {
			break
		}
		data = next
	}
	recordlist := index.NewRecordList(data)
	recordIter := recordlist.Iter()
	var keys [][]byte
	for !recordIter.Done() {
		record := recordIter.Next()
		keys = append(keys, record.Key)
	}
	require.Equal(t,
		keys,
		[][]byte{{4, 5, 6, 7}, {4, 5, 6, 9, 8}, {4, 5, 6, 9, 9}},
		"Keys are correctly sorted and trimmed",
	)
}

func TestIndexGetEmptyIndex(t *testing.T) {
	key := []byte{1, 2, 3, 4, 5, 6, 9, 9, 9, 9}
	const bucketBits uint8 = 24
	primaryStorage := inmemory.NewInmemory([][2][]byte{})
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	index, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)
	_, found, err := index.Get(key)
	require.NoError(t, err)
	require.False(t, found, "Key was not found")
}

func TestIndexGet(t *testing.T) {
	key1 := []byte{1, 2, 3, 4, 5, 6, 9, 9, 9, 9}
	key2 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	key3 := []byte{1, 2, 3, 4, 5, 6, 9, 8, 8, 8}

	const bucketBits uint8 = 24
	primaryStorage := inmemory.NewInmemory([][2][]byte{
		{key1, {0x10}},
		{key2, {0x20}},
		{key3, {0x30}},
	})
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	i, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)
	err = i.Put(key1, types.Block{Offset: 0, Size: 1})
	require.NoError(t, err)
	err = i.Put(key2, types.Block{Offset: 1, Size: 1})
	require.NoError(t, err)
	err = i.Put(key3, types.Block{Offset: 2, Size: 1})
	require.NoError(t, err)

	firstKeyBlock, found, err := i.Get(key1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, firstKeyBlock, types.Block{Offset: 0, Size: 1})

	secondKeyBlock, found, err := i.Get(key2)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, secondKeyBlock, types.Block{Offset: 1, Size: 1})

	thirdKeyBlock, found, err := i.Get(key3)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, thirdKeyBlock, types.Block{Offset: 2, Size: 1})

	// It still hits a bucket where there are keys, but that key doesn't exist.
	_, found, err = i.Get([]byte{1, 2, 3, 4, 5, 9})
	require.False(t, found)
	require.NoError(t, err)

	// A key that matches some prefixes but it shorter than the prefixes.
	_, found, err = i.Get([]byte{1, 2, 3, 4, 5})
	require.False(t, found)
	require.NoError(t, err)

	// same should hold true after flush
	_, err = i.Flush()
	require.NoError(t, err)
	err = i.Sync()
	require.NoError(t, err)

	firstKeyBlock, found, err = i.Get(key1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, firstKeyBlock, types.Block{Offset: 0, Size: 1})

	secondKeyBlock, found, err = i.Get(key2)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, secondKeyBlock, types.Block{Offset: 1, Size: 1})

	thirdKeyBlock, found, err = i.Get(key3)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, thirdKeyBlock, types.Block{Offset: 2, Size: 1})

	// It still hits a bucket where there are keys, but that key doesn't exist.
	_, found, err = i.Get([]byte{1, 2, 3, 4, 5, 9})
	require.False(t, found)
	require.NoError(t, err)

	// A key that matches some prefixes but it shorter than the prefixes.
	_, found, err = i.Get([]byte{1, 2, 3, 4, 5})
	require.False(t, found)
	require.NoError(t, err)

	err = i.Close()
	require.NoError(t, err)
	i, err = index.OpenIndex(indexPath, primaryStorage, bucketBits)
	require.NoError(t, err)

	// same should hold true when index is closed and reopened

	firstKeyBlock, found, err = i.Get(key1)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, firstKeyBlock, types.Block{Offset: 0, Size: 1})

	secondKeyBlock, found, err = i.Get(key2)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, secondKeyBlock, types.Block{Offset: 1, Size: 1})

	thirdKeyBlock, found, err = i.Get(key3)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, thirdKeyBlock, types.Block{Offset: 2, Size: 1})

}

func TestIndexHeader(t *testing.T) {
	const bucketBits uint8 = 24
	tempDir, err := ioutil.TempDir("", "sth")
	require.NoError(t, err)
	indexPath := filepath.Join(tempDir, "storethehash.index")
	{
		primaryStorage := inmemory.NewInmemory([][2][]byte{})
		_, err := index.OpenIndex(indexPath, primaryStorage, bucketBits)
		require.NoError(t, err)
		assertHeader(t, indexPath, bucketBits)
	}
	// Check that the header doesn't change if the index is opened again.
	{
		_, err := index.OpenIndex(indexPath, inmemory.NewInmemory([][2][]byte{}), bucketBits)
		require.NoError(t, err)
		assertHeader(t, indexPath, bucketBits)
	}
}
