package inmemory_test

import (
	"testing"

	store "github.com/hannahhoward/go-storethehash/store"
	"github.com/hannahhoward/go-storethehash/store/primary/inmemory"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	aa := [2][]byte{[]byte("aa"), {0x10}}
	yy := [2][]byte{[]byte("yy"), {0x11}}
	efg := [2][]byte{[]byte("efg"), {0x12}}
	storage := inmemory.NewInmemory([][2][]byte{aa, yy, efg})

	key, value, err := storage.Get(store.Block{Offset: 0})
	require.NoError(t, err)
	result_aa := [2][]byte{key, value}
	require.Equal(t, result_aa, aa)
	key, value, err = storage.Get(store.Block{Offset: 2})
	require.NoError(t, err)
	result_efg := [2][]byte{key, value}

	require.Equal(t, result_efg, efg)
	key, value, err = storage.Get(store.Block{Offset: 1})
	require.NoError(t, err)
	result_yy := [2][]byte{key, value}

	require.Equal(t, result_yy, yy)
}

func TestPut(t *testing.T) {
	aa := [2][]byte{[]byte("aa"), {0x10}}
	yy := [2][]byte{[]byte("yy"), {0x11}}
	efg := [2][]byte{[]byte("efg"), {0x12}}
	storage := inmemory.NewInmemory([][2][]byte{})

	put_aa, err := storage.Put(aa[0], aa[1])
	require.Equal(t, put_aa, store.Block{Offset: 0, Size: 1})
	put_yy, err := storage.Put(yy[0], yy[1])
	require.Equal(t, put_yy, store.Block{Offset: 1, Size: 1})
	put_efg, err := storage.Put(efg[0], efg[1])
	require.Equal(t, put_efg, store.Block{Offset: 2, Size: 1})

	key, value, err := storage.Get(store.Block{Offset: 0})
	require.NoError(t, err)
	result_aa := [2][]byte{key, value}

	require.Equal(t, result_aa, aa)
	key, value, err = storage.Get(store.Block{Offset: 2})
	require.NoError(t, err)
	result_efg := [2][]byte{key, value}

	require.Equal(t, result_efg, efg)
	key, value, err = storage.Get(store.Block{Offset: 1})
	require.NoError(t, err)
	result_yy := [2][]byte{key, value}

	require.Equal(t, result_yy, yy)
}