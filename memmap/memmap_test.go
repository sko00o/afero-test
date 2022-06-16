package memmap

import (
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestMemFsMkdirInConcurrency(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	const dir = "test_dir"
	const n = 1000

	for i := 0; i < n; i++ {
		fs := afero.NewMemMapFs()
		c1 := make(chan error, 1)
		c2 := make(chan error, 1)

		go func() {
			c1 <- fs.Mkdir(dir, 0755)
		}()
		go func() {
			c2 <- fs.Mkdir(dir, 0755)
		}()

		// Only one attempt of creating the directory should succeed.
		err1 := <-c1
		err2 := <-c2
		assert.NotEqualf(err1, err2, "run #%v, more than one success", i)
	}
}

func TestListFilesAfterConcurrencyFileWrite(t *testing.T) {
	const dir = "test_dir"
	const n = 10000
	mfs := afero.NewMemMapFs()
	assert := require.New(t)

	allFilePaths := make([]string, 0, n)

	// run concurrency test
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		fp := filepath.Join(
			dir,
			fmt.Sprintf("%02d", n%10),
			fmt.Sprintf("%d.txt", i),
		)
		allFilePaths = append(allFilePaths, fp)

		wg.Add(1)
		go func() {
			defer wg.Done()

			wt, err := newFileWriter(mfs, fp)
			assert.NoError(err)
			defer func() {
				assert.NoError(wt.Close())
			}()

			// write 30 bytes
			for j := 0; j < 10; j++ {
				_, err := wt.Write([]byte("000"))
				assert.NoError(err)
			}
		}()
	}
	wg.Wait()

	// Test1: find all files by full path access
	for _, fp := range allFilePaths {
		info, err := mfs.Stat(fp)
		assert.NoErrorf(err, "stat file %s", fp)
		assert.Equalf(int64(30), info.Size(), "file %s size unmatch", fp)
	}

	// Test2: find all files by walk
	foundFiles := make([]string, 0, n)
	wErr := afero.Walk(mfs, dir, func(path string, info fs.FileInfo, err error) error {
		assert.NoError(err)
		if info.IsDir() {
			return nil // skip dir
		}
		if strings.HasSuffix(info.Name(), ".txt") {
			foundFiles = append(foundFiles, path)
		}
		return nil
	})
	assert.NoError(wErr, "walk")
	assert.Equal(n, len(foundFiles), "missing files")
}

// newFileWriter mkdir and create file for writer
func newFileWriter(vfs afero.Fs, path string) (io.WriteCloser, error) {
	if err := vfs.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}

	wt, err := vfs.Create(path)
	if err != nil {
		return nil, err
	}
	return wt, nil
}
