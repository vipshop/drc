package file_rotater

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// It writes messages by lines limit, file size limit, or time frequency.
type FileRotater struct {
	sync.Mutex // write file order by order and  atomic incr maxLinesCurLines and maxSizeCurSize
	// The opened file
	Filename   string
	fileWriter *os.File

	// Rotate at line
	MaxLines         int
	maxLinesCurLines int

	// Rotate at size
	MaxSize        int
	maxSizeCurSize int

	// Rotate daily
	Daily         bool
	MaxDays       int64
	dailyOpenDate int

	Rotate bool

	Perm os.FileMode

	fileNameOnly, suffix string // like "project.log", project is fileNameOnly and .log is suffix
}

func NewFileRotater(filePath string) (*FileRotater, error) {
	var err error
	w := &FileRotater{
		Filename: filepath.Clean(filePath),
		MaxLines: 1000000,
		MaxSize:  1 << 24, //16 MB
		Daily:    false,
		MaxDays:  7,
		Rotate:   true,
		Perm:     0660,
	}

	w.suffix = filepath.Ext(w.Filename)
	w.fileNameOnly = strings.TrimSuffix(w.Filename, w.suffix)
	if w.suffix == "" {
		w.suffix = ".log"
	}

	err = w.doRotate()
	return w, err
}

// start file rotater. create file and set to locker-inside file writer.
func (w *FileRotater) startRotater() error {
	file, err := w.createFile()
	if err != nil {
		return err
	}
	if w.fileWriter != nil {
		w.fileWriter.Close()
	}
	w.fileWriter = file
	return w.initFd()
}

func (w *FileRotater) needRotate(size int) bool {
	var day int
	if w.Daily {
		_, _, day = time.Now().Date()
	}

	return (w.MaxLines > 0 && w.maxLinesCurLines >= w.MaxLines) ||
		(w.MaxSize > 0 && w.maxSizeCurSize >= w.MaxSize) ||
		(w.Daily && day != w.dailyOpenDate)

}

// WriteMsg write bytes into file.
func (w *FileRotater) Write(b []byte) (n int, err error) {
	if w.Rotate {
		if w.needRotate(len(b)) {
			w.Lock()
			if w.needRotate(len(b)) {
				if err := w.doRotate(); err != nil {
					fmt.Fprintf(os.Stderr, "FileRotater.Write: rotate failed: %s, path: %s\n", err, w.Filename)
				}
			}
			w.Unlock()
		}
	}

	w.Lock()
	n, err = w.fileWriter.Write(b)
	if err == nil {
		w.maxLinesCurLines++
		w.maxSizeCurSize += len(b)
	}
	w.Unlock()
	return
}

func (w *FileRotater) createFile() (*os.File, error) {
	// Open the file
	fd, err := os.OpenFile(w.Filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, w.Perm)
	return fd, err
}

func (w *FileRotater) initFd() error {
	fd := w.fileWriter
	fInfo, err := fd.Stat()
	if err != nil {
		return fmt.Errorf("FileRotater.initFd: get stat err: %s\n", err)
	}
	w.maxSizeCurSize = int(fInfo.Size())
	w.dailyOpenDate = time.Now().Day()
	w.maxLinesCurLines = 0
	if fInfo.Size() > 0 {
		count, err := w.lines()
		if err != nil {
			return err
		}
		w.maxLinesCurLines = count
	}
	return nil
}

func (w *FileRotater) lines() (int, error) {
	fd, err := os.Open(w.Filename)
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	buf := make([]byte, 32768) // 32k
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := fd.Read(buf)
		if err != nil && err != io.EOF {
			return count, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}

// DoRotate means it need to write file in new file.
// new file name like xx.2013-01-01.log (daily) or xx.2013-01-01.001.log (by line or size)
func (w *FileRotater) doRotate() error {
	var err error
	now := time.Now()

	// Find the next available number
	num := 1
	fName := ""
	if w.MaxLines > 0 || w.MaxSize > 0 {
		for ; err == nil && num <= 9999; num++ {
			fName = w.fileNameOnly + fmt.Sprintf(".%s.%04d%s", now.Format("2006-01-02"), num, w.suffix)
			_, err = os.Lstat(fName)
		}
	} else {
		fName = fmt.Sprintf("%s.%s%s", w.fileNameOnly, now.Format("2006-01-02"), w.suffix)
		_, err = os.Lstat(fName)
	}
	// return error if the last file checked still existed
	if err == nil {
		return fmt.Errorf("FileRotater.doRotate: cannot find free file name number to rename %s\n", w.Filename)
	}

	// close fileWriter before rename
	if w.fileWriter != nil {
		w.fileWriter.Close()
	}

	// Rename the file to its new found name
	// even if occurs error,we MUST guarantee to restart new rotater
	renameErr := os.Rename(w.Filename, fName)
	// re-start rotater
	startErr := w.startRotater()
	go w.deleteOldFiles()

	if startErr != nil {
		return fmt.Errorf("FileRotater.doRotate: restart rotater failed: %s\n", startErr)
	}
	if renameErr != nil && !os.IsNotExist(err) {
		return fmt.Errorf("FileRotater.doRotate: rename failed: %s\n", renameErr)
	}
	return nil

}

func (w *FileRotater) deleteOldFiles() {
	dir := filepath.Dir(w.Filename)
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) (returnErr error) {
		if path == w.Filename {
			// We don't need to delete the w.Filename, because it is always up to date,
			// and the w.Filename may not exsit because some race condition
			return
		}
		if err != nil {
			// Because some race condition, the file may not exsit now
			fmt.Fprintf(os.Stderr, "FileRotater.deleteOldFiles: unable to get file info: %s, path: %s\n", err, path)
			return
		}

		if !info.IsDir() && info.ModTime().Unix() < (time.Now().Unix()-60*60*24*w.MaxDays) {
			if strings.HasPrefix(path, w.fileNameOnly) &&
				strings.HasSuffix(path, w.suffix) {
				os.Remove(path)
			}
		}
		return
	})
}

// Destroy close the file description, close file writer.
func (w *FileRotater) Close() {
	w.fileWriter.Close()
}

// Flush flush file.
// there are no buffering messages in file rotater in memory.
// flush file means sync file from disk.
func (w *FileRotater) Flush() {
	w.fileWriter.Sync()
}
