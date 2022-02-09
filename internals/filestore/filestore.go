package filestore

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const numMutexes = 100
const separator = byte('\n')

// FileStore implements ObjectStore and stores objects in files on disk.
// It uses one file per bucket named <bucketId>.dat and in each file it stores data with the following format:
// <objId> <obj len in bytes> <obj data>\n
// In order to retrieve data faster, FileStore holds some metadata about buckets and objects in memory.
// In particular, it retains each object offset in its bucket file and its size in bytes.
//
// To allow concurrent access to multiple buckets, it is used an array of `numMutexes` mutexes. Instead of having
// a mutex per bucket, these mutexes are in a fixed number to avoid:
// 1. to have too many mutexes which can use a lot of memory and decrease performances
// 2. to have maximum 2*numMutexes open files at the same time and avoid errors related to too many open files
//
// Every change to a bucket file is performed in three steps
// 1. copy old data to a temporary file writing new data if needed (like add or replace and object)
// 2. move the temp file and overwrite the actual bucket file
// 3. update bucket's objects metadata if needed
// This mitigates the possibility of data corruption or mismatch between data on file and metadata in memory
type FileStore struct {
	storePath string
	mu        sync.RWMutex               // Global mutex to handle concurrent access to buckets metadata map
	buckets   map[string]*bucketMetadata // Map to store each bucket metadata
	bucketsMu [numMutexes]sync.RWMutex   // Array of numMutexes mutexes to handle concurrent access to each bucket
}

type bucketMetadata struct {
	filePath   string
	muIndex    uint32                     // Index of the mutex in the bucketsMu
	lastObject *objectMetadata            // Last object in the buckets file
	objects    map[string]*objectMetadata // Map to store each object metadata
}

type objectMetadata struct {
	offset   int64           // Offset in bytes of the object within the bucket file
	size     int64           // Size in bytes of the object
	metaSize int64           // Size of the object metadata (<objId> <obj size>)
	prev     *objectMetadata // Previous object in the bucket file
	next     *objectMetadata // Following object in the bucket file
}

func NewStore(storePath string) (*FileStore, error) {
	storePath = filepath.Clean(storePath)
	// Check store folder
	if _, err := os.Stat(storePath); err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("store folder does not exist")
		}
		return nil, errors.New("cannot access store folder: " + err.Error())
	}

	// Load metadata of existing buckets
	buckets, err := loadDataFromDisk(storePath)
	if err != nil {
		return nil, err
	}
	store := FileStore{
		storePath: storePath,
		buckets:   buckets,
	}
	return &store, nil
}

// Store stores the given object with ID `objId` in bucket `bucketId`.
// Returns whether the object has been replaced along with any error encountered.
// If `bucketId` is a new bucket it gets created.
func (f *FileStore) Store(obj []byte, objId, bucketId string) (bool, error) {
	f.mu.Lock()

	bucketMeta, bucketOk := f.buckets[bucketId]
	if !bucketOk {
		// New bucket, create its metadata
		muIndex, err := bucketIdToMutexIndex(bucketId)
		if err != nil {
			f.mu.Unlock()
			return false, err
		}
		bucketFilePath := path.Join(f.storePath, bucketId+".dat")
		bucketMeta = &bucketMetadata{
			filePath: bucketFilePath,
			muIndex:  muIndex,
			objects:  make(map[string]*objectMetadata),
		}
		f.buckets[bucketId] = bucketMeta
		// and create new empty bucket file
		bf, err := os.OpenFile(bucketFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return false, err
		}
		_ = bf.Close()
	}

	bucketMu := &f.bucketsMu[bucketMeta.muIndex]
	bucketMu.Lock()
	f.mu.Unlock()
	defer bucketMu.Unlock()

	// temporary bucket file to write changes to
	tmpFile, err := ioutil.TempFile(f.storePath, bucketId+"_*.tmp")
	if err != nil {
		return false, err
	}
	// delete tmp file if anything goes wrong
	defer func(f *os.File) {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}(tmpFile)

	var newObjMetaSize int64
	var newObjSize int64
	objMeta, objOk := bucketMeta.objects[objId]
	if !objOk {
		// New object, append it to the temp file
		if bucketOk {
			// Copy bucket data to temp file
			bf, err := os.Open(bucketMeta.filePath)
			if err != nil {
				return false, err
			}
			_, err = io.Copy(tmpFile, bf)
			_ = bf.Close()
			if err != nil {
				return false, err
			}
		}

		objMeta = &objectMetadata{
			prev: bucketMeta.lastObject,
		}
		if objMeta.offset, objMeta.metaSize, objMeta.size, err = appendObjectToBucketFile(obj, objId, tmpFile, -1); err != nil {
			return false, err
		}
	} else {
		newObjMetaSize, newObjSize, err = replaceObjectInBucketFile(obj, objId, objMeta, bucketMeta.filePath, tmpFile)
		if err != nil {
			return false, err
		}
	}

	_ = tmpFile.Close()
	if err = os.Rename(tmpFile.Name(), bucketMeta.filePath); err != nil {
		return false, err
	}

	// Store ok, update metadata
	if objOk && newObjSize != objMeta.size {
		// Object has been replaced, need to update other objects metadata
		offsetShift := newObjMetaSize + newObjSize - objMeta.metaSize - objMeta.size
		for nextObj := objMeta.next; nextObj != nil; nextObj = nextObj.next {
			nextObj.offset += offsetShift
		}
		objMeta.metaSize = newObjMetaSize
		objMeta.size = newObjSize
	}
	// Else new object has the same size of the old one, no need to update metadata

	if !objOk {
		bucketMeta.objects[objId] = objMeta
		if bucketMeta.lastObject != nil {
			bucketMeta.lastObject.next = objMeta
		}
		bucketMeta.lastObject = objMeta
	}

	return objOk, nil
}

// Retrieve retrieves the object `objId` in bucket `bucketId`.
// It returns the object in bytes (or nil if it was not found), whether it has been found or not, along with any error.
func (f *FileStore) Retrieve(objId, bucketId string) ([]byte, bool, error) {
	f.mu.RLock()

	bucketMeta, ok := f.buckets[bucketId]
	if !ok {
		f.mu.RUnlock()
		return nil, false, nil
	}

	bucketMu := &f.bucketsMu[bucketMeta.muIndex]
	bucketMu.RLock()
	defer bucketMu.RUnlock()

	f.mu.RUnlock()

	objMeta, ok := bucketMeta.objects[objId]
	if !ok {
		return nil, false, nil
	}

	bf, err := os.Open(bucketMeta.filePath)
	if err != nil {
		return nil, false, err
	}
	defer bf.Close()

	obj := make([]byte, objMeta.size)
	_, err = bf.ReadAt(obj, objMeta.offset+objMeta.metaSize+1)
	if err != nil {
		return nil, false, errors.New("error reading object from bucket file: " + err.Error())
	}

	return obj, true, nil
}

// Delete deletes the object `objId` in bucket `bucketId`. If the bucket is emptied it removes both the bucket file
// and metadata from the bucket metadata map.
// It returns whether the object has been deleted or not along with any error.
func (f *FileStore) Delete(objId, bucketId string) (bool, error) {
	f.mu.Lock()

	bucketMeta, bucketOk := f.buckets[bucketId]
	if !bucketOk {
		f.mu.Unlock()
		return false, nil
	}
	bucketMu := &f.bucketsMu[bucketMeta.muIndex]
	bucketMu.Lock()
	f.mu.Unlock()
	defer bucketMu.Unlock()

	objMeta, objOk := bucketMeta.objects[objId]
	if !objOk {
		return false, nil
	}

	// if bucket will be emptied remove its metadata and file
	if len(bucketMeta.objects) == 1 {
		if err := os.Remove(bucketMeta.filePath); err == nil {
			delete(f.buckets, bucketId)
		}
		return true, nil
	}

	// temporary bucket file to write changes to
	tmpFile, err := ioutil.TempFile("", bucketId+"_*.dat")
	if err != nil {
		return false, err
	}
	// delete tmp file if anything goes wrong
	defer func(f *os.File) {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}(tmpFile)

	_, _, err = replaceObjectInBucketFile(nil, objId, objMeta, bucketMeta.filePath, tmpFile)
	if err != nil {
		return false, err
	}

	// move the tmp file to the actual bucket file
	_ = tmpFile.Close()
	if err = os.Rename(tmpFile.Name(), bucketMeta.filePath); err != nil {
		return false, err
	}

	delete(bucketMeta.objects, objId)
	if objMeta.prev != nil {
		objMeta.prev.next = objMeta.next
	}

	if objMeta.next == nil {
		bucketMeta.lastObject = objMeta.prev
	} else {
		objMeta.next.prev = objMeta.prev
		// update metadata
		removedBytes := objMeta.metaSize + objMeta.size + 2
		for nextObj := objMeta.next; nextObj != nil; nextObj = nextObj.next {
			nextObj.offset -= removedBytes
		}
	}

	return true, nil
}

// appendObjectToBucketFile appends the object `obj` to the end of the file `file`.
// If `offset` parameter is < 0 calculate and return the new object offset.
// Returns the actual object offset and its metadata and object length along with any error.
func appendObjectToBucketFile(obj []byte, objId string, file *os.File, offset int64) (int64, int64, int64, error) {
	if offset < 0 {
		f, err := file.Stat()
		if err != nil {
			return 0, 0, 0, err
		}
		offset = f.Size()
	}
	metaStr := fmt.Sprintf("%s %d ", objId, len(obj))
	if _, err := file.Write([]byte(metaStr)); err != nil {
		return 0, 0, 0, err
	}
	if _, err := file.Write(obj); err != nil {
		return 0, 0, 0, err
	}
	if _, err := file.Write([]byte{separator}); err != nil {
		return 0, 0, 0, err
	}

	return offset, int64(len(metaStr) - 1), int64(len(obj)), nil
}

// replaceObjectInBucketFile replaces an object `obj` in the position defined by the `objMeta`
// by copying data from original bucket file `bf` to temporary file `tf`.
// If the new object is nil, this function deletes the object at the position defined by the `objMeta`.
// Returns the new metadata and object length along with any error encountered in the process.
func replaceObjectInBucketFile(obj []byte, objId string, objMeta *objectMetadata, bfName string, file *os.File) (int64, int64, error) {
	bf, err := os.Open(bfName)
	if err != nil {
		return 0, 0, err
	}
	defer bf.Close()

	// copy old bucket file content up to the object to be replaced to tmp file
	firstHalf := objMeta.offset
	if _, err = io.CopyN(file, bf, firstHalf); err != nil {
		return 0, 0, err
	}

	newObjSize := int64(len(obj))
	var newMetaSize int64
	if obj != nil {
		if newObjSize != objMeta.size {
			// Insert new object
			if _, newMetaSize, _, err = appendObjectToBucketFile(obj, objId, file, objMeta.offset); err != nil {
				return 0, 0, err
			}
		} else {
			// Just copy the old metadata and replace the object
			if _, err = io.CopyN(file, bf, objMeta.metaSize+1); err != nil {
				return 0, 0, err
			}
			_, err = file.Write(obj)
			if err != nil {
				return 0, 0, err
			}
			_, err = file.Write([]byte{separator})
			if err != nil {
				return 0, 0, err
			}
		}
	}

	// copy the remaining objects if any
	offset := objMeta.offset + objMeta.metaSize + objMeta.size + 2
	if offset > 0 {
		if _, err = bf.Seek(offset, 0); err != nil {
			return 0, 0, err
		}
	}
	if _, err = io.Copy(file, bf); err != nil {
		return 0, 0, err
	}

	return newMetaSize, newObjSize, nil
}

// loadDataFromDisk calculates buckets metadata from files in the given store path
func loadDataFromDisk(storePath string) (map[string]*bucketMetadata, error) {
	bucketFiles, err := filepath.Glob(path.Join(storePath, "*.dat"))
	if err != nil {
		return nil, err
	}

	buckets := make(map[string]*bucketMetadata)
	for _, bfPath := range bucketFiles {
		fileNameParts := strings.Split(path.Base(filepath.ToSlash(bfPath)), ".")
		bucketId := strings.Join(fileNameParts[:len(fileNameParts)-1], ".")
		muIndex, err := bucketIdToMutexIndex(bucketId)
		if err != nil {
			return nil, err
		}

		bf, err := os.Open(bfPath)
		if err != nil {
			return nil, errors.New("error opening bucket file: " + err.Error())
		}
		objectsMeta, lastObject, err := getObjectsMetadata(bf)
		_ = bf.Close()
		if err != nil {
			return nil, err
		}

		if len(objectsMeta) > 0 {
			buckets[bucketId] = &bucketMetadata{
				filePath:   bfPath,
				muIndex:    muIndex,
				objects:    objectsMeta,
				lastObject: lastObject,
			}
		} else {
			// remove empty bucket file
			_ = os.Remove(bfPath)
		}
	}
	return buckets, nil
}

// getObjectsMetadata calculates objects metadata of a bucket file starting from the given offset
func getObjectsMetadata(bf *os.File) (map[string]*objectMetadata, *objectMetadata, error) {
	objectsMeta := make(map[string]*objectMetadata)

	var objOffset int64
	var lastObjMeta *objectMetadata
	r := bufio.NewReader(bf)
	for {
		objId, err := r.ReadString(' ')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}
		objId = objId[:len(objId)-1]

		objSizeStr, err := r.ReadString(' ')
		if err != nil {
			return nil, nil, err
		}
		objSizeStr = objSizeStr[:len(objSizeStr)-1]

		objMetaSize := int64(len(objId)+len(objSizeStr)) + 1 // add space size

		objSize, err := strconv.ParseInt(objSizeStr, 10, 64)
		if err != nil {
			return nil, nil, errors.New("error parsing object size: " + err.Error())
		}

		if _, err = r.Discard(int(objSize + 1)); err != nil {
			return nil, nil, err
		}

		objectMeta := &objectMetadata{
			offset:   objOffset,
			size:     objSize,
			metaSize: objMetaSize,
			prev:     lastObjMeta,
		}
		if lastObjMeta != nil {
			lastObjMeta.next = objectMeta
		}
		lastObjMeta = objectMeta
		objectsMeta[objId] = objectMeta

		objOffset += objMetaSize + objSize + 2 // add space and newline size
	}

	return objectsMeta, lastObjMeta, nil
}

// bucketIdToMutexIndex calculates the index in the buckets mutexes array based on a hash of the bucket ID
func bucketIdToMutexIndex(bucketId string) (uint32, error) {
	f := fnv.New32()
	n, err := f.Write([]byte(bucketId))
	if n < len(bucketId) {
		return 0, errors.New("cannot compute bucketMetadata ID hash")
	}
	if err != nil {
		return 0, errors.New("cannot compute bucketMetadata ID hash: " + err.Error())
	}

	index := f.Sum32()
	return index % numMutexes, nil
}
