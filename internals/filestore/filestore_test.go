package filestore

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
)

type testBucket struct {
	bucketData     string
	bucketMetadata *bucketMetadata
}

var testBuckets = map[string]*testBucket{
	"testBucket1": {
		bucketData: "o1a 5 1stob\no2-a 12 2nd nice obj\no3.0a 7 3rd obj\n",
		bucketMetadata: &bucketMetadata{
			filePath: "testBucket1.dat",
			muIndex:  8,
			objects: map[string]*objectMetadata{
				"o1a":   {offset: 0, size: 5, metaSize: 5},
				"o2-a":  {offset: 12, size: 12, metaSize: 7},
				"o3.0a": {offset: 33, size: 7, metaSize: 7},
			},
		},
	},
	"testBucket2": {
		bucketData: "obj1b 9 1st obj b\nob02b 8 2nd ob b\no3-0bb 19 3rd obj in bucket b\n",
		bucketMetadata: &bucketMetadata{
			filePath: "testBucket2.dat",
			muIndex:  11,
			objects: map[string]*objectMetadata{
				"obj1b":  {offset: 0, size: 9, metaSize: 7},
				"ob02b":  {offset: 18, size: 8, metaSize: 7},
				"o3-0bb": {offset: 35, size: 19, metaSize: 9},
			},
		},
	},
}

func TestFileStore_loadDataFromDisk(t *testing.T) {
	tests := []struct {
		name    string
		buckets map[string]*testBucket
	}{{
		name: "empty store",
	}, {
		name:    "ok",
		buckets: testBuckets,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, testBucket := range tt.buckets {
				bucketPath := testBucket.bucketMetadata.filePath
				f, _ := os.Create(bucketPath)
				_, _ = f.Write([]byte(testBucket.bucketData))
				_ = f.Close()
				defer os.Remove(bucketPath)
			}

			buckets, err := loadDataFromDisk(".")
			assert.NoError(t, err)
			for bid, expBucket := range tt.buckets {
				if !assert.Contains(t, buckets, bid) {
					return
				}
				assertBucketsMetaEqualf(t, expBucket.bucketMetadata, buckets[bid], "")
			}
		})
	}
}

func TestNewFilestore(t *testing.T) {
	type args struct {
		storePath string
	}
	tests := []struct {
		name    string
		args    args
		buckets map[string]*bucketMetadata
		wantErr assert.ErrorAssertionFunc
	}{{
		name:    "no buckets",
		args:    args{storePath: "."},
		wantErr: assert.NoError,
	}, {
		name:    "one bucket",
		args:    args{storePath: "."},
		buckets: map[string]*bucketMetadata{"testBucket1": testBuckets["testBucket1"].bucketMetadata},
		wantErr: assert.NoError,
	}, {
		name: "two buckets",
		args: args{storePath: "."},
		buckets: map[string]*bucketMetadata{
			"testBucket1": testBuckets["testBucket1"].bucketMetadata,
			"testBucket2": testBuckets["testBucket2"].bucketMetadata,
		},
		wantErr: assert.NoError,
	}, {
		name:    "missing store folder ",
		args:    args{storePath: "non_ext"},
		wantErr: assert.Error,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for bucketId, bucketMeta := range tt.buckets {
				writeTestBucket(bucketId)
				bucketPath := bucketMeta.filePath
				defer os.Remove(bucketPath)
			}

			s, err := NewStore(tt.args.storePath)
			if !tt.wantErr(t, err, fmt.Sprintf("NewStore(%v)", tt.args.storePath)) {
				return
			}
			for tbId, tb := range tt.buckets {
				if assert.Contains(t, s.buckets, tbId) {
					assertBucketsMetaEqualf(t, tb, s.buckets[tbId], "NewStore(%v)", tt.args.storePath)
				}
			}
		})
	}
}

func TestFileStore_Store(t *testing.T) {
	type args struct {
		obj      []byte
		objId    string
		bucketId string
	}
	tests := []struct {
		name          string
		args          args
		repl          bool
		wantErr       assert.ErrorAssertionFunc
		bucketPath    string
		bucketContent string
		bucketObjects map[string]*objectMetadata
	}{{
		name:          "new bucket",
		args:          args{obj: []byte("new obj"), objId: "o1", bucketId: "empty"},
		wantErr:       assert.NoError,
		bucketContent: "o1 7 new obj\n",
		bucketObjects: map[string]*objectMetadata{"o1": {offset: 0, size: 7, metaSize: 4}},
	}, {
		name:          "append to bucket",
		args:          args{obj: []byte("new obj"), objId: "nObj", bucketId: "testBucket1"},
		wantErr:       assert.NoError,
		bucketContent: testBuckets["testBucket1"].bucketData + "nObj 7 new obj\n",
		bucketObjects: map[string]*objectMetadata{
			"o1a":   {offset: 0, size: 5, metaSize: 5},
			"o2-a":  {offset: 12, size: 12, metaSize: 7},
			"o3.0a": {offset: 33, size: 7, metaSize: 7},
			"nObj":  {offset: 49, size: 7, metaSize: 6},
		},
	}, {
		name:          "replace same size",
		args:          args{obj: []byte("same obj"), objId: "ob02b", bucketId: "testBucket2"},
		wantErr:       assert.NoError,
		repl:          true,
		bucketContent: strings.Replace(testBuckets["testBucket2"].bucketData, "2nd ob b", "same obj", 1),
		bucketObjects: testBuckets["testBucket2"].bucketMetadata.objects,
	}, {
		name:          "replace longer",
		args:          args{obj: []byte("new long obj"), objId: "o1a", bucketId: "testBucket1"},
		wantErr:       assert.NoError,
		repl:          true,
		bucketContent: strings.Replace(testBuckets["testBucket1"].bucketData, "5 1stob", "12 new long obj", 1),
		bucketObjects: map[string]*objectMetadata{
			"o1a":   {offset: 0, size: 12, metaSize: 6},
			"o2-a":  {offset: 20, size: 12, metaSize: 7},
			"o3.0a": {offset: 41, size: 7, metaSize: 7},
		},
	}, {
		name:          "replace shorter",
		args:          args{obj: []byte("o"), objId: "ob02b", bucketId: "testBucket2"},
		wantErr:       assert.NoError,
		repl:          true,
		bucketPath:    "testBucket2.dat",
		bucketContent: strings.Replace(testBuckets["testBucket2"].bucketData, "8 2nd ob b", "1 o", 1),
		bucketObjects: map[string]*objectMetadata{
			"obj1b":  {offset: 0, size: 9, metaSize: 7},
			"ob02b":  {offset: 18, size: 1, metaSize: 7},
			"o3-0bb": {offset: 28, size: 19, metaSize: 9},
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeTestBucket(tt.args.bucketId)
			defer os.Remove(tt.args.bucketId + ".dat")

			s, err := NewStore(".")
			if !assert.NoError(t, err) {
				return
			}

			repl, err := s.Store(tt.args.obj, tt.args.objId, tt.args.bucketId)

			if !tt.wantErr(t, err, fmt.Sprintf("Store(%v, %v, %v)", tt.args.obj, tt.args.objId, tt.args.bucketId)) {
				return
			}
			assert.Equalf(t, tt.repl, repl, "Store(%v, %v, %v)", tt.args.obj, tt.args.objId, tt.args.bucketId)

			bucketContent, err := os.ReadFile(tt.args.bucketId + ".dat")
			if err != nil {
				t.Errorf("Store(%v, %v, %v): cannot read produced bucket file: %v", tt.args.obj, tt.args.objId, tt.args.bucketId, err)
			}
			assert.Equalf(t, tt.bucketContent, string(bucketContent), "Store(%v, %v, %v)", tt.args.obj, tt.args.objId, tt.args.bucketId)
			assertObjsMetaEqualf(t, tt.bucketObjects, s.buckets[tt.args.bucketId].objects, "Store(%v, %v, %v)", tt.args.obj, tt.args.objId, tt.args.bucketId)
		})
	}
}

func TestFileStore_Retrieve(t *testing.T) {
	type args struct {
		objId    string
		bucketId string
	}
	tests := []struct {
		name    string
		args    args
		obj     string
		retr    bool
		wantErr assert.ErrorAssertionFunc
	}{{
		name: "get first",
		args: args{
			objId:    "obj1b",
			bucketId: "testBucket2",
		},
		obj:     "1st obj b",
		retr:    true,
		wantErr: assert.NoError,
	}, {
		name: "get middle",
		args: args{
			objId:    "ob02b",
			bucketId: "testBucket2",
		},
		obj:     "2nd ob b",
		retr:    true,
		wantErr: assert.NoError,
	}, {
		name: "get last",
		args: args{
			objId:    "o3-0bb",
			bucketId: "testBucket2",
		},
		obj:     "3rd obj in bucket b",
		retr:    true,
		wantErr: assert.NoError,
	}, {
		name: "bucket not found",
		args: args{
			objId:    "oo1b-",
			bucketId: "testBucket3",
		},
		wantErr: assert.NoError,
	}, {
		name: "obj not found",
		args: args{
			objId:    "objId",
			bucketId: "testBucket1",
		},
		wantErr: assert.NoError,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeTestBucket(tt.args.bucketId)
			defer os.Remove(tt.args.bucketId + ".dat")

			s, err := NewStore(".")
			if !assert.NoError(t, err) {
				return
			}

			obj, ok, err := s.Retrieve(tt.args.objId, tt.args.bucketId)
			if !tt.wantErr(t, err, fmt.Sprintf("Retrieve(%v, %v)", tt.args.objId, tt.args.bucketId)) {
				return
			}
			assert.Equalf(t, tt.obj, string(obj), "Retrieve(%v, %v)", tt.args.objId, tt.args.bucketId)
			assert.Equalf(t, tt.retr, ok, "Retrieve(%v, %v)", tt.args.objId, tt.args.bucketId)
		})
	}
}

func TestFileStore_Delete(t *testing.T) {
	type args struct {
		objId    string
		bucketId string
	}
	tests := []struct {
		name          string
		args          args
		deleted       bool
		wantErr       assert.ErrorAssertionFunc
		bucketContent string
		bucketObjects map[string]*objectMetadata
	}{{
		name:    "non existent bucket",
		args:    args{objId: "o1", bucketId: "bucketX"},
		wantErr: assert.NoError,
	}, {
		name:          "obj not found",
		args:          args{objId: "oo", bucketId: "testBucket1"},
		wantErr:       assert.NoError,
		bucketContent: "o1a 5 1stob\no2-a 12 2nd nice obj\no3.0a 7 3rd obj\n",
		bucketObjects: testBuckets["testBucket1"].bucketMetadata.objects,
	}, {
		name:          "remove last",
		args:          args{objId: "o3.0a", bucketId: "testBucket1"},
		wantErr:       assert.NoError,
		deleted:       true,
		bucketContent: "o1a 5 1stob\no2-a 12 2nd nice obj\n",
		bucketObjects: map[string]*objectMetadata{
			"o1a":  {offset: 0, size: 5, metaSize: 5},
			"o2-a": {offset: 12, size: 12, metaSize: 7},
		},
	}, {
		name:          "remove first",
		args:          args{objId: "obj1b", bucketId: "testBucket2"},
		wantErr:       assert.NoError,
		deleted:       true,
		bucketContent: "ob02b 8 2nd ob b\no3-0bb 19 3rd obj in bucket b\n",
		bucketObjects: map[string]*objectMetadata{
			"ob02b":  {offset: 0, size: 8, metaSize: 7},
			"o3-0bb": {offset: 17, size: 19, metaSize: 9},
		},
	}, {
		name:          "remove middle",
		args:          args{objId: "o2-a", bucketId: "testBucket1"},
		wantErr:       assert.NoError,
		deleted:       true,
		bucketContent: "o1a 5 1stob\no3.0a 7 3rd obj\n",
		bucketObjects: map[string]*objectMetadata{
			"o1a":   {offset: 0, size: 5, metaSize: 5},
			"o3.0a": {offset: 12, size: 7, metaSize: 7},
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeTestBucket(tt.args.bucketId)
			defer os.Remove(tt.args.bucketId + ".dat")

			s, err := NewStore(".")
			if !assert.NoError(t, err) {
				return
			}

			repl, err := s.Delete(tt.args.objId, tt.args.bucketId)

			if !tt.wantErr(t, err, fmt.Sprintf("Delete(%v, %v)", tt.args.objId, tt.args.bucketId)) {
				return
			}
			assert.Equalf(t, tt.deleted, repl, "Delete(%v, %v)", tt.args.objId, tt.args.bucketId)

			if tt.bucketContent != "" {
				bucketContent, err := os.ReadFile(tt.args.bucketId + ".dat")
				if err != nil {
					t.Errorf("Delete(%v, %v): cannot read produced bucket file: %v", tt.args.objId, tt.args.bucketId, err)
				}
				assert.Equalf(t, tt.bucketContent, string(bucketContent), "Delete(%v, %v)", tt.args.objId, tt.args.bucketId)
			} else {
				_, err = os.Open(tt.args.bucketId + ".dat")
				assert.Truef(t, errors.Is(err, os.ErrNotExist), "Delete(%v, %v)", tt.args.objId, tt.args.bucketId)
			}

			if tt.bucketObjects == nil {
				assert.NotContainsf(t, s.buckets, tt.args.bucketId, "Delete(%v, %v)", tt.args.objId, tt.args.bucketId)
			} else {
				assertObjsMetaEqualf(t, tt.bucketObjects, s.buckets[tt.args.bucketId].objects, "Delete(%v, %v)", tt.args.objId, tt.args.bucketId)
			}
		})
	}
}

func writeTestBucket(bucketId string) {
	if tb, ok := testBuckets[bucketId]; ok {
		// write bucket file
		f, _ := os.Create(tb.bucketMetadata.filePath)
		_, _ = f.Write([]byte(tb.bucketData))
		_ = f.Close()
	}
}

func assertBucketsMetaEqualf(t *testing.T, exp, bm *bucketMetadata, msg string, args ...interface{}) bool {
	if exp == nil && bm == nil {
		return true
	}

	if exp == nil {
		if !assert.Nilf(t, bm, msg+" Bucket metadata should be nil", args...) {
			return false
		}
	} else {
		if !assert.NotNilf(t, bm, msg+"Bucket metadata should not be nil", args...) {
			return false
		}
	}

	equal := assert.Equalf(t, exp.filePath, bm.filePath, msg, args...) && assert.Equalf(t, exp.muIndex, bm.muIndex, msg, args...)
	equal = equal && assertObjsMetaEqualf(t, exp.objects, bm.objects, msg, args...)

	return equal
}

func assertObjsMetaEqualf(t *testing.T, exp, osm map[string]*objectMetadata, msg string, args ...interface{}) bool {
	if exp == nil && osm == nil {
		return true
	}

	if exp == nil && !assert.Nilf(t, osm, msg+" Objects metadata should be nil", args...) {
		return false
	}

	if exp != nil && !assert.NotNilf(t, osm, msg+" Objects metadata should not be nil", args...) {
		return false
	}

	equal := true
	for objId, expObjMeta := range exp {
		if assert.Containsf(t, osm, objId, msg, args...) {
			om := osm[objId]
			equal = equal && assert.Equalf(t, expObjMeta.offset, om.offset, msg, args...) && assert.Equalf(t, expObjMeta.metaSize, om.metaSize, msg, args...) && assert.Equalf(t, expObjMeta.size, om.size, msg, args...)
		} else {
			equal = false
		}
	}

	return equal
}
