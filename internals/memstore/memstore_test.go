package memstore

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestNewStore(t *testing.T) {
	s := NewStore()
	assert.IsType(t, sync.RWMutex{}, s.mu)
	assert.NotNil(t, s.buckets)
}

func Test_memStore_Store(t *testing.T) {
	type fields struct {
		buckets map[string]map[string]string
	}
	type args struct {
		obj      []byte
		objId    string
		bucketId string
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		replaced   bool
		bucketSize int
	}{{
		name:   "empty store",
		fields: fields{buckets: make(map[string]map[string]string)},
		args: args{
			[]byte("test obj"),
			"oid",
			"bid",
		},
		bucketSize: 1,
	}, {
		name: "non empty store",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid2": "old obj"}},
		},
		args: args{
			[]byte("test obj"),
			"oid",
			"bid",
		},
		bucketSize: 2,
	}, {
		name: "replace",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "old obj"}},
		},
		args: args{
			[]byte("test obj"),
			"oid",
			"bid",
		},
		replaced:   true,
		bucketSize: 1,
	}, {
		name: "replace",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "old obj"}},
		},
		args: args{
			[]byte("test obj"),
			"oid",
			"bid",
		},
		replaced:   true,
		bucketSize: 1,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := MemStore{
				buckets: tt.fields.buckets,
			}
			replaced, err := s.Store(tt.args.obj, tt.args.objId, tt.args.bucketId)
			assert.NoErrorf(t, err, "Store(%v, %v)", tt.args.objId, tt.args.bucketId)
			assert.Equalf(t, tt.replaced, replaced, "Store(%v, %v)", tt.args.objId, tt.args.bucketId)
			assert.Equalf(t, string(tt.args.obj), s.buckets[tt.args.bucketId][tt.args.objId], "Store(%v, %v)", tt.args.objId, tt.args.bucketId)
			assert.Lenf(t, s.buckets[tt.args.bucketId], tt.bucketSize, "Store(%v, %v)", tt.args.objId, tt.args.bucketId)
		})
	}
}

func Test_memStore_Retrieve(t *testing.T) {
	type fields struct {
		buckets map[string]map[string]string
	}
	type args struct {
		objId    string
		bucketId string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		obj       string
		retrieved bool
	}{{
		name: "ok",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "test obj"}},
		},
		args:      args{objId: "oid", bucketId: "bid"},
		obj:       "test obj",
		retrieved: true,
	}, {
		name: "empty store",
		fields: fields{
			buckets: make(map[string]map[string]string),
		},
		args: args{objId: "oid", bucketId: "bid"},
	}, {
		name: "obj not found",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "test obj"}},
		},
		args: args{objId: "oid1", bucketId: "bid"},
	}, {
		name: "bucket not found",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "test obj"}},
		},
		args: args{objId: "oid", bucketId: "bid1"},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := MemStore{
				buckets: tt.fields.buckets,
			}
			obj, retrieved, err := s.Retrieve(tt.args.objId, tt.args.bucketId)
			assert.NoErrorf(t, err, "Retrieve(%v, %v)", tt.args.objId, tt.args.bucketId)
			assert.Equalf(t, tt.retrieved, retrieved, "Retrieve(%v, %v)", tt.args.objId, tt.args.bucketId)
			assert.Equalf(t, tt.obj, string(obj), "Retrieve(%v, %v)", tt.args.objId, tt.args.bucketId)
		})
	}
}

func Test_memStore_Delete(t *testing.T) {
	type fields struct {
		buckets map[string]map[string]string
	}
	type args struct {
		objId    string
		bucketId string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		deleted     bool
		bucketsSize map[string]int
	}{{
		name: "emptied",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "test obj"}},
		},
		args:    args{objId: "oid", bucketId: "bid"},
		deleted: true,
	}, {
		name: "not emptied",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "test obj", "oid2": "test obj2"}, "bid1": {"oid": "test obj3"}},
		},
		args:        args{objId: "oid", bucketId: "bid"},
		deleted:     true,
		bucketsSize: map[string]int{"bid": 1, "bid1": 1},
	}, {
		name: "empty",
		fields: fields{
			buckets: make(map[string]map[string]string),
		},
		args: args{objId: "oid", bucketId: "bid"},
	}, {
		name: "obj not found",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "test obj"}},
		},
		args:        args{objId: "oid1", bucketId: "bid"},
		bucketsSize: map[string]int{"bid": 1},
	}, {
		name: "bucket not found",
		fields: fields{
			buckets: map[string]map[string]string{"bid": {"oid": "test obj"}},
		},
		args:        args{objId: "oid", bucketId: "bid1"},
		bucketsSize: map[string]int{"bid": 1},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := MemStore{
				buckets: tt.fields.buckets,
			}
			deleted, err := s.Delete(tt.args.objId, tt.args.bucketId)
			assert.NoErrorf(t, err, "Delete(%v, %v)", tt.args.objId, tt.args.bucketId)
			assert.Equalf(t, tt.deleted, deleted, "Delete(%v, %v)", tt.args.objId, tt.args.bucketId)
			if tt.bucketsSize == nil {
				assert.Empty(t, s.buckets)
			} else {
				for bucketId, bucketSize := range tt.bucketsSize {
					assert.Lenf(t, s.buckets[bucketId], bucketSize, "Store(%v, %v)", tt.args.objId, tt.args.bucketId)
					if bucketSize == 0 {
						// If bucket has been emptied, it should have been removed from the map
						assert.Zerof(t, s.buckets[bucketId], "Store(%v, %v)", tt.args.objId, tt.args.bucketId)
					}
				}
			}
		})
	}
}
