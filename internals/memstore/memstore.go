package memstore

import "sync"

// MemStore implements ObjectStore and stores objects in memory.
// It stores the objects as strings in a matrix [bucketId][objId].
// Storing, retrieving and deletion time are O(1)
type MemStore struct {
	mu      sync.RWMutex                 // Mutex used to modify the buckets data
	buckets map[string]map[string]string // Map where objects are actually stored
}

func NewStore() *MemStore {
	return &MemStore{
		buckets: make(map[string]map[string]string),
	}
}

func (s *MemStore) Store(obj []byte, objId, bucketId string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, ok := s.buckets[bucketId]
	if !ok {
		bucket = make(map[string]string)
		s.buckets[bucketId] = bucket
	}

	_, ok = bucket[objId]
	bucket[objId] = string(obj)

	return ok, nil
}

func (s *MemStore) Retrieve(objId, bucketId string) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	bucket, ok := s.buckets[bucketId]
	if !ok {
		return nil, false, nil
	}

	obj, ok := bucket[objId]
	if !ok {
		return nil, false, nil
	}

	return []byte(obj), true, nil
}

func (s *MemStore) Delete(objId, bucketId string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucket, ok := s.buckets[bucketId]
	if !ok {
		return false, nil
	}

	_, ok = bucket[objId]
	if !ok {
		return false, nil
	}
	delete(bucket, objId)

	// if bucket has been emptied, delete it
	if len(bucket) == 0 {
		delete(s.buckets, bucketId)
	}

	return true, nil
}
