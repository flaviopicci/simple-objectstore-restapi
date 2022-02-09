package rest

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
)

// defaultMaxMem is the default maximum memory usable for an object read from the PUT request
const defaultMaxMem = 10 << 20 // 10MiB

// ObjectStore is the interface representing an object store. It exposes methods to manipulate stored objects.
//
// Store stores the given object as a bytes array using the provided object ID and bucket ID.
// It returns whether an object with the same ID has now been replaced along with any error encountered in the process.
//
// Retrieve retrieves the object identified by objId and bucketId. It returns the retrieved object or its null value,
// whether the object has been found in the storage, along with any error encountered in the process.
//
// Delete deletes the object identified by objId and bucketId. It returns whether the object was stored
// and it was actually deleted, along with any error encountered in the process.
//
// NOTE: both objId and bucketId will match this regex `[a-z0-9_-]+`
type ObjectStore interface {
	Store(obj []byte, objId, bucketId string) (bool, error)
	Retrieve(objId, bucketId string) ([]byte, bool, error)
	Delete(objId, bucketId string) (bool, error)
}

type storedResponse struct {
	Id string `json:"id"`
}

// Handler is the
type Handler struct {
	store  ObjectStore
	maxMem int64
}

func (h *Handler) HandleStore(w http.ResponseWriter, r *http.Request) {
	bucketId, objectId := getBucketObjectId(r)
	if ct := r.Header.Get("Content-Type"); ct != "text/plain" {
		http.Error(w, fmt.Sprintf("Content typwe %q not supported", ct), http.StatusUnsupportedMediaType)
		return
	}
	if r.Body == nil {
		http.Error(w, "Object content not set", http.StatusBadRequest)
		return
	}
	if r.ContentLength > h.maxMem {
		http.Error(w, "Object size exceeds maximum size of "+formatSizeBinary(h.maxMem), http.StatusRequestEntityTooLarge)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, h.maxMem))
	if err != nil {
		http.Error(w, "Cannot read object: "+err.Error(), http.StatusInternalServerError)
		return
	}

	replaced, err := h.store.Store(body, objectId, bucketId)
	if err != nil {
		http.Error(w, "Error storing object: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resBody, err := json.Marshal(storedResponse{Id: objectId})
	if err != nil {
		http.Error(w, "Error marshalling response"+err.Error(), http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")

	if replaced {
		// The exercise said to return a 201, but, in case the object was replaced,
		// a 200 might be a better status code
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	_, _ = w.Write(resBody)
}

func (h *Handler) HandleRetrieve(w http.ResponseWriter, r *http.Request) {
	bucketId, objectId := getBucketObjectId(r)
	obj, ok, err := h.store.Retrieve(objectId, bucketId)

	if err != nil {
		http.Error(w, "Error retrieving object: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		// The exercise said to return a 400 in case the object isn't found,
		// but a 404 might be a more precise status code in this case
		http.Error(w, fmt.Sprintf("Object %s/%s not found", bucketId, objectId), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write(obj)
}

func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	bucketId, objectId := getBucketObjectId(r)
	ok, err := h.store.Delete(objectId, bucketId)
	if err != nil {
		http.Error(w, "Error deleting object: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		http.Error(w, fmt.Sprintf("Object %s/%s not found", bucketId, objectId), http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func getBucketObjectId(r *http.Request) (string, string) {
	vars := mux.Vars(r)
	return vars["bucket"], vars["objectId"]
}

// formatSizeBinary formats a number representing a size in bytes to a string with binary unit
func formatSizeBinary(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d Bytes", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}
