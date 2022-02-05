package rest

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockStore struct {
	obj []byte
	ok  bool
	err error
}

func (s *mockStore) Store(obj []byte, _, _ string) (bool, error) {
	s.obj = obj
	return s.ok, s.err
}

func (s *mockStore) Retrieve(_, _ string) ([]byte, bool, error) {
	return s.obj, s.ok, s.err
}

func (s *mockStore) Delete(_, _ string) (bool, error) {
	s.obj = nil
	return s.ok, s.err
}

func TestHandler_HandleStore(t *testing.T) {
	tests := []struct {
		name       string
		obj        string
		store      *mockStore
		maxMem     int64
		statusCode int
	}{{
		name:       "createNew",
		obj:        "test obj",
		store:      &mockStore{},
		statusCode: http.StatusCreated,
	}, {
		name:       "replace",
		obj:        "test obj",
		store:      &mockStore{ok: true},
		statusCode: http.StatusOK,
	}, {
		name:       "errorStore",
		obj:        "test obj",
		store:      &mockStore{err: errors.New("store error")},
		statusCode: http.StatusInternalServerError,
	}, {
		name:       "noBody",
		store:      &mockStore{},
		statusCode: http.StatusBadRequest,
	}, {
		name:       "tooLarge",
		obj:        "not so long object",
		store:      &mockStore{},
		maxMem:     5,
		statusCode: http.StatusRequestEntityTooLarge,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRouter(tt.store, tt.maxMem, nil)

			var body io.Reader
			if tt.obj != "" {
				body = strings.NewReader(tt.obj)
			}

			req, _ := http.NewRequest("PUT", "/objects/bid/oid", body)
			req.Header.Set("Content-Type", "text/plain")

			res := executeRequest(req, r)
			assert.Equal(t, tt.statusCode, res.Code)
			if tt.statusCode < http.StatusBadRequest {
				assert.Equal(t, tt.obj, string(tt.store.obj))
			}
		})
	}
}

func TestHandler_HandleRetrieve(t *testing.T) {
	tests := []struct {
		name       string
		store      *mockStore
		statusCode int
	}{{
		name:       "retrieve",
		store:      &mockStore{obj: []byte("test obj"), ok: true},
		statusCode: http.StatusOK,
	}, {
		name:       "notFound",
		store:      &mockStore{},
		statusCode: http.StatusNotFound,
	}, {
		name:       "errorRetrieve",
		store:      &mockStore{err: errors.New("retrieve error")},
		statusCode: http.StatusInternalServerError,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRouter(tt.store, 0, nil)

			req, _ := http.NewRequest("GET", "/objects/bid/oid", nil)
			res := executeRequest(req, r)
			assert.Equal(t, tt.statusCode, res.Code)

			if tt.store.obj != nil {
				assert.Equal(t, string(tt.store.obj), res.Body.String())
			}
		})
	}
}

func TestHandler_HandleDelete(t *testing.T) {
	tests := []struct {
		name       string
		store      *mockStore
		statusCode int
	}{{
		name:       "delete",
		store:      &mockStore{obj: []byte("test obj"), ok: true},
		statusCode: http.StatusOK,
	}, {
		name:       "notFound",
		store:      &mockStore{},
		statusCode: http.StatusNotFound,
	}, {
		name:       "errorDelete",
		store:      &mockStore{err: errors.New("delete error")},
		statusCode: http.StatusInternalServerError,
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRouter(tt.store, 0, nil)

			req, _ := http.NewRequest("DELETE", "/objects/bid/oid", nil)
			res := executeRequest(req, r)
			assert.Equal(t, tt.statusCode, res.Code)

			assert.Empty(t, tt.store.obj)
		})
	}
}

func executeRequest(req *http.Request, r http.Handler) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)
	return rr
}
