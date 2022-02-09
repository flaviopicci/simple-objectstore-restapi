package rest

import (
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"net/http"
)

var logger *log.Logger

func NewRouter(s ObjectStore, maxMem int64, l *log.Logger) http.Handler {
	r := mux.NewRouter().PathPrefix("/objects").Subrouter()
	if l != nil {
		logger = l
		r.Use(loggingMiddleware)
	}

	if maxMem == 0 {
		maxMem = defaultMaxMem
	}
	h := Handler{
		store:  s,
		maxMem: maxMem,
	}
	r.HandleFunc("/{bucket:[a-z0-9_-]+}/{objectId:[a-z0-9_-]+}", h.HandleStore).Methods("PUT")
	r.HandleFunc("/{bucket:[a-z0-9_-]+}/{objectId:[a-z0-9_-]+}", h.HandleRetrieve).Methods("GET")
	r.HandleFunc("/{bucket:[a-z0-9_-]+}/{objectId:[a-z0-9_-]+}", h.HandleDelete).Methods("DELETE")

	return r
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lrw := NewLoggingResponseWriter(w)
		next.ServeHTTP(lrw, r)

		statusCode := lrw.statusCode

		level := log.DebugLevel
		if statusCode >= http.StatusBadRequest {
			level = log.WarnLevel
		}
		logger.Logf(level, "%d %s %s", statusCode, r.Method, r.URL.Path)
	})
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func NewLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{w, http.StatusOK}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}
