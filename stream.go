package main

import (
	"io"
	"log"
	"net/http"
)

type flushWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

func (fw *flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if err == nil {
		fw.f.Flush()
	}
	return n, err
}

func streamResponseBody(w http.ResponseWriter, body io.ReadCloser, clientReq *http.Request, account string) (int64, error) {
	dst := io.Writer(w)
	if flusher, ok := w.(http.Flusher); ok {
		dst = &flushWriter{w: w, f: flusher}
	}
	n, err := io.Copy(dst, body)
	if err != nil {
		if clientReq != nil && clientReq.Context().Err() != nil {
			log.Printf("proxy: client disconnected during stream for %s (written=%d): %v", account, n, err)
		} else {
			log.Printf("proxy: upstream stream error for %s (written=%d): %v", account, n, err)
		}
		// Drain the upstream body so the account connection is released cleanly
		// even when the downstream client has already gone away.
		if _, drainErr := io.Copy(io.Discard, body); drainErr != nil {
			log.Printf("proxy: drain upstream body for %s after copy error: %v", account, drainErr)
		}
		return n, err
	}
	return n, nil
}