package http

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"

	"github.com/benbjohnson/litestream"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	ln          net.Listener
	server      *http.Server
	promHandler http.Handler

	DB *litestream.DB

	Addr string

	Logger *log.Logger
}

func NewServer(addr string) *Server {
	s := &Server{
		Addr:   addr,
		Logger: log.New(os.Stderr, "http: ", litestream.LogFlags),
	}

	s.promHandler = promhttp.Handler()
	s.server = &http.Server{
		Handler: http.HandlerFunc(s.serveHTTP),
	}
	return s
}

func (s *Server) Open() (err error) {
	if s.ln, err = net.Listen("tcp", s.Addr); err != nil {
		return err
	}
	go func() {
		err := s.server.Serve(s.ln)
		s.Logger.Printf("server shutting down: %s", err)
	}()
	return nil
}

func (s *Server) Close() (err error) {
	if s.ln != nil {
		err = s.ln.Close()
	}
	return err
}

func (s *Server) Port() int {
	if s.ln == nil {
		return 0
	}
	return s.ln.Addr().(*net.TCPAddr).Port
}

func (s *Server) URL() string {
	host, _, _ := net.SplitHostPort(s.Addr)
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s", net.JoinHostPort(host, fmt.Sprint(s.Port())))
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/metrics":
		s.promHandler.ServeHTTP(w, r)
	case "/stream":
		switch r.Method {
		case http.MethodGet:
			s.handleGetStream(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleGetStream(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// TODO: Read database path from query params & fetch DB reference.
	db := s.DB

	// Read position from query parameters, if generation is specified.
	pos := litestream.Pos{Generation: q.Get("generation")}
	if pos.Generation != "" {
		var err error
		if pos.Index, err = litestream.ParseIndex(q.Get("index")); err != nil {
			http.Error(w, "Invalid index", http.StatusBadRequest)
			return
		}

		if pos.Offset, err = litestream.ParseOffset(q.Get("offset")); err != nil {
			http.Error(w, "Invalid offset", http.StatusBadRequest)
			return
		}
	}

	// If no position specified, find current position and write a snapshot.
	if pos.IsZero() {
		if pos = db.Pos(); pos.IsZero() {
			http.Error(w, "No position available", http.StatusServiceUnavailable)
			return
		}

		// TODO: Write snapshot
	}

	s.Logger.Printf("stream connected @ %s", pos)
	defer s.Logger.Printf("stream disconnected")

	// Obtain an iterator from the database.
	itr, err := db.WALSegments(r.Context(), pos.Generation)
	if err != nil {
		http.Error(w, "Cannot obtain iterator", http.StatusInternalServerError)
		return
	}
	defer itr.Close()

	for {
		// Move to next WAL segment entry. If none exist, wait for DB to change.
		notify := db.Notify()
		if !itr.Next() {
			if err := itr.Err(); err != nil {
				s.Logger.Printf("wal iterator error, disconnecting: %s", err)
				return
			}

			select {
			case <-notify:
				continue
			case <-r.Context().Done():
				return
			}
		}

		// Check for generation change. Exit if changed.
		if pos.Generation != db.Pos().Generation {
			s.Logger.Printf("generation changed, disconnecting")
			return
		}

		info := itr.WALSegment()
		hdr := litestream.StreamRecordHeader{
			Type:       litestream.StreamRecordTypeWALSegment,
			Flags:      0,
			Generation: info.Generation,
			Index:      info.Index,
			Offset:     info.Offset,
			Size:       info.Size,
		}

		// Write record header.
		data, err := hdr.MarshalBinary()
		println("dbg/marshal", len(data))
		hexdump(data)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		} else if _, err := w.Write(data); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Copy WAL segment data to writer.
		if err := func() error {
			rd, err := db.WALSegmentReader(r.Context(), info.Pos())
			if err != nil {
				return fmt.Errorf("cannot fetch wal segment reader: %w", err)
			}
			defer rd.Close()
			println("dbg/wal", hdr.Size)

			if _, err := io.CopyN(w, rd, hdr.Size); err != nil {
				return fmt.Errorf("cannot copy wal segment: %w", err)
			}
			return nil
		}(); err != nil {
			log.Print(err)
			return
		}
	}
}

var _ litestream.StreamClient = (*StreamClient)(nil)

type StreamClient struct {
	URL url.URL

	// Underlying HTTP client
	HTTPClient *http.Client
}

func NewStreamClient() *StreamClient {
	return &StreamClient{}
}

func (c *StreamClient) Stream(ctx context.Context, pos litestream.Pos) (litestream.StreamReader, error) {
	if c.URL.Scheme != "http" && c.URL.Scheme != "https" {
		return nil, fmt.Errorf("invalid scheme")
	} else if c.URL.Host == "" {
		return nil, fmt.Errorf("host required")
	}

	// Strip off everything but the scheme & host.
	u := url.URL{
		Scheme: c.URL.Scheme,
		Host:   c.URL.Host,
		Path:   "/stream",
		RawQuery: (url.Values{
			"generation": {pos.Generation},
			"index":      {litestream.FormatIndex(pos.Index)},
			"offset":     {litestream.FormatOffset(pos.Offset)},
		}).Encode(),
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}

	return &StreamReader{
		body: resp.Body,
		file: io.LimitedReader{R: resp.Body},
	}, nil
}

type StreamReader struct {
	body io.ReadCloser
	file io.LimitedReader
	err  error
}

func (r *StreamReader) Close() error {
	if e := r.body.Close(); e != nil && r.err == nil {
		r.err = e
	}
	return r.err
}

func (r *StreamReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	} else if r.file.R == nil {
		return 0, io.EOF
	}
	return r.file.Read(p)
}

func (r *StreamReader) Next() (*litestream.StreamRecordHeader, error) {
	if r.err != nil {
		return nil, r.err
	}

	// If bytes remain on the current file, discard.
	if r.file.N > 0 {
		if _, r.err = io.Copy(io.Discard, &r.file); r.err != nil {
			return nil, r.err
		}
	}

	// Read record header.
	buf := make([]byte, litestream.StreamRecordHeaderSize)
	if _, err := io.ReadFull(r.body, buf); err != nil {
		r.err = fmt.Errorf("http.StreamReader.Next(): %w", err)
		return nil, r.err
	}

	var hdr litestream.StreamRecordHeader
	if r.err = hdr.UnmarshalBinary(buf); r.err != nil {
		return nil, r.err
	}

	// Update remaining bytes on file reader.
	r.file.N = hdr.Size

	return &hdr, nil
}

func hexdump(b []byte) { fmt.Println(hex.Dump(b)) }
