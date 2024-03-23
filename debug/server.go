package debug

import (
	"context"
	"net/http"
	"net/http/pprof"
	"sync"

	"github.com/replicate/go/logging"
)

var logger = logging.New("debug")

var defaultServeMux http.ServeMux

func init() {
	defaultServeMux.HandleFunc("/debug/pprof/", pprof.Index)
	defaultServeMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	defaultServeMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	defaultServeMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	defaultServeMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

type Server struct {
	Addr string

	once   sync.Once
	server *http.Server
}

func (s *Server) init() {
	s.server = &http.Server{
		Addr:    s.Addr,
		Handler: &defaultServeMux,
	}
}

func (s *Server) ListenAndServe() error {
	s.once.Do(s.init)

	logger.Sugar().Infow("starting debug http server", "address", s.Addr)
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.once.Do(s.init)

	return s.server.Shutdown(ctx)
}
