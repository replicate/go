package debug

import (
	"fmt"
	"mime"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"

	"github.com/replicate/go/logging"
)

const Addr = "localhost:7878"

var (
	logger = logging.New("debug")

	mux      http.ServeMux
	patterns []string
)

func init() {
	mux.HandleFunc("/", Index)

	HandleFunc("POST /disable", func(w http.ResponseWriter, _ *http.Request) {
		Enabled.Store(false)
		fmt.Fprintln(w, "debug mode disabled")
	})
	HandleFunc("/debug/pprof/", pprof.Index)
	HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	HandleFunc("/debug/pprof/profile", pprof.Profile)
	HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	HandleFunc("/debug/pprof/trace", pprof.Trace)
	HandleFunc("/log/level", logging.LevelHandler)

	s := &http.Server{
		Addr:    Addr,
		Handler: enabledMiddleware(&mux),
	}

	logger.Sugar().Infof("debug server listening on %s", Addr)
	go func() {
		if err := s.ListenAndServe(); err != http.ErrServerClosed {
			logger.Sugar().Errorw("debug server exited with error", "error", err)
		}
	}()
}

func Handle(pattern string, handler http.Handler) {
	patterns = append(patterns, pattern)
	mux.Handle(pattern, handler)
}

func HandleFunc(pattern string, handler http.HandlerFunc) {
	patterns = append(patterns, pattern)
	mux.HandleFunc(pattern, handler)
}

func Index(w http.ResponseWriter, r *http.Request) {
	wantHTML := false

	for _, t := range strings.Split(r.Header.Get("Accept"), ",") {
		mediatype, _, err := mime.ParseMediaType(t)
		if err == nil && mediatype == "text/html" {
			wantHTML = true
			break
		}
	}

	// If Accept is text/html, render HTML, otherwise render plain text.
	if wantHTML {
		for _, pattern := range patterns {
			fmt.Fprintf(w, "<a href=\"%s\">%s</a><br>", pattern, pattern)
		}
		return
	}

	fmt.Fprintf(w, "Available debug endpoints:\n\n")

	for _, pattern := range patterns {
		fmt.Fprintf(w, "  %s\n", pattern)
	}
}

func enabledMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if Enabled.Load() {
			next.ServeHTTP(w, r)
			return
		}

		http.Error(w, fmt.Sprintf("debug mode disabled (pid: %d)", os.Getpid()), http.StatusForbidden)
	})
}
