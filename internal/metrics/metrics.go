package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"ydbcp/internal/config"
	"ydbcp/internal/util/xlog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type MetricsRegistry interface {
	Factory() promauto.Factory
}

type MetricsRegistryImpl struct {
	server *http.Server
	reg    *prometheus.Registry
	cfg    config.MetricsServerConfig
}

func (s *MetricsRegistryImpl) Factory() promauto.Factory {
	return promauto.With(s.reg)
}

func NewMetricsRegistry(ctx context.Context, wg *sync.WaitGroup, cfg *config.MetricsServerConfig) *MetricsRegistryImpl {
	s := &MetricsRegistryImpl{
		reg: prometheus.NewRegistry(),
		cfg: *cfg,
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{Registry: s.reg}))

	s.server = &http.Server{
		Addr:     fmt.Sprintf("%s:%d", s.cfg.BindAddress, s.cfg.BindPort),
		Handler:  mux,
		ErrorLog: zap.NewStdLog(xlog.Logger(ctx)),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		xlog.Info(ctx, "Starting metrics server", zap.String("address", s.server.Addr))
		var err error
		if len(s.cfg.TLSCertificatePath) > 0 && len(s.cfg.TLSKeyPath) > 0 {
			err = s.server.ListenAndServeTLS(s.cfg.TLSCertificatePath, s.cfg.TLSKeyPath)
		} else {
			err = s.server.ListenAndServe()
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			xlog.Fatal(ctx, "metrics server failed to serve ", zap.Error(err))
		}
		xlog.Info(ctx, "metrics server stopped")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			xlog.Error(ctx, "metrics server shutdown error", zap.Error(err))
		}
	}()
	return s
}
