package metrics

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"ydbcp/internal/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestMetricsCount(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.MetricsServerConfig{
		BindPort:    8080,
		BindAddress: "127.0.0.1",
	}
	p := NewMetricsRegistry(ctx, &wg, cfg)

	count := p.Factory().NewCounter(prometheus.CounterOpts{Name: "test_counter"})
	count.Add(123)

	repeat := 10
	for {
		res, err := http.Get("http://127.0.0.1:8080/metrics")
		if err != nil {
			if repeat > 0 {
				repeat--
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
		}
		assert.NoError(t, err)

		resBody, err := io.ReadAll(res.Body)
		assert.NoError(t, err)

		pattern := []byte("test_counter")
		val := 0
		for _, line := range bytes.Split(resBody, []byte("\n")) {
			if len(line) == 0 {
				continue
			}
			if line[0] == byte('#') {
				continue
			}

			i := bytes.Index(line, pattern)
			if i < 0 {
				continue
			}
			i += len(pattern)
			val, _ = strconv.Atoi(string(line[i+1:]))
			break
		}

		assert.Equal(t, val, 123)
		break
	}
	cancel()
	wg.Wait()
}
