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
	"ydbcp/internal/types"

	"github.com/jonboulle/clockwork"

	"ydbcp/internal/config"

	io_prometheus_client "github.com/prometheus/client_model/go"
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
	InitializeMetricsRegistry(ctx, &wg, cfg, clockwork.NewFakeClock())

	s := GlobalMetricsRegistry.(*MetricsRegistryImpl)
	s.apiCallsCounter.WithLabelValues("test_service", "test_method", "test_status").Add(123)

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

		pattern := []byte("api_calls_count{method=\"test_method\",service=\"test_service\",status=\"test_status\"}")
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

func TestResetScheduleMetrics(t *testing.T) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.MetricsServerConfig{
		BindPort:    8080,
		BindAddress: "127.0.0.1",
	}
	InitializeMetricsRegistry(ctx, &wg, cfg, clockwork.NewFakeClock())
	s := GlobalMetricsRegistry.(*MetricsRegistryImpl)

	scheduleName := "test_schedule"
	schedule := types.BackupSchedule{
		ID:           "123",
		ContainerID:  "test_container",
		DatabaseName: "test_db",
		Name:         &scheduleName,
	}

	// set schedule metrics
	s.scheduleLastBackupTimestamp.WithLabelValues(
		schedule.ContainerID,
		schedule.DatabaseName,
		schedule.ID,
		scheduleName,
	).Set(float64(time.Now().Unix()))

	s.backupsSucceededCount.WithLabelValues(schedule.ContainerID, schedule.DatabaseName, schedule.ID, UNENCRYPTED_LABEL).Set(1)
	s.backupsFailedCount.WithLabelValues(schedule.ContainerID, schedule.DatabaseName, schedule.ID).Set(0)

	getMetricValue := func(metricFamilies []*io_prometheus_client.MetricFamily, familyName string) (float64, bool) {
		for _, mf := range metricFamilies {
			if mf.GetName() == familyName {
				var val float64
				for _, m := range mf.Metric {
					if m.Counter != nil {
						val += m.Counter.GetValue()
					}

					if m.Gauge != nil {
						val += m.Gauge.GetValue()
					}
				}
				return val, true
			}
		}
		return 0, false
	}

	// check metrics before ResetScheduleCounters
	metricFamilies, err := GlobalMetricsRegistry.GetReg().Gather()
	assert.Equal(t, nil, err)

	{ // check schedules_last_backup_timestamp
		_, found := getMetricValue(metricFamilies, "schedules_last_backup_timestamp")
		assert.Equal(t, true, found)
	}

	{ // check backups_succeeded_count (should be 1)
		val, found := getMetricValue(metricFamilies, "backups_succeeded_count")
		assert.Equal(t, true, found)
		assert.Equal(t, 1, int(val))
	}

	{ // check backups_succeeded_count (should be 0)
		val, found := getMetricValue(metricFamilies, "backups_failed_count")
		assert.Equal(t, true, found)
		assert.Equal(t, 0, int(val))
	}

	GlobalMetricsRegistry.ResetScheduleCounters(&schedule)

	// check metrics after ResetScheduleCounters
	metricFamilies, err = GlobalMetricsRegistry.GetReg().Gather()
	assert.Equal(t, nil, err)

	{ // check schedules_last_backup_timestamp (should be deleted)
		_, found := getMetricValue(metricFamilies, "schedules_last_backup_timestamp")
		assert.Equal(t, false, found)
	}

	{ // check backups_succeeded_count (should be deleted)
		_, found := getMetricValue(metricFamilies, "backups_succeeded_count")
		assert.Equal(t, false, found)
	}

	{ // check backups_succeeded_count (should be deleted)
		_, found := getMetricValue(metricFamilies, "backups_failed_count")
		assert.Equal(t, false, found)
	}

	cancel()
	wg.Wait()
}
