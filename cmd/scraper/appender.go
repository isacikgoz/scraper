package main

import (
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/storage"
)

const maxAheadTime = 10 * time.Minute

type appendErrors struct {
	numOutOfOrder         int
	numDuplicates         int
	numOutOfBounds        int
	numExemplarOutOfOrder int
}

// appender returns an appender for ingested samples from the target.
func appender(app storage.Appender, sampleLimit, bucketLimit int) storage.Appender {
	app = &timeLimitAppender{
		Appender: app,
		maxTime:  timestamp.FromTime(time.Now().Add(maxAheadTime)),
	}

	// The sampleLimit is applied after metrics are potentially dropped via relabeling.
	if sampleLimit > 0 {
		app = &limitAppender{
			Appender: app,
			limit:    sampleLimit,
		}
	}

	if bucketLimit > 0 {
		app = &bucketLimitAppender{
			Appender: app,
			limit:    bucketLimit,
		}
	}
	return app
}

var (
	errSampleLimit = errors.New("sample limit exceeded")
	errBucketLimit = errors.New("histogram bucket limit exceeded")
)

// limitAppender limits the number of total appended samples in a batch.
type limitAppender struct {
	storage.Appender

	limit int
	i     int
}

func (app *limitAppender) Append(ref storage.SeriesRef, lset labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if !value.IsStaleNaN(v) {
		app.i++
		if app.i > app.limit {
			return 0, errSampleLimit
		}
	}
	ref, err := app.Appender.Append(ref, lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

type timeLimitAppender struct {
	storage.Appender

	maxTime int64
}

func (app *timeLimitAppender) Append(ref storage.SeriesRef, lset labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if t > app.maxTime {
		return 0, storage.ErrOutOfBounds
	}

	ref, err := app.Appender.Append(ref, lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

// bucketLimitAppender limits the number of total appended samples in a batch.
type bucketLimitAppender struct {
	storage.Appender

	limit int
}

func (app *bucketLimitAppender) AppendHistogram(ref storage.SeriesRef, lset labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	if h != nil {
		if len(h.PositiveBuckets)+len(h.NegativeBuckets) > app.limit {
			return 0, errBucketLimit
		}
	}
	if fh != nil {
		if len(fh.PositiveBuckets)+len(fh.NegativeBuckets) > app.limit {
			return 0, errBucketLimit
		}
	}
	ref, err := app.Appender.AppendHistogram(ref, lset, t, h, fh)
	if err != nil {
		return 0, err
	}
	return ref, nil
}
