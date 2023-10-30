package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/prometheus/prometheus/model/textparse"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/scrape"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/spf13/cobra"
)

const (
	scrapeAcceptHeader = `application/openmetrics-text;version=1.0.0,application/openmetrics-text;version=0.0.1;q=0.75,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`
	// More than this many samples post metric-relabeling will cause the scrape to fail. 0 means no limit.
	sampleLimit = 0
	// More than this many buckets in a native histogram will cause the scrape to fail.
	bucketLimit = 0
	// Indicator whether the scraped timestamps should be respected.
	honorTimestamps = true
	// Option to enable the experimental in-memory metadata storage and append metadata to the WAL.
	enableMetadataStorage = false
)

var RootCmd = &cobra.Command{
	Use:   "scraper <path/to/tsdb> <http://scrapte_target>",
	Short: "Lightweight openmetrics scraper",
	RunE:  runCmdF,
	Args:  cobra.MinimumNArgs(2),
}

var (
	errBodySizeLimit            = errors.New("body size limit exceeded")
	UserAgent                   = fmt.Sprintf("Prometheus/%s", "1")
	bodySizeLimit         int64 = 10000000
	errNameLabelMandatory       = fmt.Errorf("missing metric name (%s label)", labels.MetricName)
	// returning an empty label set is interpreted as "drop"
	sampleMutator func(labels.Labels) labels.Labels
)

func main() {
	RootCmd.Flags().IntP("interval", "i", 60, "scrape interval in seconds")
	err := RootCmd.Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}

	fmt.Println("done.")
}

func runCmdF(cmd *cobra.Command, args []string) error {
	logger := log.NewLogfmtLogger(os.Stdout)

	db, err := tsdb.Open(args[0], logger, nil, &tsdb.Options{
		RetentionDuration:              int64(30 * 24 * time.Hour / time.Millisecond),
		AllowOverlappingCompaction:     true,
		EnableMemorySnapshotOnShutdown: true,
	}, nil)
	if err != nil {
		return fmt.Errorf("could not open target tsdb: %w", err)
	}
	defer db.Close()

	app := db.Appender(cmd.Context())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)
	interval, _ := cmd.Flags().GetInt("interval")

	cache := newScrapeCache()

	ls := labels.FromMap(map[string]string{
		model.AddressLabel:        "localhost:8067",
		model.ScrapeIntervalLabel: fmt.Sprintf("%ds", interval),
		model.ScrapeTimeoutLabel:  "10s",
	})
	lb := labels.NewBuilder(ls)

	lset, origLabels, err := scrape.PopulateLabels(lb, &config.ScrapeConfig{
		JobName: "prometheus",
	}, true)
	if err != nil {
		return err
	}

	target := scrape.NewTarget(lset, origLabels, url.Values{})

	sampleMutator = func(l labels.Labels) labels.Labels {
		return mutateSampleLabels(l, target, true, []*relabel.Config{})
	}

	ticker := time.Tick(time.Duration(interval) * time.Second)
	heartbeat := time.Tick(15 * time.Minute)

	go func() {
		for {
			select {
			case sig := <-sigs:
				fmt.Println(sig)
				done <- true
				return
			case <-heartbeat:
				logger.Log("status", "alive!")
			case <-ticker:
				buf := new(bytes.Buffer)
				content, err := scrapeFn(context.Background(), args[1], buf)
				if err != nil {
					panic(err)
				}

				_, _, _, err = appendFn(app, logger, cache, buf.Bytes(), content, time.Now().Round(0))
				if err != nil {
					app.Rollback()
					panic(err)
				}

				err = app.Commit()
				if err != nil {
					level.Error(logger).Log("msg", "scrape commit failed", "err", err)
				}
				app = db.Appender(cmd.Context())
			}
		}
	}()

	fmt.Println("awaiting interrupt signal...")
	<-done

	err = app.Commit()
	if err != nil {
		level.Error(logger).Log("msg", "final commit failed", "err", err)
	}

	return nil
}

func scrapeFn(ctx context.Context, url string, w io.Writer) (string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Add("Accept", scrapeAcceptHeader)
	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", strconv.FormatFloat(30, 'f', -1, 64)) // 30 seconds

	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer func() {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("server returned HTTP status %s", resp.Status)
	}

	if bodySizeLimit <= 0 {
		bodySizeLimit = math.MaxInt64
	}
	if resp.Header.Get("Content-Encoding") != "gzip" {
		n, err := io.Copy(w, io.LimitReader(resp.Body, bodySizeLimit))
		if err != nil {
			return "", err
		}
		if n >= bodySizeLimit {
			return "", errBodySizeLimit
		}
		return resp.Header.Get("Content-Type"), nil
	}

	buf := bufio.NewReader(resp.Body)
	gzipr, err := gzip.NewReader(buf)
	if err != nil {
		return "", err
	}

	n, err := io.Copy(w, io.LimitReader(gzipr, bodySizeLimit))
	gzipr.Close()
	if err != nil {
		return "", err
	}
	if n >= bodySizeLimit {
		return "", errBodySizeLimit
	}

	return resp.Header.Get("Content-Type"), nil
}

func appendFn(app storage.Appender, logger log.Logger, cache *scrapeCache, b []byte, contentType string, ts time.Time) (total, added, seriesAdded int, err error) {
	p, err := textparse.New(b, contentType, true)
	if err != nil {
		level.Debug(logger).Log(
			"msg", "Invalid content type on scrape, using prometheus parser as fallback.",
			"content_type", contentType,
			"err", err,
		)
	}

	var (
		defTime         = timestamp.FromTime(ts)
		appErrs         = appendErrors{}
		sampleLimitErr  error
		bucketLimitErr  error
		lset            labels.Labels     // escapes to heap so hoisted out of loop
		e               exemplar.Exemplar // escapes to heap so hoisted out of loop
		meta            metadata.Metadata
		metadataChanged bool
	)

	// updateMetadata updates the current iteration's metadata object and the
	// metadataChanged value if we have metadata in the scrape cache AND the
	// labelset is for a new series or the metadata for this series has just
	// changed. It returns a boolean based on whether the metadata was updated.
	updateMetadata := func(lset labels.Labels, isNewSeries bool) bool {
		if !enableMetadataStorage {
			return false
		}

		cache.metaMtx.Lock()
		defer cache.metaMtx.Unlock()
		metaEntry, metaOk := cache.metadata[lset.Get(labels.MetricName)]
		if metaOk && (isNewSeries || metaEntry.lastIterChange == cache.iter) {
			metadataChanged = true
			meta.Type = metaEntry.Type
			meta.Unit = metaEntry.Unit
			meta.Help = metaEntry.Help
			return true
		}
		return false
	}

	// Take an appender with limits.
	app = appender(app, sampleLimit, bucketLimit)

	defer func() {
		if err != nil {
			return
		}
		// Only perform cache cleaning if the scrape was not empty.
		// An empty scrape (usually) is used to indicate a failed scrape.
		cache.iterDone(len(b) > 0)
	}()

loop:
	for {
		var (
			et                       textparse.Entry
			sampleAdded, isHistogram bool
			met                      []byte
			parsedTimestamp          *int64
			val                      float64
			h                        *histogram.Histogram
			fh                       *histogram.FloatHistogram
		)
		if et, err = p.Next(); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			break
		}
		switch et {
		case textparse.EntryType:
			cache.setType(p.Type())
			continue
		case textparse.EntryHelp:
			cache.setHelp(p.Help())
			continue
		case textparse.EntryUnit:
			cache.setUnit(p.Unit())
			continue
		case textparse.EntryComment:
			continue
		case textparse.EntryHistogram:
			isHistogram = true
		default:
		}
		total++

		t := defTime
		if isHistogram {
			met, parsedTimestamp, h, fh = p.Histogram()
		} else {
			met, parsedTimestamp, val = p.Series()
		}
		if !honorTimestamps {
			parsedTimestamp = nil
		}
		if parsedTimestamp != nil {
			t = *parsedTimestamp
		}

		// Zero metadata out for current iteration until it's resolved.
		meta = metadata.Metadata{}
		metadataChanged = false

		if cache.getDropped(met) {
			continue
		}
		ce, ok := cache.get(met)
		var (
			ref  storage.SeriesRef
			hash uint64
		)

		if ok {
			ref = ce.ref
			lset = ce.lset

			// Update metadata only if it changed in the current iteration.
			updateMetadata(lset, false)
		} else {
			p.Metric(&lset)
			hash = lset.Hash()

			// Hash label set as it is seen local to the target. Then add target labels
			// and relabeling and store the final label set.
			lset = sampleMutator(lset)

			// The label set may be set to empty to indicate dropping.
			if lset.IsEmpty() {
				cache.addDropped(met)
				continue
			}

			if !lset.Has(labels.MetricName) {
				err = errNameLabelMandatory
				break loop
			}
			if !lset.IsValid() {
				err = fmt.Errorf("invalid metric name or label names: %s", lset.String())
				break loop
			}

			// TODO: check lable limits, skipping for now
			// If any label limits is exceeded the scrape should fail.
			// if err = verifyLabelLimits(lset, sl.labelLimits); err != nil {
			// 	break loop
			// }

			// Append metadata for new series if they were present.
			updateMetadata(lset, true)
		}

		if isHistogram {
			if h != nil {
				ref, err = app.AppendHistogram(ref, lset, t, h, nil)
			} else {
				ref, err = app.AppendHistogram(ref, lset, t, nil, fh)
			}
		} else {
			ref, err = app.Append(ref, lset, t, val)
		}
		sampleAdded, err = checkAddError(cache, ce, logger, met, parsedTimestamp, err, &sampleLimitErr, &bucketLimitErr, &appErrs)
		if err != nil {
			if err != storage.ErrNotFound {
				level.Debug(logger).Log("msg", "Unexpected error", "series", string(met), "err", err)
			}
			break loop
		}

		if !ok {
			if parsedTimestamp == nil {
				// Bypass staleness logic if there is an explicit timestamp.
				cache.trackStaleness(hash, lset)
			}
			cache.addRef(met, ref, lset, hash)
			if sampleAdded && sampleLimitErr == nil && bucketLimitErr == nil {
				seriesAdded++
			}
		}

		// Increment added even if there's an error so we correctly report the
		// number of samples remaining after relabeling.
		added++

		for hasExemplar := p.Exemplar(&e); hasExemplar; hasExemplar = p.Exemplar(&e) {
			if !e.HasTs {
				e.Ts = t
			}
			_, exemplarErr := app.AppendExemplar(ref, lset, e)
			exemplarErr = checkAddExemplarError(logger, exemplarErr, e, &appErrs)
			if exemplarErr != nil {
				// Since exemplar storage is still experimental, we don't fail the scrape on ingestion errors.
				level.Debug(logger).Log("msg", "Error while adding exemplar in AddExemplar", "exemplar", fmt.Sprintf("%+v", e), "err", exemplarErr)
			}
			e = exemplar.Exemplar{} // reset for next time round loop
		}

		if enableMetadataStorage && metadataChanged {
			if _, merr := app.UpdateMetadata(ref, lset, meta); merr != nil {
				// No need to fail the scrape on errors appending metadata.
				level.Debug(logger).Log("msg", "Error when appending metadata in scrape loop", "ref", fmt.Sprintf("%d", ref), "metadata", fmt.Sprintf("%+v", meta), "err", merr)
			}
		}
	}
	if sampleLimitErr != nil {
		if err == nil {
			err = sampleLimitErr
		}
	}
	if bucketLimitErr != nil {
		if err == nil {
			err = bucketLimitErr // If sample limit is hit, that error takes precedence.
		}
	}
	if appErrs.numOutOfOrder > 0 {
		level.Warn(logger).Log("msg", "Error on ingesting out-of-order samples", "num_dropped", appErrs.numOutOfOrder)
	}
	if appErrs.numDuplicates > 0 {
		level.Warn(logger).Log("msg", "Error on ingesting samples with different value but same timestamp", "num_dropped", appErrs.numDuplicates)
	}
	if appErrs.numOutOfBounds > 0 {
		level.Warn(logger).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "num_dropped", appErrs.numOutOfBounds)
	}
	if appErrs.numExemplarOutOfOrder > 0 {
		level.Warn(logger).Log("msg", "Error on ingesting out-of-order exemplars", "num_dropped", appErrs.numExemplarOutOfOrder)
	}
	if err == nil {
		cache.forEachStale(func(lset labels.Labels) bool {
			// Series no longer exposed, mark it stale.
			_, err = app.Append(0, lset, defTime, math.Float64frombits(value.StaleNaN))
			switch errors.Cause(err) {
			case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
				// Do not count these in logging, as this is expected if a target
				// goes away and comes back again with a new scrape loop.
				err = nil
			}
			return err == nil
		})
	}
	return
}
