package chunk

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"

	"github.com/cortexproject/cortex/pkg/configs/legacy_promql"
	"github.com/cortexproject/cortex/pkg/util"
	intervals_util "github.com/cortexproject/cortex/pkg/util/intervals"
)

const tombstonesReloadDuration = 15 * time.Minute

// TombstonesSet holds all the pending delete requests for a user
type TombstonesSet struct {
	tombstones                               []DeleteRequest
	oldestTombstoneStart, newestTombstoneEnd model.Time // Used as optimization to find whether we want to iterate over tombstones or not
}

// tombstonesLoader loads delete requests and gen numbers from store and keeps checking for updates.
// It keeps checking for changes in gen numbers, which also means changes in delete requests and reloads specific users delete requests.
type tombstonesLoader struct {
	tombstones    map[string]*TombstonesSet
	tombstonesMtx sync.RWMutex

	deleteStore *DeleteStore
	quit        chan struct{}
}

// NewTombstonesLoader creates a tombstonesLoader
func NewTombstonesLoader(deleteStore *DeleteStore) TombstonesLoader {
	tl := tombstonesLoader{
		tombstones:  map[string]*TombstonesSet{},
		deleteStore: deleteStore,
	}
	go tl.loop()

	return &tl
}

// Stop stops tombstonesLoader
func (tl *tombstonesLoader) Stop() {
	close(tl.quit)
}

func (tl *tombstonesLoader) loop() {
	tombstonesReloadTimer := time.NewTicker(tombstonesReloadDuration)
	for {
		select {
		case <-tombstonesReloadTimer.C:
			err := tl.reloadTombstones()
			if err != nil {
				level.Error(util.Logger).Log("msg", "error reloading tombstones", "err", err)
			}
		case <-tl.quit:
			return
		}
	}
}

func (tl *tombstonesLoader) reloadTombstones() error {
	// check for updates in loaded gen numbers
	tl.tombstonesMtx.Lock()

	userIDs := make([]string, 0, len(tl.tombstones))
	for userID := range tl.tombstones {
		userIDs = append(userIDs, userID)
	}

	tl.tombstonesMtx.Unlock()

	// for all the updated gen numbers, reload delete requests
	for _, userID := range userIDs {
		err := tl.loadPendingTombstones(userID)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetPendingTombstones returns all pending tombstones
func (tl *tombstonesLoader) GetPendingTombstones(userID string) (TombstonesAnalyzer, error) {
	tl.tombstonesMtx.RLock()

	tombstoneSet, isOK := tl.tombstones[userID]
	if isOK {
		tl.tombstonesMtx.RUnlock()
		return tombstoneSet, nil
	}

	tl.tombstonesMtx.RUnlock()
	err := tl.loadPendingTombstones(userID)
	if err != nil {
		return nil, err
	}

	tl.tombstonesMtx.RLock()
	defer tl.tombstonesMtx.RUnlock()

	return tl.tombstones[userID], nil
}

func (tl *tombstonesLoader) loadPendingTombstones(userID string) error {
	pendingDeleteRequests, err := tl.deleteStore.GetPendingDeleteRequestsForUser(context.Background(), userID)
	if err != nil {
		return err
	}

	tombstoneSet := TombstonesSet{tombstones: pendingDeleteRequests, oldestTombstoneStart: model.Now()}
	for i := range tombstoneSet.tombstones {
		tombstoneSet.tombstones[i].Matchers = make([][]*labels.Matcher, len(tombstoneSet.tombstones[i].Selectors))

		for j, selector := range tombstoneSet.tombstones[i].Selectors {
			tombstoneSet.tombstones[i].Matchers[j], err = promql.ParseMetricSelector(selector)

			if err != nil {
				return err
			}
		}

		if tombstoneSet.tombstones[i].StartTime < tombstoneSet.oldestTombstoneStart {
			tombstoneSet.oldestTombstoneStart = tombstoneSet.tombstones[i].StartTime
		}

		if tombstoneSet.tombstones[i].EndTime > tombstoneSet.newestTombstoneEnd {
			tombstoneSet.newestTombstoneEnd = tombstoneSet.tombstones[i].EndTime
		}
	}

	tl.tombstonesMtx.Lock()
	defer tl.tombstonesMtx.Unlock()
	tl.tombstones[userID] = &tombstoneSet

	return nil
}

// GetDeletedIntervals returns non-overlapping, sorted  deleted intervals.
func (ts TombstonesSet) GetDeletedIntervals(labels labels.Labels, from, to model.Time) Intervals {
	if len(ts.tombstones) == 0 || to < ts.oldestTombstoneStart || from > ts.newestTombstoneEnd {
		return nil
	}

	var deletedIntervals []model.Interval

	for i := range ts.tombstones {
		overlaps, overlappingInterval := intervals_util.GetOverlappingInterval(model.Interval{Start: from, End: to},
			model.Interval{Start: ts.tombstones[i].StartTime, End: ts.tombstones[i].EndTime})

		if !overlaps {
			continue
		}

		matches := false
		for _, matchers := range ts.tombstones[i].Matchers {
			if util.CompareMatchersWithLabels(matchers, labels) {
				matches = true
				break
			}
		}

		if !matches {
			continue
		}

		if overlappingInterval.Start == from && overlappingInterval.End == to {
			// whole interval deleted
			return []model.Interval{overlappingInterval}
		}

		deletedIntervals = append(deletedIntervals, overlappingInterval)
	}

	if len(deletedIntervals) == 0 {
		return nil
	}

	return mergeIntervals(deletedIntervals)
}

// Len returns number of tombstones that are there
func (ts TombstonesSet) Len() int {
	return len(ts.tombstones)
}

// HasTombstonesForInterval tells whether there are any tombstones which overlapping given interval
func (ts TombstonesSet) HasTombstonesForInterval(from, to model.Time) bool {
	if to < ts.oldestTombstoneStart || from > ts.newestTombstoneEnd {
		return false
	}

	return true
}

// sorts and merges overlapping intervals
func mergeIntervals(intervals []model.Interval) []model.Interval {
	if len(intervals) <= 1 {
		return intervals
	}

	mergedIntervals := make([]model.Interval, 0, len(intervals))
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].Start < intervals[j].Start
	})

	ongoingTrFrom, ongoingTrTo := intervals[0].Start, intervals[0].End
	for i := 1; i < len(intervals); i++ {
		// if there is no overlap add it to mergedIntervals
		if intervals[i].Start > ongoingTrTo {
			mergedIntervals = append(mergedIntervals, model.Interval{Start: ongoingTrFrom, End: ongoingTrTo})
			ongoingTrFrom = intervals[i].Start
			ongoingTrTo = intervals[i].End
			continue
		}

		// there is an overlap but check whether existing time range is bigger than the current one
		if intervals[i].End > ongoingTrTo {
			ongoingTrTo = intervals[i].End
		}
	}

	// add the last time range
	mergedIntervals = append(mergedIntervals, model.Interval{Start: ongoingTrFrom, End: ongoingTrTo})

	return mergedIntervals
}

// Intervals represent a set of continuous non-overlapping intervals
type Intervals []model.Interval

// GetOverlappingIntervalsFor returns overlapping intervals for given time range. It clamps the interval to include only overlaps
func (intervals Intervals) GetOverlappingIntervalsFor(interval model.Interval) Intervals {
	overlappingIntervals := Intervals{}

	for i := range intervals {
		overlaps, overlappingInterval := intervals_util.GetOverlappingInterval(intervals[i], interval)
		if !overlaps {
			continue
		}

		overlappingIntervals = append(overlappingIntervals, overlappingInterval)
	}

	return overlappingIntervals
}
