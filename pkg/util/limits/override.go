package limits

import (
	"os"
	"sync"
	"time"

	"github.com/go-kit/kit/log/level"
	yaml "gopkg.in/yaml.v2"

	"github.com/cortexproject/cortex/pkg/util"
)

// When we load YAML from disk, we want the various per-customer limits
// to default to any values specified on the command line, not default
// command line values.  This global contains those values.  I (Tom) cannot
// find a nicer way I'm afraid.
var defaultLimits limitTypes

// Overrides periodically fetch a set of per-user overrides, and provides convenience
// functions for fetching the correct value.
type Overrides struct {
	Defaults     Limits
	overridesMtx sync.RWMutex
	overrides    map[string]*limitTypes
	quit         chan struct{}
}

// NewOverrides makes a new Overrides.
// We store the supplied limits in a global variable to ensure per-tenant limits
// are defaulted to those values.  As such, the last call to NewOverrides will
// become the new global defaults.
func NewOverrides(defaults Limits) (*Overrides, error) {
	defaultLimits = defaults.limitTypes

	if defaults.PerTenantOverrideConfig == "" {
		level.Info(util.Logger).Log("msg", "per-tenant overides disabled")
		return &Overrides{
			Defaults:  defaults,
			overrides: map[string]*limitTypes{},
			quit:      make(chan struct{}),
		}, nil
	}

	overrides, err := loadOverrides(defaults.PerTenantOverrideConfig)
	if err != nil {
		return nil, err
	}

	o := &Overrides{
		Defaults:  defaults,
		overrides: overrides,
		quit:      make(chan struct{}),
	}

	go o.loop()
	return o, nil
}

func (o *Overrides) loop() {
	ticker := time.NewTicker(o.Defaults.PerTenantOverridePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			overrides, err := loadOverrides(o.Defaults.PerTenantOverrideConfig)
			if err != nil {
				level.Error(util.Logger).Log("msg", "failed to reload overrides", "err", err)
				continue
			}

			o.overridesMtx.Lock()
			o.overrides = overrides
			o.overridesMtx.Unlock()
		case <-o.quit:
			return
		}
	}
}

// Stop background reloading of overrides.
func (o *Overrides) Stop() {
	close(o.quit)
}

func loadOverrides(filename string) (map[string]*limitTypes, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var overrides struct {
		Overrides map[string]*limitTypes `yaml:"overrides"`
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	if err := decoder.Decode(&overrides); err != nil {
		return nil, err
	}

	return overrides.Overrides, nil
}

func (o *Overrides) getValue(limitName string, userID string) interface{} {
	o.overridesMtx.RLock()
	defer o.overridesMtx.RUnlock()
	override, ok := o.overrides[userID]
	if ok {
		value, ok := (*override)[limitName]
		if ok {
			return value
		}
	}
	return o.Defaults.limitTypes[limitName]
}

func (o *Overrides) GetBool(limitName string, userID string) bool {
	return o.getValue(limitName, userID).(bool)
}

func (o *Overrides) GetInt(limitName string, userID string) int {
	return o.getValue(limitName, userID).(int)
}

func (o *Overrides) GetDuration(limitName string, userID string) time.Duration {
	return o.getValue(limitName, userID).(time.Duration)
}
