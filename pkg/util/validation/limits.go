package validation

import (
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	"gopkg.in/yaml.v2"
	"os"
	"sync"
	"time"
)

type LimitsUnmarshaler interface {
	UnmarshalYAML(unmarshal func(interface{}) error) error
}

type OverridesManager struct {
	perTenantOverridePeriod time.Duration
	perTenantOverrideConfig string
	defaults     LimitsUnmarshaler
	overrides    map[string]LimitsUnmarshaler
	overridesMtx sync.RWMutex
	quit         chan struct{}
}

func NewOverridesManager(perTenantOverridePeriod time.Duration, perTenantOverrideConfig string, defaults LimitsUnmarshaler) (*OverridesManager, error) {
	overridesManager := OverridesManager{
		perTenantOverrideConfig: perTenantOverrideConfig,
		perTenantOverridePeriod: perTenantOverridePeriod,
		defaults: defaults,
	}

	if perTenantOverrideConfig != "" {
		overridesManager.loop()
	} else {
		level.Info(util.Logger).Log("msg", "per-tenant overides disabled")
	}

	return &overridesManager, nil
}

func (lm *OverridesManager) loop() {
	ticker := time.NewTicker(lm.perTenantOverridePeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			overrides, err := loadOverrides(lm.perTenantOverrideConfig)
			if err != nil {
				overridesReloadSuccess.Set(0)
				level.Error(util.Logger).Log("msg", "failed to reload overrides", "err", err)
				continue
			}
			overridesReloadSuccess.Set(1)

			lm.overridesMtx.Lock()
			lm.overrides = overrides
			lm.overridesMtx.Unlock()
		case <-lm.quit:
			return
		}
	}
}

func (lm *OverridesManager) Stop() {
	close(lm.quit)
}

func (lm *OverridesManager) GetLimits(userID string) LimitsUnmarshaler {
	lm.overridesMtx.RLock()
	defer lm.overridesMtx.RUnlock()

	override, ok := lm.overrides[userID]
	if !ok {
		return lm.defaults
	}

	return override
}

func loadOverrides(filename string) (map[string]LimitsUnmarshaler, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	var overrides struct {
		Overrides map[string]LimitsUnmarshaler `yaml:"overrides"`
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)
	if err := decoder.Decode(&overrides); err != nil {
		return nil, err
	}

	return overrides.Overrides, nil
}
