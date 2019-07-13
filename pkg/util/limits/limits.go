package limits

import (
	"flag"
	"time"
)

type LimitDefinition struct {
	Name string
	DefaultValue interface{}
	Usage string
}

type limitTypes map[string]interface{}

// Limits describe all the limits for users; can be used to describe global default
// limits via flags, or per-user limits via yaml config.
type Limits struct {
	limitTypes limitTypes

	// Config for overrides, convenient if it goes here.
	PerTenantOverrideConfig string        `yaml:"per_tenant_override_config"`
	PerTenantOverridePeriod time.Duration `yaml:"per_tenant_override_period"`
}

func New() *Limits {
	return &Limits{limitTypes: map[string]interface{}{}}
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (l *Limits) RegisterLimits(f *flag.FlagSet, limitDefinitions []LimitDefinition) {
	if l.limitTypes == nil {
		l.limitTypes = map[string]interface{}{}
	}

	for _, definition := range limitDefinitions {
		switch definition.DefaultValue.(type){
		case int:
			l.limitTypes[definition.Name] = *f.Int(definition.Name, definition.DefaultValue.(int), definition.Usage)
		case bool:
			l.limitTypes[definition.Name] = *f.Bool(definition.Name, definition.DefaultValue.(bool), definition.Usage)
		}
	}

	f.StringVar(&l.PerTenantOverrideConfig, "new_limits.per-user-override-config", "", "File name of per-user overrides.")
	f.DurationVar(&l.PerTenantOverridePeriod, "new_limits.per-user-override-period", 10*time.Second, "Period with this to reload the overrides.")
}
