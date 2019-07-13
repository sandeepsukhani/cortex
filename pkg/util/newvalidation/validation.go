package newvalidation

import (
	"flag"
	"github.com/cortexproject/cortex/pkg/util/limits"
)

var LimitsToRegister = []limits.LimitDefinition{
	{"limit1", true, "first limit with bool type"},
	{"limit2", 5, "second limit with int type"},
}

type Validation struct {
	overrides *limits.Overrides
	limits    *limits.Limits
}

func New(limits *limits.Limits) (*Validation, error) {
	validation := &Validation{
		limits:limits,
	}
	err := validation.loadOverrides()
	if err != nil {
		return nil, err
	}

	return validation, nil
}

func (v *Validation) RegisterFlags(f *flag.FlagSet) {
	v.limits.RegisterLimits(f, LimitsToRegister)
}

func (v *Validation) loadOverrides() error {
	var err error
	v.overrides, err = limits.NewOverrides(*v.limits)
	if err != nil {
		return err
	}

	return nil
}

func (v *Validation) GetLimit1(userID string) bool {
	return v.overrides.GetBool("limit1", userID)
}
