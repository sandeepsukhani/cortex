package chunk

import (
	"context"

	"github.com/cortexproject/cortex/pkg/util/flagext"
)

func SetupTestDeleteStore() (*DeleteStore, error) {
	var deleteStoreConfig DeleteStoreConfig
	flagext.DefaultValues(&deleteStoreConfig)

	mockStorage := NewMockStorage()

	err := mockStorage.CreateTable(context.Background(), TableDesc{
		Name: deleteStoreConfig.RequestsTableName,
	})
	if err != nil {
		return nil, err
	}

	return NewDeleteStore(deleteStoreConfig, mockStorage)
}
