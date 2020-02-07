package chunk

import (
	"context"
	"fmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

type DeleteRequestStatus string

const (
	Received     DeleteRequestStatus = "0"
	BuildingPlan DeleteRequestStatus = "1"
	Deleting     DeleteRequestStatus = "2"
	Processed    DeleteRequestStatus = "3"
)

// DeleteRequest holds all the details about a delete request
type DeleteRequest struct {
	RequestID string              `json:"request_id"`
	UserID    string              `json:"-"`
	StartTime model.Time          `json:"start_time"`
	EndTime   model.Time          `json:"end_time"`
	Selectors []string            `json:"selectors"`
	Status    string              `json:"status"`
	Matchers  [][]*labels.Matcher `json:"-"`
	CreatedAt model.Time          `json:"created_at"`
}

// DeleteStoreConfig holds configuration for delete store
type DeleteStoreConfig struct {
	Store             string `yaml:"store"`
	RequestsTableName string `yaml:"requests_table_name"`
	PlansTableName    string `yaml:"plans_table_name"`
}

type DeleteRequestsStore struct {
	DeleteRequestsSchema
	client DeleteRequestsStorageClient
}

func NewDeleteRequestsStore(client DeleteRequestsStorageClient) (*DeleteRequestsStore, error) {
	store := &DeleteRequestsStore{
		client:client,
	}
	store.GetDeleteRequestsForUserByStatus(context.Background())

	return store, nil
}

func (d DeleteRequestsStore) AddDeleteRequest(ctx context.Context, userID string, startTime, endTime model.Time, selectors []string) error {
	writeEntry := d.GetDeleteRequestWriteEntry(userID, startTime, endTime, selectors)
	return d.client.AddDeleteRequest(ctx, writeEntry)
}

func (d DeleteRequestsStore) GetDeleteRequestsForUserByStatus(ctx context.Context)  {
	readQuery := d.GetDeleteRequestsForUserByStatusQuery("fake", Received)
	fmt.Println(d.client.GetDeleteRequests(ctx, readQuery))
}
