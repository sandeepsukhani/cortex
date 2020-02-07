package local

import (
	"context"
	"errors"
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/prometheus/common/model"
	"strconv"
	"strings"
)

type boltDBDeleteRequestsClient struct {
	*BoltIndexClient
	requestsTableName string
}

func NewBoltDBDeleteRequestsClient(cfg BoltDBConfig, requestsTableName string) (chunk.DeleteRequestsStorageClient, error) {
	boltIndexClient, err := NewBoltDBIndexClient(cfg)
	if err != nil {
		return nil, err
	}

	return &boltDBDeleteRequestsClient{
		BoltIndexClient: boltIndexClient,
		requestsTableName: requestsTableName,
	}, nil
}

func (b *boltDBDeleteRequestsClient) AddDeleteRequest(ctx context.Context, deleteRequest chunk.DeleteRequestEntry) error {
	writeBatch := b.NewWriteBatch()

	userIDAndRequestID := fmt.Sprintf("%s:%s", deleteRequest.UserID, deleteRequest.RequestID)

	// Add an entry with userID, requestID as range key and status as value to make it easy to manage and lookup status
	// We don't want to set anything in hash key here since we would want to find delete requests by just status
	writeBatch.Add(b.requestsTableName, "", []byte(userIDAndRequestID), []byte(deleteRequest.Status))

	// Add another entry with additional details like creation time, time range of delete request and selectors in value
	rangeValue := fmt.Sprintf("%x:%x:%x", int64(deleteRequest.CreatedAt), int64(deleteRequest.StartTime), int64(deleteRequest.EndTime))
	writeBatch.Add(b.requestsTableName, userIDAndRequestID, []byte(rangeValue), []byte(strings.Join(deleteRequest.Selectors, "&")))

	return b.BatchWrite(ctx, writeBatch)
}

func (b *boltDBDeleteRequestsClient) GetDeleteRequests(ctx context.Context, deleteRequestQuery chunk.DeleteRequestQuery) ([]chunk.DeleteRequestEntry, error) {
	var deleteRequestEntries []chunk.DeleteRequestEntry
	if deleteRequestQuery.UserID == nil && deleteRequestQuery.Status == nil {
		return deleteRequestEntries, errors.New("either of userID or status must included in delete request queries")
	}

	firstQuery := chunk.IndexQuery{}
	if deleteRequestQuery.UserID != nil && deleteRequestQuery.RequestID!= nil && deleteRequestQuery.Status != nil {
		deleteRequestEntries = append(deleteRequestEntries, chunk.DeleteRequestEntry{
			UserID: *deleteRequestQuery.UserID,
			RequestID: *deleteRequestQuery.RequestID,
			Status: *deleteRequestQuery.Status,
		})
	} else {
		firstQuery.TableName = b.requestsTableName
		if deleteRequestQuery.UserID != nil {
			if deleteRequestQuery.RequestID != nil {
				userIDAndRequestID := fmt.Sprintf("%s:%s", deleteRequestQuery.UserID, deleteRequestQuery.RequestID)
				firstQuery.RangeValuePrefix = []byte(userIDAndRequestID)
			} else {
				firstQuery.RangeValuePrefix = []byte(*deleteRequestQuery.UserID)
			}
		} else {
			firstQuery.ValueEqual = []byte(*deleteRequestQuery.Status)
		}
	}

	if len(deleteRequestEntries) == 0 {
		err := b.QueryPages(ctx, []chunk.IndexQuery{firstQuery}, func(query chunk.IndexQuery, batch chunk.ReadBatch) (shouldContinue bool) {
			itr := batch.Iterator()
			for itr.Next() {
				split := strings.Split(string(itr.RangeValue()), ":")
				deleteRequestEntries = append(deleteRequestEntries, chunk.DeleteRequestEntry{
					UserID: split[0],
					RequestID: split[1],
					Status: chunk.DeleteRequestStatus(itr.Value()),
				})
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}

	for i, deleteRequestEntry := range deleteRequestEntries {
		query := chunk.IndexQuery{
			TableName: b.requestsTableName,
			HashValue: fmt.Sprintf("%s:%s", deleteRequestEntry.UserID, deleteRequestEntry.RequestID),
		}

		var readError error
		err := b.QueryPages(ctx, []chunk.IndexQuery{query}, func(query chunk.IndexQuery, batch chunk.ReadBatch) (shouldContinue bool) {
			itr := batch.Iterator()
			itr.Next()
			itr.RangeValue()

			hexParts := strings.Split(string(itr.RangeValue()), ":")
			if len(hexParts) != 3 {
				readError = errors.New("invalid key in parsing delete request lookup response")
				return
			}

			createdAt, err := strconv.ParseInt(hexParts[0], 16, 64)
			if err != nil {
				readError = err
				return
			}

			from, err := strconv.ParseInt(hexParts[1], 16, 64)
			if err != nil {
				readError = err
				return
			}
			through, err := strconv.ParseInt(hexParts[2], 16, 64)
			if err != nil {
				readError = err
				return
			}

			deleteRequestEntry.CreatedAt = model.Time(createdAt)
			deleteRequestEntry.StartTime = model.Time(from)
			deleteRequestEntry.EndTime = model.Time(through)

			return
		})

		if err != nil {
			return nil, err
		}

		if readError != nil {
			return nil, readError
		}

		deleteRequestEntries[i] = deleteRequestEntry
	}

	return deleteRequestEntries, nil
}

func ( boltDBDeleteRequestsClient) UpdateDeleteRequest(ctx context.Context, deleteRequestUpdate chunk.DeleteRequestEntryUpdate) error {
	panic("implement me")
}
