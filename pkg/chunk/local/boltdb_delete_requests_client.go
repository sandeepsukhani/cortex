package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/prometheus/common/model"
	"go.etcd.io/bbolt"
	"path"
	"strconv"
	"strings"
	"time"
)

var deleteRequestsBucketName = []byte("delete_requests")

type boltDeleteRequestsClient struct {
	cfg BoltDBConfig
	deleteRequestsDB *bbolt.DB
}

func NewBoltDBDeleteRequestsClient(cfg BoltDBConfig, deleteRequestsConfig chunk.DeleteStoreConfig) (chunk.DeleteRequestsStorageClient, error) {
	if err := ensureDirectory(cfg.Directory); err != nil {
		return nil, err
	}

	db, err := openDB(path.Join(cfg.Directory, deleteRequestsConfig.RequestsTableName))
	if err != nil {
		return nil, err
	}

	return &boltDeleteRequestsClient{
		deleteRequestsDB: db,
	}, nil
}

func (b *boltDeleteRequestsClient) AddDeleteRequest(ctx context.Context, deleteRequest chunk.DeleteRequestEntry) error {
	return b.deleteRequestsDB.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(deleteRequestsBucketName)
		if err != nil {
			return err
		}

		key := deleteRequest.UserID + separator + deleteRequest.RequestID
		if err := bucket.Put([]byte(key), []byte(deleteRequest.Status)); err != nil {
			return err
		}

		key = fmt.Sprintf("%s/%x:%x:%x", key, int64(deleteRequest.CreatedAt), int64(deleteRequest.StartTime), int64(deleteRequest.EndTime))
		return bucket.Put([]byte(key), []byte(strings.Join(deleteRequest.Selectors, "&")))
	})
}

func (b *boltDeleteRequestsClient) GetDeleteRequests(ctx context.Context, query chunk.DeleteRequestQuery) ([]chunk.DeleteRequestEntry, error) {
	prefix := []byte(nil)
	valueEqual := []byte(nil)

	if query.UserID != nil {
		prefixStr := *query.UserID + separator
		if query.RequestID != nil {
			prefixStr += *query.RequestID
		}
		prefix = []byte(prefixStr)
	}

	if query.Status != nil {
		valueEqual = []byte(*query.Status)
	}

	var deleteRequestEntries []chunk.DeleteRequestEntry

	err := b.deleteRequestsDB.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(deleteRequestsBucketName)
		if b == nil {
			return nil
		}

		c := b.Cursor()
		status := []byte(nil)

		for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
			if status == nil && len(valueEqual) > 0 && !bytes.Equal(v, valueEqual) {
				continue
			}

			if !bytes.HasPrefix(k, prefix) {
				break
			}

			if status == nil {
				status = v
				continue
			}

			deleteRequestEntry, err := parseDeleteRequestLookupResponse(k, v)
			if err != nil {
				return err
			}

			deleteRequestEntry.Status = chunk.DeleteRequestStatus(status)
			deleteRequestEntries = append(deleteRequestEntries, *deleteRequestEntry)
			status = nil
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return deleteRequestEntries, nil
}

func (b *boltDeleteRequestsClient) UpdateDeleteRequest(ctx context.Context, deleteRequestUpdate chunk.DeleteRequestEntryUpdate) error {
	panic("implement me")
}

func openDB(path string) (*bbolt.DB, error) {
	return bbolt.Open(path, 0666, &bbolt.Options{Timeout: 5 * time.Second})
}

func parseDeleteRequestLookupResponse(key []byte, val []byte) (*chunk.DeleteRequestEntry, error) {
	deleteRequestEntry := chunk.DeleteRequestEntry{}
	parts := strings.Split(string(key), "/")
	if len(parts) != 2 {
		return nil, errors.New("Invalid key in parsing delete request lookup response")
	}

	userAndRequestID := strings.SplitN(parts[0], separator, 2)
	deleteRequestEntry.UserID = userAndRequestID[0]
	deleteRequestEntry.RequestID = userAndRequestID[1]

	hexParts := strings.Split(parts[1], ":")
	if len(hexParts) != 3 {
		return nil, errors.New("Invalid key in parsing delete request lookup response")
	}

	createdAt, err := strconv.ParseInt(hexParts[1], 16, 64)
	if err != nil {
		return nil, err
	}

	from, err := strconv.ParseInt(hexParts[1], 16, 64)
	if err != nil {
		return nil, err
	}
	through, err := strconv.ParseInt(hexParts[2], 16, 64)
	if err != nil {
		return nil, err
	}

	deleteRequestEntry.CreatedAt = model.Time(createdAt)
	deleteRequestEntry.StartTime = model.Time(from)
	deleteRequestEntry.EndTime = model.Time(through)
	deleteRequestEntry.Selectors = strings.Split(string(val), "&")

	return &deleteRequestEntry, nil
}
