package chunk

import (
	"encoding/binary"
	"encoding/hex"
	"hash/fnv"
	"time"

	"github.com/prometheus/common/model"
)

type DeleteRequestEntry struct {
	UserID, RequestID  string
	CreatedAt, StartTime, EndTime model.Time
	Selectors          []string
	Status             DeleteRequestStatus
}

type DeleteRequestEntryUpdate struct {
	UserID, RequestID string
	Status            DeleteRequestStatus
}

type DeleteRequestQuery struct {
	TableName         string
	Status            *DeleteRequestStatus
	UserID, RequestID *string
}

type DeleteRequestsSchema struct {
}

func (DeleteRequestsSchema) GetDeleteRequestWriteEntry(userID string, startTime, endTime model.Time, selectors []string) DeleteRequestEntry {
	return DeleteRequestEntry{
		UserID:    userID,
		RequestID: generateUniqueDeleteRequestID(userID, selectors),
		CreatedAt: model.Now(),
		StartTime: startTime,
		EndTime:   endTime,
		Selectors: selectors,
		Status:    Received,
	}
}

func (DeleteRequestsSchema) GetDeleteRequestsForUserByStatusQuery(userID string, status DeleteRequestStatus) DeleteRequestQuery {
	return DeleteRequestQuery{
		UserID: &userID,
		Status: &status,
	}
}

func (DeleteRequestsSchema) GetDeleteRequestUpdateStatusEntry(userID, requestID string, status DeleteRequestStatus) DeleteRequestEntryUpdate {
	return DeleteRequestEntryUpdate{
		UserID:    userID,
		RequestID: requestID,
		Status:    status,
	}
}

// An id is useful in managing delete requests
func generateUniqueDeleteRequestID(userID string, selectors []string) string {
	uniqueID := fnv.New32()
	_, _ = uniqueID.Write([]byte(userID))

	timeNow := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeNow, uint64(time.Now().UnixNano()))
	_, _ = uniqueID.Write(timeNow)

	for _, selector := range selectors {
		_, _ = uniqueID.Write([]byte(selector))
	}

	return encodeUniqueID(uniqueID.Sum32())
}

func encodeUniqueID(t uint32) string {
	throughBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throughBytes, t)
	encodedThroughBytes := make([]byte, 8)
	hex.Encode(encodedThroughBytes, throughBytes)
	return string(encodedThroughBytes)
}
