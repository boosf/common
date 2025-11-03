package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"sort"
	"time"

	"github.com/boosf/common/pkg/clock"
)

func New(clockClient clock.Client) *HashRing {
	return &HashRing{
		clockClient: clockClient,
		keys:        []int64{},
		keyData:     map[int64]*keyData{},
	}
}

type HashRing struct {
	clockClient clock.Client
	keys        []int64
	keyData     map[int64]*keyData
}

type keyData struct {
	partition string
	expiresAt *time.Time
}

type serializedHashRing struct {
	Keys    []int64                      `json:"keys"`
	KeyData map[int64]*serializedKeyData `json:"keyData"`
}

type serializedKeyData struct {
	Partition string     `json:"partition"`
	ExpiresAt *time.Time `json:"expiresAt"`
}

func (h *HashRing) Add(partition string, expiresAt *time.Time) {
	hash := h.getHash(partition)
	out, ok := h.keyData[hash]
	if ok {
		out.expiresAt = expiresAt
		return
	}
	h.keys = append(h.keys, hash)
	h.keyData[hash] = &keyData{
		expiresAt: expiresAt,
		partition: partition,
	}
	slices.Sort(h.keys)
}

func (h *HashRing) Get(keys []string) ([]string, error) {
	h.removeExpiredKeys()
	if len(h.keys) == 0 {
		return nil, errors.New("no valid keys")
	}
	partitions := []string{}
	for _, key := range keys {
		hash := h.getHash(key)
		idx := sort.Search(len(h.keys), func(i int) bool { return h.keys[i] >= hash })
		if idx >= len(h.keys) {
			idx = 0
		}
		partition := h.keyData[h.keys[idx]].partition
		partitions = append(partitions, partition)
	}
	return partitions, nil
}

func (h *HashRing) MarshalJSON() ([]byte, error) {
	serialized := &serializedHashRing{}
	serialized.Keys = h.keys
	serialized.KeyData = map[int64]*serializedKeyData{}
	for key, data := range h.keyData {
		serialized.KeyData[key] = &serializedKeyData{
			Partition: data.partition,
			ExpiresAt: data.expiresAt,
		}
	}
	out, err := json.Marshal(serialized)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal serialized, err=%w", err)
	}
	return out, nil
}

func (h *HashRing) UnmarshalJSON(data []byte) error {
	serialized := &serializedHashRing{}
	if err := json.Unmarshal(data, serialized); err != nil {
		return fmt.Errorf("failed to unmarshal serialized, err=%w", err)
	}
	h.keys = serialized.Keys
	outKeyData := map[int64]*keyData{}
	for key, data := range serialized.KeyData {
		outKeyData[key] = &keyData{
			partition: data.Partition,
			expiresAt: data.ExpiresAt,
		}
	}
	h.keyData = outKeyData
	return nil
}

func (h *HashRing) removeExpiredKeys() {
	now := h.clockClient.UnixNow()
	keys := []int64{}
	keyMap := map[int64]*keyData{}
	for _, key := range h.keys {
		expiresAt := h.keyData[key].expiresAt
		if expiresAt == nil || expiresAt.Unix() > now {
			keys = append(keys, key)
			keyMap[key] = h.keyData[key]
		}
	}
	h.keys = keys
	h.keyData = keyMap
}

func (h *HashRing) getHash(data string) int64 {
	return int64(crc32.ChecksumIEEE([]byte(data)))
}
