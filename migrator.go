package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
	"github.com/schollz/progressbar"
)

type migrator struct {
	src          *redis.Client
	dst          *redis.ClusterClient
	countScript  *redis.Script
	bar          *progressbar.ProgressBar
	transformer  KeyTransformer
	scanCount    int
	poolSize     int
	successCount uint64
	failureCount uint64
	restoreTTL   bool
	readOnly     bool
	stopCh       chan struct{}

	failedKeysMutex sync.Mutex
	failedKeys      []string
}

func (m *migrator) dumpFailed(file string) error {

	if len(m.failedKeys) == 0 {
		return nil
	}

	os.Remove(file)
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	for _, key := range m.failedKeys {
		if _, err := w.WriteString(key); err != nil {
			return err
		}
		if _, err := w.WriteString("\n"); err != nil {
			return err
		}
	}

	return nil
}

func (m *migrator) verify(key string) error {
	if key == "" {
		return nil
	}

	newKey, err := m.transformer.Transform(key)
	if err != nil {
		return err
	}

	result, err := m.dst.Exists(newKey).Result()
	if err != nil {
		return err
	}

	if result != 1 {
		return fmt.Errorf("key doesn't exist: %s", newKey)
	}

	return nil
}

func (m *migrator) migrate(key string) error {
	if key == "" {
		return nil
	}

	var ttl time.Duration
	var err error

	newKey, err := m.transformer.Transform(key)
	if err != nil {
		return err
	}

	if m.restoreTTL {
		ttl, err = m.src.PTTL(key).Result()
		if err != nil && err != redis.Nil {
			return err
		}
		// PTTL returns -2 if the key does not exist
		if err == redis.Nil || ttl == time.Duration(-2)*time.Millisecond {
			return nil
		}
		// PTTL returns -1 if the key exists but has no associated expire
		if ttl == time.Duration(-1)*time.Millisecond {
			ttl = 0
		}
	}

	value, err := m.src.Dump(key).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	if m.readOnly {
		return nil
	}

	if _, err = m.dst.RestoreReplace(newKey, ttl, value).Result(); err != nil {
		return err
	}

	return nil
}

func (m *migrator) Start(parentWg *sync.WaitGroup, verify bool) {

	defer parentWg.Done()

	iter := m.src.Scan(0, m.transformer.Pattern(), int64(m.scanCount)).Iterator()

	if n, err := m.countScript.Run(m.src, nil).Result(); err == nil {
		m.bar = progressbar.NewOptions64(n.(int64), progressbar.OptionShowCount())
		m.bar.RenderBlank()
	}

	wg := new(sync.WaitGroup)
	wg.Add(m.poolSize)

	for i := 0; i < m.poolSize; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case <-m.stopCh:
					return
				default:
				}
				if !iter.Next() {
					return
				}
				key := iter.Val()
				var err error
				if verify {
					err = m.verify(key)
				} else {
					err = m.migrate(key)
				}
				if m.bar != nil {
					m.bar.Add64(1)
				}
				if err != nil {
					atomic.AddUint64(&m.failureCount, 1)
					m.failedKeysMutex.Lock()
					m.failedKeys = append(m.failedKeys, key)
					m.failedKeysMutex.Unlock()
					log.Printf("Migration failed for key %s: %s", key, err.Error())
					continue
				}
				atomic.AddUint64(&m.successCount, 1)
			}

			if err := iter.Err(); err != nil {
				log.Printf("Scan iterator returned error: %s", err.Error())
				return
			}
		}()
	}

	wg.Wait()
}

func (m *migrator) Stop() {
	close(m.stopCh)
}

func NewMigrator(srcAddr, dstAddr string) (*migrator, error) {
	srcClient := redis.NewClient(&redis.Options{
		Addr: srcAddr,
		DB:   0, // use default DB
	})

	if _, err := srcClient.Ping().Result(); err != nil {
		return nil, err
	}

	dstClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{dstAddr},
	})

	if _, err := dstClient.Ping().Result(); err != nil {
		return nil, err
	}

	transformer := new(dndTransformer)

	// this is a bad idea, to use keys command which is blocking
	// putting it in lua script makes it slightly less bad :/
	countScript := fmt.Sprintf("return #redis.call('keys', '%s')", transformer.Pattern())

	return &migrator{
		src:         srcClient,
		dst:         dstClient,
		countScript: redis.NewScript(countScript),
		transformer: transformer,
		scanCount:   1000,
		poolSize:    50,
		restoreTTL:  true,
		readOnly:    false,
		stopCh:      make(chan struct{}),
	}, nil
}
