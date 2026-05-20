package event_redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/infrago/event"
	"github.com/infrago/infra"
	"github.com/redis/go-redis/v9"
)

func init() {
	infra.Register("redis", &redisDriver{})
}

type (
	redisDriver struct{}

	redisConnection struct {
		mutex    sync.RWMutex
		running  bool
		client   *redis.Client
		instance *event.Instance
		setting  redisSetting

		pubsubs map[string]struct{}
		streams map[string]string

		subs        []*redis.PubSub
		done        chan struct{}
		root        context.Context
		stop        context.CancelFunc
		wg          sync.WaitGroup
		retryTokens chan struct{}
	}

	syncInstance interface {
		ServeSync(string, []byte) bool
	}

	redisSetting struct {
		Timeout     time.Duration
		ReadBlock   time.Duration
		PendingIdle time.Duration
		Batch       int64
		MaxAttempts int
		RetryDelay  time.Duration
		RetryLimit  int
		DeadLetter  string
		TrimMaxLen  int64
	}
)

const (
	redisDefaultTimeout     = 5 * time.Second
	redisDefaultReadBlock   = time.Second
	redisDefaultPendingIdle = 30 * time.Second
	redisDefaultBatchSize   = 16
	redisDefaultDeadLetter  = "event:dead"
)

func (d *redisDriver) Connect(inst *event.Instance) (event.Connection, error) {
	setting := inst.Config.Setting
	redisCfg := parseRedisSetting(setting)

	addr := "127.0.0.1:6379"
	host := ""
	port := "6379"
	if v, ok := setting["port"].(string); ok && v != "" {
		port = v
	}
	if v, ok := setting["server"].(string); ok && v != "" {
		host = v
	}
	if v, ok := setting["host"].(string); ok && v != "" {
		host = v
	}
	if host != "" {
		addr = host + ":" + port
	}
	if v, ok := setting["addr"].(string); ok && v != "" {
		addr = v
	}

	username, _ := setting["username"].(string)
	password, _ := setting["password"].(string)

	database := 0
	dbValue := setting["database"]
	if _, ok := setting["database"]; !ok {
		dbValue = setting["db"]
	}
	switch v := dbValue.(type) {
	case int:
		database = v
	case int64:
		database = int(v)
	case float64:
		database = int(v)
	case string:
		if vv, err := strconv.Atoi(v); err == nil {
			database = vv
		}
	}

	root, stop := context.WithCancel(context.Background())

	return &redisConnection{
		instance: inst,
		setting:  redisCfg,
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Username: username,
			Password: password,
			DB:       database,
		}),
		pubsubs:     make(map[string]struct{}, 0),
		streams:     make(map[string]string, 0),
		done:        make(chan struct{}),
		root:        root,
		stop:        stop,
		retryTokens: makeRetryTokens(redisCfg.RetryLimit),
	}, nil
}

func (c *redisConnection) Open() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	return c.client.Ping(ctx).Err()
}

func (c *redisConnection) Close() error {
	_ = c.Stop()
	return c.client.Close()
}

func (c *redisConnection) Register(name, group string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if group == "" {
		c.pubsubs[name] = struct{}{}
		return nil
	}

	stream := streamKey(name)
	c.streams[stream] = group

	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	err := c.client.XGroupCreateMkStream(ctx, stream, group, "$").Err()
	if err != nil && !isBusyGroup(err) {
		return err
	}
	return nil
}

func (c *redisConnection) Start() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.running {
		return nil
	}

	for subject := range c.pubsubs {
		ps := c.client.Subscribe(context.Background(), subject)
		ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
		if _, err := ps.Receive(ctx); err != nil {
			cancel()
			_ = ps.Close()
			return err
		}
		cancel()
		c.subs = append(c.subs, ps)
		ch := ps.Channel()

		c.wg.Add(1)
		go func(eventName string, msgCh <-chan *redis.Message) {
			defer c.wg.Done()
			for {
				select {
				case msg, ok := <-msgCh:
					if !ok {
						return
					}
					c.instance.Submit(func() {
						c.instance.Serve(eventName, []byte(msg.Payload))
					})
				case <-c.done:
					return
				}
			}
		}(subject, ch)
	}

	for stream, group := range c.streams {
		consumer := infra.Generate("event")

		c.wg.Add(1)
		go func(streamName, groupName, consumerName string) {
			defer c.wg.Done()
			claimStart := "0-0"

			for {
				select {
				case <-c.done:
					return
				default:
				}

				ctx, cancel := c.readContext()
				pending, nextStart, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
					Stream:   streamName,
					Group:    groupName,
					Consumer: consumerName,
					MinIdle:  c.setting.PendingIdle,
					Start:    claimStart,
					Count:    c.setting.Batch,
				}).Result()
				cancel()
				if err == nil {
					c.handleStreamMessages(streamName, groupName, pending)
					if nextStart == "0-0" || nextStart == "0" || nextStart == "" {
						claimStart = "0-0"
					} else {
						claimStart = nextStart
					}
				}

				ctx, cancel = c.readContext()
				res, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
					Group:    groupName,
					Consumer: consumerName,
					Streams:  []string{streamName, ">"},
					Count:    c.setting.Batch,
					Block:    c.setting.ReadBlock,
				}).Result()
				cancel()
				if err != nil {
					if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
						continue
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}

				for _, streamRes := range res {
					c.handleStreamMessages(streamRes.Stream, groupName, streamRes.Messages)
				}
			}
		}(stream, group, consumer)
	}

	c.running = true
	return nil
}

func (c *redisConnection) Stop() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if !c.running {
		return nil
	}

	close(c.done)
	c.stop()
	for _, sub := range c.subs {
		_ = sub.Close()
	}
	c.wg.Wait()
	c.root, c.stop = context.WithCancel(context.Background())
	c.subs = nil
	c.done = make(chan struct{})
	c.running = false
	return nil
}

func (c *redisConnection) Publish(name string, data []byte) error {
	if c.isPublishSubject(name) {
		stream := streamKey(name)
		ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
		defer cancel()
		_, err := c.client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]any{
				"data":    string(data),
				"attempt": 1,
			},
		}).Result()
		if err == nil {
			c.trimStream(stream)
		}
		c.trace("publish", name, err, map[string]any{"driver": "redis", "stream": stream, "bytes": len(data)})
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	err := c.client.Publish(ctx, name, data).Err()
	c.trace("publish", name, err, map[string]any{"driver": "redis", "bytes": len(data)})
	return err
}

func (c *redisConnection) handleStreamMessages(streamName, groupName string, messages []redis.XMessage) {
	for _, msg := range messages {
		raw, ok := msg.Values["data"].(string)
		if !ok {
			c.ackStreamMessage(streamName, groupName, msg.ID)
			continue
		}
		eventName := subjectFromStream(streamName)
		if serveEvent(c.instance, eventName, []byte(raw)) {
			c.ackStreamMessage(streamName, groupName, msg.ID)
			c.trace("ack", eventName, nil, map[string]any{"driver": "redis", "stream": streamName, "message": msg.ID, "attempt": streamAttempt(msg), "bytes": len(raw)})
		} else {
			c.handleFailedStreamMessage(streamName, groupName, msg)
		}
	}
}

func (c *redisConnection) ackStreamMessage(streamName, groupName, messageID string) {
	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	_ = c.client.XAck(ctx, streamName, groupName, messageID).Err()
}

func (c *redisConnection) handleFailedStreamMessage(streamName, groupName string, msg redis.XMessage) {
	eventName := subjectFromStream(streamName)
	attempt := streamAttempt(msg)

	if c.setting.MaxAttempts > 0 && attempt >= c.setting.MaxAttempts {
		err := c.publishDeadLetter(streamName, msg, attempt)
		c.trace("dead_letter", eventName, err, map[string]any{"driver": "redis", "stream": streamName, "message": msg.ID, "attempt": attempt})
		if err == nil {
			c.ackStreamMessage(streamName, groupName, msg.ID)
			c.trimStream(streamName)
		}
		return
	}

	if c.setting.MaxAttempts <= 0 && c.setting.RetryDelay <= 0 {
		c.trace("nak", eventName, nil, map[string]any{"driver": "redis", "stream": streamName, "message": msg.ID, "attempt": attempt})
		return
	}

	if c.setting.RetryDelay > 0 {
		c.scheduleRetry(streamName, groupName, msg, attempt+1)
		c.trace("retry", eventName, nil, map[string]any{"driver": "redis", "stream": streamName, "message": msg.ID, "attempt": attempt + 1, "delay": c.setting.RetryDelay.String()})
		return
	}

	err := c.requeueStreamMessage(streamName, msg, attempt+1)
	c.trace("retry", eventName, err, map[string]any{"driver": "redis", "stream": streamName, "message": msg.ID, "attempt": attempt + 1})
	if err == nil {
		c.ackStreamMessage(streamName, groupName, msg.ID)
	}
}

func (c *redisConnection) scheduleRetry(streamName, groupName string, msg redis.XMessage, attempt int) {
	if c.retryTokens != nil {
		select {
		case c.retryTokens <- struct{}{}:
		default:
			c.trace("retry_limited", subjectFromStream(streamName), nil, map[string]any{"driver": "redis", "stream": streamName, "message": msg.ID, "attempt": attempt})
			return
		}
	}
	c.wg.Add(1)
	timer := time.NewTimer(c.setting.RetryDelay)
	go func() {
		defer c.wg.Done()
		defer timer.Stop()
		if c.retryTokens != nil {
			defer func() { <-c.retryTokens }()
		}
		select {
		case <-c.done:
			return
		case <-timer.C:
		}
		err := c.requeueStreamMessage(streamName, msg, attempt)
		c.trace("retry_publish", subjectFromStream(streamName), err, map[string]any{"driver": "redis", "stream": streamName, "message": msg.ID, "attempt": attempt})
		if err == nil {
			c.ackStreamMessage(streamName, groupName, msg.ID)
		}
	}()
}

func (c *redisConnection) requeueStreamMessage(streamName string, msg redis.XMessage, attempt int) error {
	raw, ok := msg.Values["data"].(string)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	_, err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamName,
		Values: map[string]any{
			"data":    raw,
			"attempt": attempt,
		},
	}).Result()
	if err == nil {
		c.trimStream(streamName)
	}
	return err
}

func (c *redisConnection) publishDeadLetter(streamName string, msg redis.XMessage, attempt int) error {
	if c.setting.DeadLetter == "" {
		return nil
	}
	raw, _ := msg.Values["data"].(string)
	subject := subjectFromStream(streamName)
	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	dlq := deadLetterStream(c.setting.DeadLetter, subject)
	_, err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: dlq,
		Values: map[string]any{
			"data":     raw,
			"subject":  subject,
			"source":   streamName,
			"message":  msg.ID,
			"attempt":  attempt,
			"driver":   "redis",
			"datetime": time.Now().Unix(),
		},
	}).Result()
	if err == nil {
		c.trimStream(dlq)
	}
	return err
}

func (c *redisConnection) trimStream(streamName string) {
	if c.setting.TrimMaxLen <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.setting.Timeout)
	defer cancel()
	_ = c.client.XTrimMaxLenApprox(ctx, streamName, c.setting.TrimMaxLen, 0).Err()
}

func (c *redisConnection) readContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.root, c.setting.Timeout)
}

func (c *redisConnection) trace(operation, name string, err error, attrs map[string]any) {
	if attrs == nil {
		attrs = map[string]any{}
	}
	attrs["module"] = "event"
	attrs["operation"] = operation
	if err != nil {
		attrs["status"] = "error"
		attrs["error"] = err.Error()
	} else {
		attrs["status"] = "ok"
	}
	_ = infra.NewMeta().Trace("event:"+name, infra.TraceAttrs("infrago", infra.TraceKindEvent, name, attrs))
}

func isBusyGroup(err error) bool {
	if err == nil {
		return false
	}
	return len(err.Error()) >= 9 && err.Error()[:9] == "BUSYGROUP"
}

func (c *redisConnection) isPublishSubject(name string) bool {
	stream := streamKey(name)
	c.mutex.RLock()
	_, ok := c.streams[stream]
	c.mutex.RUnlock()
	if ok {
		return true
	}
	if c.instance != nil && c.instance.Config.Prefix != "" {
		name = strings.TrimPrefix(name, c.instance.Config.Prefix)
	}
	return strings.HasPrefix(name, "publish.")
}

func streamKey(subject string) string {
	return "event:stream:" + subject
}

func subjectFromStream(stream string) string {
	return strings.TrimPrefix(stream, "event:stream:")
}

func deadLetterStream(prefix, subject string) string {
	if strings.Contains(prefix, "{subject}") {
		return strings.ReplaceAll(prefix, "{subject}", subject)
	}
	return strings.TrimRight(prefix, ":") + ":" + subject
}

func streamAttempt(msg redis.XMessage) int {
	switch v := msg.Values["attempt"].(type) {
	case int:
		if v > 0 {
			return v
		}
	case int64:
		if v > 0 {
			return int(v)
		}
	case float64:
		if v > 0 {
			return int(v)
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n > 0 {
			return n
		}
	}
	return 1
}

func parseRedisSetting(setting map[string]any) redisSetting {
	return redisSetting{
		Timeout:     durationSetting(setting, "timeout", redisDefaultTimeout),
		ReadBlock:   durationSetting(setting, "read_block", redisDefaultReadBlock),
		PendingIdle: durationSetting(setting, "pending_idle", redisDefaultPendingIdle),
		Batch:       int64Setting(setting, "batch", redisDefaultBatchSize),
		MaxAttempts: intSetting(setting, "max_attempts", 0),
		RetryDelay:  durationSetting(setting, "retry_delay", 0),
		RetryLimit:  intSetting(setting, "retry_limit", 1024),
		DeadLetter:  stringSetting(setting, "dead_letter", redisDefaultDeadLetter),
		TrimMaxLen:  int64Setting(setting, "trim_max_len", 0),
	}
}

func makeRetryTokens(limit int) chan struct{} {
	if limit <= 0 {
		return nil
	}
	return make(chan struct{}, limit)
}

func durationSetting(setting map[string]any, key string, def time.Duration) time.Duration {
	switch v := setting[key].(type) {
	case time.Duration:
		if v >= 0 {
			return v
		}
	case int:
		if v >= 0 {
			return time.Duration(v) * time.Second
		}
	case int64:
		if v >= 0 {
			return time.Duration(v) * time.Second
		}
	case float64:
		if v >= 0 {
			return time.Duration(v * float64(time.Second))
		}
	case string:
		text := strings.TrimSpace(v)
		if text == "" {
			return def
		}
		if d, err := time.ParseDuration(text); err == nil && d >= 0 {
			return d
		}
		if n, err := strconv.Atoi(text); err == nil && n >= 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}

func intSetting(setting map[string]any, key string, def int) int {
	switch v := setting[key].(type) {
	case int:
		if v >= 0 {
			return v
		}
	case int64:
		if v >= 0 {
			return int(v)
		}
	case float64:
		if v >= 0 {
			return int(v)
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && n >= 0 {
			return n
		}
	}
	return def
}

func int64Setting(setting map[string]any, key string, def int64) int64 {
	n := intSetting(setting, key, int(def))
	if n <= 0 {
		return def
	}
	return int64(n)
}

func stringSetting(setting map[string]any, key, def string) string {
	if v, ok := setting[key].(string); ok {
		return strings.TrimSpace(v)
	}
	return def
}

func serveEvent(inst *event.Instance, name string, data []byte) bool {
	if inst == nil {
		return false
	}
	syncInst, ok := any(inst).(syncInstance)
	if !ok {
		return false
	}
	return syncInst.ServeSync(name, data)
}

var _ event.Connection = (*redisConnection)(nil)
