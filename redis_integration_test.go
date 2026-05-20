package event_redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisStreamIntegration(t *testing.T) {
	if os.Getenv("EVENT_REDIS_INTEGRATION") != "1" {
		t.Skip("set EVENT_REDIS_INTEGRATION=1 to run")
	}
	addr := os.Getenv("EVENT_REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping failed: %v", err)
	}

	stream := "event:test:" + time.Now().Format("20060102150405.000000000")
	group := "test"
	if err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		t.Fatalf("create group failed: %v", err)
	}
	defer client.Del(context.Background(), stream)

	if _, err := client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]any{"data": "body", "attempt": 1},
	}).Result(); err != nil {
		t.Fatalf("xadd failed: %v", err)
	}
	res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "consumer",
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    time.Second,
	}).Result()
	if err != nil {
		t.Fatalf("xreadgroup failed: %v", err)
	}
	if len(res) != 1 || len(res[0].Messages) != 1 {
		t.Fatalf("unexpected stream result: %+v", res)
	}
	msg := res[0].Messages[0]
	if got := streamAttempt(msg); got != 1 {
		t.Fatalf("attempt = %d", got)
	}
	claimed, _, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: "claimer",
		MinIdle:  0,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil {
		t.Fatalf("xautoclaim failed: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != msg.ID {
		t.Fatalf("unexpected claimed messages: %+v", claimed)
	}
	if err := client.XAck(ctx, stream, group, msg.ID).Err(); err != nil {
		t.Fatalf("xack failed: %v", err)
	}
	if _, err := client.XTrimMaxLenApprox(ctx, stream, 1, 0).Result(); err != nil {
		t.Fatalf("xtrim failed: %v", err)
	}
}
