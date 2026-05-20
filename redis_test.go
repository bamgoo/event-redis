package event_redis

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestParseRedisSetting(t *testing.T) {
	setting := parseRedisSetting(map[string]any{
		"timeout":      "250ms",
		"read_block":   2,
		"pending_idle": "3s",
		"batch":        "32",
		"max_attempts": float64(5),
		"retry_delay":  "150ms",
		"retry_limit":  "7",
		"dead_letter":  "event:dlq",
		"trim_max_len": "128",
	})

	if setting.Timeout != 250*time.Millisecond {
		t.Fatalf("unexpected timeout %v", setting.Timeout)
	}
	if setting.ReadBlock != 2*time.Second {
		t.Fatalf("unexpected read block %v", setting.ReadBlock)
	}
	if setting.PendingIdle != 3*time.Second {
		t.Fatalf("unexpected pending idle %v", setting.PendingIdle)
	}
	if setting.Batch != 32 {
		t.Fatalf("unexpected batch %d", setting.Batch)
	}
	if setting.MaxAttempts != 5 {
		t.Fatalf("unexpected max attempts %d", setting.MaxAttempts)
	}
	if setting.RetryDelay != 150*time.Millisecond {
		t.Fatalf("unexpected retry delay %v", setting.RetryDelay)
	}
	if setting.RetryLimit != 7 {
		t.Fatalf("unexpected retry limit %d", setting.RetryLimit)
	}
	if setting.DeadLetter != "event:dlq" {
		t.Fatalf("unexpected dead letter %q", setting.DeadLetter)
	}
	if setting.TrimMaxLen != 128 {
		t.Fatalf("unexpected trim max len %d", setting.TrimMaxLen)
	}
}

func TestRedisDeadLetterStreamTemplate(t *testing.T) {
	if got := deadLetterStream("event:dead", "publish.created"); got != "event:dead:publish.created" {
		t.Fatalf("unexpected dead letter stream %q", got)
	}
	if got := deadLetterStream("dead:{subject}", "publish.created"); got != "dead:publish.created" {
		t.Fatalf("unexpected templated dead letter stream %q", got)
	}
}

func TestMakeRetryTokens(t *testing.T) {
	if tokens := makeRetryTokens(0); tokens != nil {
		t.Fatal("expected nil tokens for disabled retry limit")
	}
	tokens := makeRetryTokens(1)
	if cap(tokens) != 1 {
		t.Fatalf("unexpected token capacity %d", cap(tokens))
	}
	tokens <- struct{}{}
	select {
	case tokens <- struct{}{}:
		t.Fatal("expected full retry token channel")
	default:
	}
}

func TestStreamAttempt(t *testing.T) {
	cases := []struct {
		name string
		msg  redis.XMessage
		want int
	}{
		{name: "missing", msg: redis.XMessage{Values: map[string]any{}}, want: 1},
		{name: "string", msg: redis.XMessage{Values: map[string]any{"attempt": "4"}}, want: 4},
		{name: "int64", msg: redis.XMessage{Values: map[string]any{"attempt": int64(7)}}, want: 7},
		{name: "invalid", msg: redis.XMessage{Values: map[string]any{"attempt": "bad"}}, want: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := streamAttempt(tc.msg); got != tc.want {
				t.Fatalf("attempt = %d, want %d", got, tc.want)
			}
		})
	}
}
