package taskrunner

import (
	"errors"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name   string
		cfg    Config
		wantOK bool
		kind   ErrorKind
	}{
		{"defaults", DefaultConfig(), true, ""},
		{"zero workers", Config{QueueSize: 1}, false, KindInvalidArgument},
		{"negative queue", Config{Workers: 1, QueueSize: -1}, false, KindInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			logCase(t, "ConfigValidate", tc.cfg, "err=%v", err)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				return
			}
			if err == nil || !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("expected ErrInvalidArgument, got %v", err)
			}
			if KindOf(err) != tc.kind {
				t.Fatalf("expected kind %s, got %s", tc.kind, KindOf(err))
			}
		})
	}
}

func TestErrorKindDistinction(t *testing.T) {
	// 四类对外失败原因必须可用 errors.Is / KindOf 相互区分。
	errs := []error{
		NewError(KindInvalidArgument, "bad key"),
		ErrIdempotencyConflict,
		NewError(KindNotFound, "missing"),
		NewError(KindOverloaded, "full"),
		ErrRunnerClosed,
		ErrTaskCanceled,
	}
	kinds := map[ErrorKind]bool{}
	for _, err := range errs {
		kinds[KindOf(err)] = true
	}
	logCase(t, "ErrorKindDistinction", kinds, "all kinds distinct=%v",
		len(kinds) == 6)
	if len(kinds) != 6 {
		t.Fatalf("expected 6 distinct kinds, got %d: %v", len(kinds), kinds)
	}
	// 带附加信息的错误实例仍与哨兵按 kind 匹配。
	if !errors.Is(NewError(KindOverloaded, "custom detail"), ErrOverloaded) {
		t.Fatal("errors.Is kind matching failed")
	}
}
