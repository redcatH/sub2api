package service

import (
	"testing"
	"time"
)

func TestMonitorHub_NoSubscribers(t *testing.T) {
	h := NewMonitorHub()
	if h.HasSubscribers(1) {
		t.Fatal("expected no subscribers for a fresh hub")
	}
	if h.HasSubscribers(0) {
		t.Fatal("HasSubscribers(0) must be false")
	}
	// 向无订阅者的 key 发布不应 panic
	h.Publish(1, MonitorSnapshot{Body: "x"})
}

func TestMonitorHub_SubscribePublishReceive(t *testing.T) {
	h := NewMonitorHub()
	sub := h.Subscribe(42)
	defer h.Unsubscribe(42, sub)

	if !h.HasSubscribers(42) {
		t.Fatal("expected subscribers for key 42 after subscribe")
	}

	h.Publish(42, MonitorSnapshot{Body: "hello", Status: 200, Model: "m"})

	select {
	case snap := <-sub.Channel():
		if snap.Body != "hello" || snap.Status != 200 || snap.Model != "m" {
			t.Fatalf("unexpected snapshot: %+v", snap)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for snapshot")
	}
}

func TestMonitorHub_UnsubscribeStopsDelivery(t *testing.T) {
	h := NewMonitorHub()
	sub := h.Subscribe(7)
	h.Unsubscribe(7, sub)

	if h.HasSubscribers(7) {
		t.Fatal("expected no subscribers after unsubscribe")
	}

	h.Publish(7, MonitorSnapshot{Body: "x"})
	select {
	case snap := <-sub.Channel():
		t.Fatalf("unexpected snapshot after unsubscribe: %+v", snap)
	case <-time.After(50 * time.Millisecond):
		// ok: nothing delivered
	}

	select {
	case <-sub.Done():
		// ok: Done closed
	default:
		t.Fatal("expected Done() to be closed after unsubscribe")
	}
}

func TestMonitorHub_UnsubscribeIdempotent(t *testing.T) {
	h := NewMonitorHub()
	sub := h.Subscribe(8)
	h.Unsubscribe(8, sub)
	h.Unsubscribe(8, sub) // 二次 unsubscribe 不应 panic
}

func TestMonitorHub_DropOnFullReportsMissed(t *testing.T) {
	h := NewMonitorHub()
	sub := h.Subscribe(9)
	defer h.Unsubscribe(9, sub)

	// 填满缓冲
	for i := 0; i < MonitorChanBuffer; i++ {
		h.Publish(9, MonitorSnapshot{Body: "fill"})
	}
	// channel 满，这条被丢弃，missed 累加为 1
	h.Publish(9, MonitorSnapshot{Body: "overflow"})

	// 排空一条腾出空位，否则后续投递也会被丢
	<-sub.Channel()

	// 再发：应携带 Missed>=1
	h.Publish(9, MonitorSnapshot{Body: "after"})

	var maxMissed int
	remaining := MonitorChanBuffer // 剩余 fill(MonitorChanBuffer-1) + after(1)
	for i := 0; i < remaining; i++ {
		select {
		case s := <-sub.Channel():
			if s.Missed > maxMissed {
				maxMissed = s.Missed
			}
		case <-time.After(time.Second):
			t.Fatal("timed out reading snapshots")
		}
	}
	if maxMissed != 1 {
		t.Fatalf("expected a post-drop snapshot with Missed=1, max=%d", maxMissed)
	}
}
