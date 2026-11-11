// Copyright 2026 The gVisor Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tcp

import (
	"bytes"
	"fmt"
	"testing"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/sync"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/faketime"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

// TestWritePayloadPieces checks that the chunks which hold the data of a write
// hold less than one smallest chunk more than the bytes of the data, which is
// what the send queue counts for the data.
func TestWritePayloadPieces(t *testing.T) {
	for _, n := range []int{1, 3, 63, 64, 65, 1025, 1460, 16385, 32767, 32769, 65536, 65537, 3*buffer.MaxChunkSize + 5} {
		t.Run(fmt.Sprintf("%d", n), func(t *testing.T) {
			data := make([]byte, n)
			for i := range data {
				data[i] = byte(i)
			}
			var ep Endpoint
			ep.mu.Lock()
			ep.sndQueueInfo.sndQueueMu.Lock()
			buf, err := ep.readFromPayloader(bytes.NewReader(data), tcpip.WriteOptions{Atomic: true}, n)
			ep.sndQueueInfo.sndQueueMu.Unlock()
			ep.mu.Unlock()
			if err != nil {
				t.Fatalf("readFromPayloader failed: %s", err)
			}
			defer buf.Release()
			if !bytes.Equal(buf.Flatten(), data) {
				t.Fatalf("readFromPayloader read other data")
			}
			held := 0
			views := buf.AsViewList()
			for v := views.Front(); v != nil; v = v.Next() {
				held += v.Capacity()
			}
			if extra := held - n; extra < 0 || extra >= minChunkSize {
				t.Errorf("chunks hold %d bytes for %d bytes of data, want less than %d more", held, n, minChunkSize)
			}
		})
	}
}

// TestSendMemUsedPurge checks that the memory count of the send queue returns
// to 0 when the write queue is purged, as on a reset or a close.
func TestSendMemUsedPurge(t *testing.T) {
	var ep Endpoint
	ep.mu.Lock()
	defer ep.mu.Unlock()
	ep.snd = &sender{ep: &ep}
	ep.snd.writeList.set = make(map[*segment]struct{})
	clock := faketime.NewManualClock()
	want := 0
	for _, n := range []int{3, 1025, 16385} {
		s := newOutgoingSegment(stack.TransportEndpointID{}, clock, buffer.MakeWithView(buffer.NewViewSize(n)), 0)
		s.setOwner(&ep, sendQ)
		ep.snd.writeList.PushBack(s)
		want += s.segMemSize()
	}
	if got := ep.sendMemUsed(); got != want {
		t.Fatalf("send memory used = %d, want %d", got, want)
	}

	ep.purgeWriteQueue()
	if got := ep.sendMemUsed(); got != 0 {
		t.Errorf("send memory used after purge = %d, want 0", got)
	}
}

func TestSendBufferStateSnapshot(t *testing.T) {
	var ep Endpoint
	var snapshot TCPSndBufState
	// Neither the initial nor updated endpoint state contains this value.
	snapshot.AutoTuneSndBufDisabled.Store(2)

	// The socket option callback does not take sndQueueMu. Join only after
	// both accesses so that the wait group cannot serialize them.
	var wg sync.WaitGroup
	wg.Go(func() {
		_ = ep.OnSetSendBufferSize(4096)
	})
	wg.Go(func() {
		ep.sndQueueInfo.sndQueueMu.Lock()
		ep.sndQueueInfo.CloneState(&snapshot)
		ep.sndQueueInfo.sndQueueMu.Unlock()
	})
	wg.Wait()

	ep.sndQueueInfo.sndQueueMu.Lock()
	sourceDisabled := ep.sndQueueInfo.AutoTuneSndBufDisabled.Load()
	ep.sndQueueInfo.sndQueueMu.Unlock()
	if sourceDisabled != 1 {
		t.Errorf("source AutoTuneSndBufDisabled = %d, want 1", sourceDisabled)
	}
	if got := snapshot.AutoTuneSndBufDisabled.Load(); got > 1 {
		t.Errorf("snapshot AutoTuneSndBufDisabled = %d, want 0 or 1", got)
	}
}
