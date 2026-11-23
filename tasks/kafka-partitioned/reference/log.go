// The in-memory log model: one logState per named log, touched only by
// its owning node, so the per-log lock only ever serializes concurrent
// handlers of that one node. Local reads and writes, the committed
// marker, and the offsets already given to relayed sends live here.

package main

import "sync"

// logState is one log, kept in the owner's memory. Only the owning node
// touches it, which is what keeps offsets monotonic without any
// compare-and-swap on the log data.
type logState struct {
	lock      sync.Mutex
	messages  []int
	committed int
	// given holds the offset each relayed send was given, by request id,
	// so a retried relay gets the same offset instead of a second copy.
	given map[string]int
}

// logs maps a log name to its state. Entries are never removed, so
// pointers into the map stay valid without the map lock.
var (
	logsLock sync.Mutex
	logs     = map[string]*logState{}
)

// stateFor returns (and creates) the state of a log.
func stateFor(name string) *logState {
	logsLock.Lock()
	state, ok := logs[name]
	if !ok {
		state = &logState{committed: -1, given: map[string]int{}}
		logs[name] = state
	}
	logsLock.Unlock()
	return state
}

// appendLocal appends a message to our own log and answers with its
// offset. Offsets are dense from zero, so a fresh message's offset is
// the last index, which only grows. A request id seen before gets the
// offset it was given then, and appends nothing.
func appendLocal(name string, value int, requestID string) int {
	state := stateFor(name)
	state.lock.Lock()
	if offset, seen := state.given[requestID]; seen && requestID != "" {
		state.lock.Unlock()
		return offset
	}
	state.messages = append(state.messages, value)
	offset := len(state.messages) - 1
	if requestID != "" {
		state.given[requestID] = offset
	}
	state.lock.Unlock()
	return offset
}

// pollLocal answers one group of poll offsets from our own state,
// leaving out keys with nothing at or after the requested offset.
func pollLocal(group map[string]int) map[string][][2]int {
	out := map[string][][2]int{}
	for name, rawFrom := range group {
		from := rawFrom
		if from < 0 {
			from = 0
		}

		state := stateFor(name)
		state.lock.Lock()
		if from < len(state.messages) {
			pairs := make([][2]int, 0, len(state.messages)-from)
			for i := from; i < len(state.messages); i++ {
				pairs = append(pairs, [2]int{i, state.messages[i]})
			}
			out[name] = pairs
		}
		state.lock.Unlock()
	}
	return out
}

// commitLocal moves a log's committed marker forward, but only ever
// upward: a client may commit an offset we already hold.
func commitLocal(name string, commitTo int) {
	state := stateFor(name)
	state.lock.Lock()
	if commitTo > state.committed {
		state.committed = commitTo
	}
	state.lock.Unlock()
}

// committedLocal answers which of the named logs we own are committed,
// leaving out keys we never committed.
func committedLocal(names []string) map[string]int {
	out := map[string]int{}
	for _, name := range names {
		state := stateFor(name)
		state.lock.Lock()
		if state.committed != -1 {
			out[name] = state.committed
		}
		state.lock.Unlock()
	}
	return out
}

// logKey maps a user log name to its store key.
func logKey(name string) string {
	return "log:" + name
}
