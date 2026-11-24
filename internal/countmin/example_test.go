package countmin_test

import (
	"fmt"

	"github.com/mresti/slidingcache/internal/countmin"
)

// A window of two slots: events are counted into the current slot, and
// expiring a slot removes its events from the window estimate.
func ExampleRing() {
	ring, err := countmin.NewRing(2719, 5, 2)
	if err != nil {
		panic(err)
	}
	const older, newer = 0, 1
	const key = 0x9e3779b97f4a7c15 // a hash the caller computed once.

	ring.Add(older, key)
	ring.Add(older, key)
	estimate, _ := ring.Add(newer, key)
	fmt.Println("window:", estimate)

	ring.Expire(older)
	fmt.Println("after expiring the older slot:", ring.Count(key))
	// Output:
	// window: 3
	// after expiring the older slot: 1
}
