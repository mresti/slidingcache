package countmin_test

import (
	"fmt"

	"github.com/mresti/slidingcache/internal/countmin"
)

// A window of two slots: events are counted into the current slot through the
// aggregate, and expiring a slot removes its events from the window estimate.
func ExampleAggregate() {
	agg, err := countmin.NewAggregate(2719, 5)
	if err != nil {
		panic(err)
	}
	older, newer := agg.NewSlot(), agg.NewSlot()
	const key = 0x9e3779b97f4a7c15 // a hash the caller computed once.

	agg.Add(older, key)
	agg.Add(older, key)
	estimate, _ := agg.Add(newer, key)
	fmt.Println("window:", estimate)

	if err := agg.Sub(older); err != nil {
		panic(err)
	}
	older.Reset()
	fmt.Println("after expiring the older slot:", agg.Count(key))
	// Output:
	// window: 3
	// after expiring the older slot: 1
}
