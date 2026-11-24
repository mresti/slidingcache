package countminsketch_test

import (
	"fmt"
	"time"

	"github.com/mresti/slidingcache"
	"github.com/mresti/slidingcache/countminsketch"
)

// A rate limiter over a one-minute window. The estimate never undercounts, so
// comparing it with >= trips no later than the exact cache would.
func ExampleCache() {
	c, err := countminsketch.New(countminsketch.Config{
		Precision:  time.Second,
		WindowSize: time.Minute,
		EpochUnit:  slidingcache.EpochInSeconds,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = c.Close() }()

	const limit = 3
	for second := range int64(4) {
		count := c.Store(1_700_000_000+second, "client-42")
		fmt.Println("over the limit:", count >= limit)
	}
	// Get never moves the window; the next Store, of any key, does.
	c.Store(1_700_000_063, "client-7")
	fmt.Println("a minute later:", c.Get(1_700_000_063, "client-42"))
	fmt.Println("an epoch behind the window:", c.Store(1_700_000_000, "client-42") == slidingcache.LateEvent)
	// Output:
	// over the limit: false
	// over the limit: false
	// over the limit: true
	// over the limit: true
	// a minute later: 0
	// an epoch behind the window: true
}
