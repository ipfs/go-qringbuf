// Package qringbuf provides a concurrency-friendly zero-copy abstraction of
// io.ReadAtLeast() over a pre-allocated ring-buffer, populated asynchronously
// by a standalone goroutine. It is primarily designed for processing a series
// of arbitrary streams each comprised of variable-length records.
//
// Specifically an object of this package makes the following guarantees:
//  • Memory is allocated only at construction time, never during streaming
//  • StartFill() spawns off a single goroutine (collector) which terminates when:
//    ◦ It reaches readLimit, if one was supplied
//    ◦ It receives any error from the wrapped reader (including io.EOF)
//    ◦ It had a chance to evaluate a signal received from StopFill()
//      (most io.Read() operations are not cancelable, and may block forever)
//  • Every call to NextRegion() blocks until it can return:
//    ◦ A *Region object representing a contiguous slice of at least MinRegion bytes
//    ◦ A smaller "trailing" *Region and an io.EOF or ErrCollectorStopped
//    ◦ A nil-Region and any other error
//  • *Region.Bytes() is always a slice of the underlying buffer, no data copying takes place
//  • Data backing a *Region is guaranteed to remain intact provided:
//    ◦ NextRegion() has not been called again allowing writes into the buffer
//    ◦ *Region.Reserve() was invoked, which blocks writes until a *Region.Release()
//
// Examples
//
// In code the basic usage looks roughly like this:
//
//  qrb, initErr := qringbuf.NewFromReader( *someReader, qringbuf.Config{ … } )
//  …
//  readLimit = int64( stopReadingAfterThatManyBytes )
//  startErr := qrb.StartFill( readLimit )
//  …
//  var available, processed int
//  for {
//      reg, streamErr := qrb.NextRegion( available - processed )
//      if reg == nil {
//          return streamErr
//      }
//
//      // work with region, processing all or just a portion of the data
//      available = reg.Size()
//      processed = frobnicate( reg.Bytes(), … )
//  }
//
// In addition one can operate over individual (sub)regions with "fearless
// concurrency":
//
//  …
//  var available, processed int
//  for {
//      reg, streamErr := qrb.NextRegion( available - processed )
//      if reg == nil {
//          return streamErr
//      }
//
//      available = reg.Size()
//      processed = 256
//
//      subReg := reg.SubRegion( 0, 256 )
//      subReg.Reserve()
//
//      go func() {
//          frobnicate( subReg.Bytes(), … )
//          subReg.Release()
//      }()
//  }
//
// In all cases the background collector goroutine reading from *someReader
// into the ring buffer is guaranteed to:
//  - never overwrite the buffer portion backing the latest result of NextRegion()
//  - never overwrite any buffer portion backing a Reserve()d (Sub)Region
//
// Implementation notes
//
// Unlike io.ReadAtLeast, errors from the underlying reader are always made
// available on NextRegion(). As with the standard io.* semantics an error can
// be returned together with a result. One should always check whether the
// *Region return value is nil first, before processing the error,
//
// Changes of the emitter and collector positions are protected by a mutex on
// the qringbuf object. Any call accessing the buffer state will block until
// this lock can be obtained. One can also lock the mutex if the collector
// goroutine needs to be paused for whatever reason.
//
// The region Reserve()/Release() functionality does not use the mutex, ensuring
// that an asynchronous Release() call can not be affected by the current state
// of the buffer. Reservation tracking is implemented as an atomically modified
// list of reservation counts, one int32 per SectorSize bytes of the buffer.
//
// The reservation system explicitly allows "recursive locking": you can hold
// an arbitrary number of reservations over a sector by repeatedly creating
// SubRegion() objects. Care must be taken to release every single reservation
// obtained previously, otherwise the collector will remain blocked forever.
//
// Here is an illustration of a qringbuf object lifecycle initialized with
//  {
//    BufferSize: 64,
//    MinRegion:  16,
//    MinRead:    8,
//    MaxCopy     24,
//  }
// ·
//
//  † C is the collector position: the *end* of the most recent read from the underlying io.Reader
//    E is the emitter position: the *start* of the most recently returned NextRegion()
//    W is the last value of "E" before a wrap took place, 0 otherwise
//
//  ⓪ Buffer initialized, StartFill(…) is called.
//       ╆━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╅
//       C=0                                                             ┃
//       E=0                                                             ┃
//       ╄━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╃
//       |0        |10       |20       |30       |40       |50       |60
//
//  ① NextRegion(0) is blocked until MinRegion of 16 is available,
//     fill in progress
//       ╆━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╅
//       cccccccccc|C=10                                                 ┃
//       E=0                                                             ┃
//       ╄━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╃
//       |0        |10       |20       |30       |40       |50       |60
//
//  ② NextRegion(0) returned the first 30 bytes when it could,
//     collector keeps reading further
//       ╆━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╅
//       eeeeeeeeeeeeeeeeeeeeeeeeeeeeeecccccccccc|C=40                   ┃
//       E=0==========================<                                  ┃
//       ╄━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╃
//       |0        |10       |20       |30       |40       |50       |60
//
//  ③ User reserves subRegion 18~21 for async workers, recycles last 6 of the
//     30 bytes, NextRegion(6) returns 17 bytes available at the time, 23 total.
//     Collector keeps reading, until it can no longer satisfy MinRead
//       ╆━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╅
//       ┋                 RRRR  eeeeeeeeeeeeeeeeeeeeeeecccccccccccc|C=59┃
//       ┋                 RRRR  E=24==================<                 ┃
//       ╄━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╃
//       |0        |10       |20       |30       |40       |50       |60
//
//  ④ User recycles last 6 bytes, NextRegion(6) serves the remaining 18 bytes
//     Collector now can satisfy MaxCopy, and copies everything over,
//     repositioning the emitter index. It then blocks, as it can't write
//     past the not-yet released reservation.
//       ╆━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╅
//       W=41wwwwwwwwwwwwww|C=18                  eeeeeeeeeeeeeeeeee|    ┃
//       E=0               RRRR                   W=41=============<|    ┃
//       ╄━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╃
//       |0        |10       |20       |30       |40       |50       |60
//
//  ⑤ The async job finishes, reservation is released, collector can now
//     advance further, and blocks again as NextRegion() has not been called
//     meaning the last 18 bytes are still being processed.
//       ╆━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╅
//       W=41wwwwwwwwwwwwwwccccccccccccccccccccccc|C=41|eeeeeeeeeeee|    ┃
//       E=0                                      W=41=============<|    ┃
//       ╄━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╃
//       |0        |10       |20       |30       |40       |50       |60
//
//  ⑥ User recycles 4 bytes, NextRegion(4) serves available 27 bytes, and
//     the cycle repeats from the top until error or EOF
//       ╆━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╅
//       ┋             wwwwccccccccccccccccccccccc|C=41                  ┃
//       ┋             E=14======================<|                      ┃
//       ╄━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╃
//       |0        |10       |20       |30       |40       |50       |60
//
//
package qringbuf

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const (
	impossibleStreamLimit     int64  = 1 << 42
	impossibleStreamLimitText string = "over 4 terabytes"
)

// ErrCollectorStopped is the error returned by NextRegion() before the
// background collector goroutine has been started with StartFill(), or
// after StopFill() has been called explicitly. Note that when a readLimit
// supplied to StartFill() is reached, and the collector stops on its own,
// the error state is io.EOF or io.ErrUnexpectedEOF
var ErrCollectorStopped error = errors.New("collector stopped")

// Region is an object representing a part of the buffer. Initially a *Region
// is obtained by calling NextRegion(), but then one can subdivide a *Region
// object into smaller portions via SubRegion().
type Region struct {
	reserved int32
	gen      int
	offset   int
	size     int
	qrb      *QuantizedRingBuffer
}

// Size returns the size of the region in bytes. It is equivalent to (but
// cheaper than):
//  len( r.Bytes )
func (r *Region) Size() int { return r.size }

// Bytes returns a slice of the underlying ring buffer. One should take care
// to copy or finish using the returned slice before making another call to
// NextRegion(). If Reserve() has been invoked, then the slice is guaranteed
// to remain intact before the corresponding Release() call.
func (r *Region) Bytes() []byte { return r.qrb.buf[r.offset : r.offset+r.size] }

// SubRegion is analogous to re-slicing. Supplying offset/length values causing
// an out of bounds re-slice results in log.Panic(). As a special case one can
// "clone" a *Region object via:
//  clone := region.SubRegion( 0, region.Size() )
func (r *Region) SubRegion(offset, length int) *Region {
	if offset < 0 || length <= 0 || offset+length > r.size {
		log.Panicf(
			"subregion bounds out of range [%d:%d] with capacity %d",
			offset,
			offset+length,
			r.size,
		)
	}

	return &Region{
		offset: r.offset + offset,
		size:   length,
		gen:    r.gen,
		qrb:    r.qrb,
	}
}

// Reserve marks the buffer area backing this *Region object as a "no-write"
// zone, until the corresponding Release() has been called. Losing a Reserve()d
// *Region object before calling Release() will lead to a deadlock: neither
// NextRegion() nor the collector will be able to proceed beyond the point
// now-forever reserved. Calling Reserve() more than once on the same object
// results in log.Panic()
func (r *Region) Reserve() {
	if atomic.AddInt32(&r.reserved, 1) != 1 {
		log.Panicf(
			"double-reservation of region %T(%p) [%d:%d]",
			r, &r,
			r.offset, r.offset+r.size,
		)
	} else if r.gen != r.qrb.gen {
		log.Panic("only regions obtained from the most recent NextRegion() call can be reserved")
	}

	if debugReservationsEnabled {
		debugReservations(" reserve [%9d:%9d]", r.offset, r.offset+r.size)
	}
	for _, s := range r.qrb.regionSectors(r.offset, r.size) {
		atomic.AddInt32(&s.userCount, 1)
	}
	if debugReservationsEnabled {
		debugReservations("reserved [%9d:%9d]", r.offset, r.offset+r.size)
	}
}

// Release marks the buffer area backing this *Region object free after
// a previous Reserve() call. Calling Release() more than once on the same
// object, or before Reserve() has been called, results in log.Panic()
func (r *Region) Release() {
	if atomic.AddInt32(&r.reserved, -1) != 0 {
		log.Panicf(
			"double-free of region %T(%p) [%d:%d]",
			r, &r,
			r.offset, r.offset+r.size,
		)
	}

	if debugReservationsEnabled {
		debugReservations(" release [%9d:%9d]", r.offset, r.offset+r.size)
	}
	for _, s := range r.qrb.regionSectors(r.offset, r.size) {
		atomic.AddInt32(&s.userCount, -1)
	}
	if debugReservationsEnabled {
		debugReservations("released [%9d:%9d]", r.offset, r.offset+r.size)
	}

	r.qrb.signalCond(r.qrb.condReservationRelease)
}

// Config is the structure of options expected at initialization time. Note
// that passing a reference to a *Stats structure will incur a penalty, mainly
// from repeatedly calling time.Now()
type Config struct {
	MinRegion  int // [1:…] Any result of NextRegion() is guaranteed to be at least that many bytes long, except at EOF
	MinRead    int // [1:MinRegion] Do not read data from io.Reader until buffer has space to store that many bytes
	BufferSize int // [MinRead+2*MinRegion:…] Size of the allocated buffer in bytes
	MaxCopy    int // [MinRegion:BufferSize/2] Delay "wrap around" until amount of data to copy falls under this threshold
	SectorSize int // [4096:BufferSize/3] Size of each occupancy sector for region.{Reserve|Release}() tracking
	Stats      *Stats
}

type QuantizedRingBuffer struct {
	sync.Mutex
	reader             io.Reader
	opts               Config
	streamRemaining    int64
	cPos               int
	ePos               int
	wrapPos            int
	curRegionSize      int
	gen                int
	buf                []byte
	reservationSectors []*sectorState
	errCondition       error
	statsEnabled       bool

	/*

		>
		> ...CompareAndSwapUint32 is excellent for building state machines...
		> ...It doesn't give you mutexes or locking semantics...
		>
		that was my exact journey!
		- I will use counters ⭆ shit, I can't block on them, only Go itself can sync.runtime_Semacquire ⟱⭅
		- I will use sync.Cond ⭆ shit, I can't do part of my ops lock-free this way ⟱⭅
		- I will use channels for the lock-free part and sync.Cond for the tricky part
			⭆ shit, once I miss a .Signal, I need to have a timer to wake things up, this is horrible ⟱⭅
		- ok ⭆ channels for everything it is...

	*/
	// Each one of these channels emulates a distinct sync.Cond.(Signal|Wait) with strictly 1 .Wait-er
	// * The channels are initialized with len(1) and never closed
	// * Signalling is achieved by send/receive of a single empty struct
	//
	// The reason for not simply using sync.Cond is to keep Region.Release() lock-free
	//
	// See multiple implementations at https://stackoverflow.com/questions/29923666/waiting-on-a-sync-cond-with-a-timeout
	// and the *very* involved discussion in https://github.com/golang/go/issues/21165
	condReservationRelease chan struct{}
	// If we were using sync.Cond we would have used a single var for both of these
	// But because the channels can not really observe locks, we need to have one
	// dedicated channel for each thread. Otherwise after a broadcast+wait a thread
	// could "eat" its own broadcast, never do anything with it, and then get stuck
	// waiting on the other thread that now can never move
	condCollectorChange chan struct{}
	condEmitterChange   chan struct{}

	// This pair of channels implements stop/end semaphores for the collector goroutine
	// Think of them as very specialized, internal-only ctx, that exist to correctly
	// mesh with the select()s waiting on the "cond-channels" above
	//
	// They are initialized anew on every Restart(), never receive any values,
	// and are closed in definition-order when a stop condition arises
	semStopCollector chan struct{}
	semCollectorDone chan struct{}
}

// Stats is a simple struct of counters. When (optionally) supplied as part of
// the constructor options, its fields will be incremented through the course
// of the qringbuf object lifetime. In order to obtain a consistent read of
// the stats values, either the collector should have terminated, or you should
// obtain a Lock() before accessing the structure.
// Note that collecting these stats will incur a performance penalty, mainly
// due to the repeated calls to time.Now()
type Stats struct {
	ReadCalls                int64 `json:"readCalls"`
	CollectorYields          int64 `json:"collectorYields"`
	CollectorWaitNanoseconds int64 `json:"collectorWaitNanoseconds"`
	NextRegionCalls          int64 `json:"nextRegionCalls"`
	EmitterYields            int64 `json:"emitterYields"`
	EmitterWaitNanoseconds   int64 `json:"emitterWaitNanoseconds"`
}

type sectorState struct {
	userCount       int32
	thisSectorStart int
	nextSectorStart int
}

func NewFromReader(
	reader io.Reader,
	cfg Config,
) (*QuantizedRingBuffer, error) {

	if cfg.MinRegion < 1 {
		return nil, fmt.Errorf(
			"value of MinRegion '%d' out of range [1:...]",
			cfg.MinRegion,
		)
	}
	if cfg.MinRead < 1 || cfg.MinRead > cfg.MinRegion {
		return nil, fmt.Errorf(
			"value of MinRead '%d' out of range [1:%d]",
			cfg.MinRead,
			cfg.MinRegion,
		)
	}
	if cfg.BufferSize < 2*cfg.MinRegion+cfg.MinRead {
		return nil, fmt.Errorf(
			"value of BufferSize '%d' out of range [%d:...]",
			cfg.BufferSize,
			2*cfg.MinRegion+cfg.MinRead,
		)
	}
	if cfg.MaxCopy < cfg.MinRegion || cfg.MaxCopy > cfg.BufferSize/2 {
		return nil, fmt.Errorf(
			"value of MaxCopy '%d' out of range [%d:%d]",
			cfg.MaxCopy,
			cfg.MinRegion,
			cfg.BufferSize/2,
		)
	}
	if cfg.SectorSize < 4096 || cfg.SectorSize > cfg.BufferSize/3 {
		return nil, fmt.Errorf(
			"value of SectorSize '%d' out of range [4096:%d]",
			cfg.SectorSize,
			cfg.BufferSize/3,
		)
	}

	qrb := &QuantizedRingBuffer{
		reader:           reader,
		opts:             cfg,
		statsEnabled:     (cfg.Stats != nil),
		buf:              make([]byte, cfg.BufferSize),
		errCondition:     ErrCollectorStopped,
		semStopCollector: make(chan struct{}),
		semCollectorDone: make(chan struct{}),

		// never close these, they replace sync.Cond's
		condReservationRelease: make(chan struct{}, 1),
		condCollectorChange:    make(chan struct{}, 1),
		condEmitterChange:      make(chan struct{}, 1),
	}
	close(qrb.semStopCollector)
	close(qrb.semCollectorDone)

	sectorCount := cfg.BufferSize / cfg.SectorSize
	// silliness to avoid invoking math.Ceil
	if cfg.BufferSize%cfg.SectorSize != 0 {
		sectorCount++
	}
	qrb.reservationSectors = make([]*sectorState, sectorCount)
	for sectorCount > 0 {
		sectorCount--
		qrb.reservationSectors[sectorCount] = &sectorState{
			thisSectorStart: cfg.SectorSize * sectorCount,
			nextSectorStart: cfg.SectorSize * (sectorCount + 1),
		}
	}

	return qrb, nil
}

func (qrb *QuantizedRingBuffer) signalCond(c chan<- struct{}) {
	select {
	case c <- struct{}{}:
	default:
		// a signal is already waiting to be picked up - blast through
	}
}

// Buffered returns the current amount of data already read from the underlying
// reader, but not yet served via NextRegion(). It is primarily useful for
// informative error messages:
//
//  if err == io.ErrUnexpectedEOF {
//    return fmt.Errorf(
//      "unexpected end of stream after %d bytes (stream expected to be %d bytes long)",
//      totalProcessedSoFar + int64( qrb.Buffered() ),
//      expectedStreamLengthPassedToStartFill,
//    )
//  }
func (qrb *QuantizedRingBuffer) Buffered() int {
	defer qrb.Unlock()
	qrb.Lock()
	return qrb.cPos - (qrb.ePos + qrb.curRegionSize)
}

// NextRegion returns a *Region object representing a portion of the underlying
// stream. You can explicitly request overlapping *Region's by supplying the
// number of bytes to "step back". This is especially useful when processing a
// stream of variable-length records where the only information you have is the
// maximum size of a record. By initializing your qringbuf with MinRegion equal
// to this maximum value, you guarantee never experiencing an under-read.
// Each call to NextRegion must advance the stream by at least a single byte:
// calling NextRegion with regionRemainder equal or larger than Size() of the
// last *Region results in log.Panic()
func (qrb *QuantizedRingBuffer) NextRegion(regionRemainder int) (r *Region, err error) {
	var t0 time.Time

	defer qrb.Unlock()
	if qrb.statsEnabled {
		t0 = time.Now()
	}
	qrb.Lock()
	if qrb.statsEnabled {
		qrb.opts.Stats.EmitterWaitNanoseconds += time.Since(t0).Nanoseconds()
		qrb.opts.Stats.NextRegionCalls++
	}

	if regionRemainder < 0 {
		log.Panicf(
			"supplied invalid negative remainder %d",
			regionRemainder,
		)
	} else if regionRemainder != 0 &&
		regionRemainder >= qrb.curRegionSize {
		log.Panicf(
			"supplied remainder %d must be smaller than the %d obtained from the last NextRegion() call",
			regionRemainder, qrb.curRegionSize,
		)
	}

	if debugReservationsEnabled && qrb.curRegionSize > 0 {
		debugReservations("   letgo [%9d:%9d]\trem:%d", qrb.ePos, qrb.ePos+qrb.curRegionSize, regionRemainder)
	}
	qrb.wrapPos = 0 // regardles whether was set or not
	qrb.ePos += qrb.curRegionSize - regionRemainder
	qrb.curRegionSize = 0 // when collector is finished, this signals drain-end for Restart()
	qrb.signalCond(qrb.condEmitterChange)

	// Wait ( collector moves our start pos on wraparound ) while:
	// - no error at all ( not even EOF )
	// - there is not enough space to serve between us and the collector "write-start"
waitOnCollector:
	for qrb.errCondition == nil && qrb.ePos+qrb.opts.MinRegion > qrb.cPos {
		if qrb.statsEnabled {
			qrb.opts.Stats.EmitterYields++
			t0 = time.Now()
		}
		qrb.Unlock()
		select {
		case <-qrb.condCollectorChange:
			// just waiting, nothing to do
		case <-qrb.semCollectorDone:
			// when we are done - we are done
			qrb.Lock()
			if qrb.statsEnabled {
				qrb.opts.Stats.EmitterWaitNanoseconds += time.Since(t0).Nanoseconds()
			}
			break waitOnCollector
		}
		qrb.Lock()
		if qrb.statsEnabled {
			qrb.opts.Stats.EmitterWaitNanoseconds += time.Since(t0).Nanoseconds()
		}
	}

	if qrb.errCondition != nil {
		if qrb.ePos < qrb.cPos &&
			(qrb.errCondition == io.EOF ||
				qrb.errCondition == ErrCollectorStopped) {
			// not yet done with what the collector left us
			// return the remaining range at the end
		} else {
			return nil, qrb.errCondition
		}
	}

	qrb.gen++ // counter separate from the stats, for mis-reservation errors
	qrb.curRegionSize = qrb.cPos - qrb.ePos
	if debugReservationsEnabled {
		debugReservations("    held [%9d:%9d]", qrb.ePos, qrb.ePos+qrb.curRegionSize)
	}
	return &Region{
		offset: qrb.ePos,
		size:   qrb.curRegionSize,
		gen:    qrb.gen,
		qrb:    qrb,
	}, qrb.errCondition
}

func (qrb *QuantizedRingBuffer) collector() {
	// No .Unlock() in this defer - see comment a bit further down
	defer func() {
		close(qrb.semCollectorDone)
		qrb.signalCond(qrb.condCollectorChange) // one last signal, emitter won't wait after above closes
	}()

	var mustRead, couldRead, didRead int
	var t0 time.Time

	for {
		if qrb.statsEnabled {
			t0 = time.Now()
		}
		// Lock() and Unlock() repeatedly every time we loop through this outer
		// scope. Provides one more point (aside from the wait's below) for the
		// emitter side to begin dispensing regions to the end-user
		qrb.Lock()
		if qrb.statsEnabled {
			qrb.opts.Stats.CollectorWaitNanoseconds += time.Since(t0).Nanoseconds()
		}

	spaceWaitLoop:
		// INNER spaceWaitLoop START
		for {

			if qrb.cPos < qrb.ePos {
				log.Panicf(
					"collector is behind emitter, this is not possible\tePos:%d\tcPos:%d\tbufSize:%d\tcurRegSize:%d",
					qrb.ePos,
					qrb.cPos,
					qrb.opts.BufferSize,
					qrb.curRegionSize,
				)
			}

			if qrb.wrapPos > 0 {
				couldRead = qrb.wrapPos - qrb.cPos
			} else {
				couldRead = qrb.opts.BufferSize - qrb.cPos
			}

			// Either we are end-of-streaming, OR we
			// - BOTH have enough to fit a minimum read,
			// - AND we won't put the emitter in a situation where it has nothing to emit
			if (qrb.streamRemaining > 0 && int64(couldRead) >= qrb.streamRemaining) ||
				(couldRead >= qrb.opts.MinRead &&
					(qrb.cPos+couldRead)-(qrb.ePos+qrb.curRegionSize) >= qrb.opts.MinRegion) {
				break spaceWaitLoop

			} else if qrb.cPos-qrb.ePos <= qrb.opts.MaxCopy &&
				qrb.ePos >= qrb.cPos-qrb.ePos {
				// - we can no longer write at our current position (see above)
				// - emitter is less than MaxCopy away from catching up to us
				// - there is enough room before the emitter to copy&rewind
				//
				// We do not know what the emitter will "release back", but
				// we already checked it is "close enough to us". So what we do
				// is copy what's available, move emitter to 0, move us to
				// the copy-end and proceed
				qrb.regionFreeWait(0, qrb.cPos-qrb.ePos, 0)
				if qrb.errCondition != nil {
					qrb.Unlock()
					return
				}

				// If we do end up waiting above, we will unlock the mutex,
				// which means emitter could advance, which in turn means
				// we may end up copying less (or nothing at all)
				if qrb.cPos-qrb.ePos > 0 {
					if debugReservationsEnabled {
						debugReservations("    copy [%9d:%9d]", 0, qrb.cPos-qrb.ePos)
					}
					qrb.cPos = copy(qrb.buf, qrb.buf[qrb.ePos:qrb.cPos])
					if debugReservationsEnabled {
						debugReservations("  copied [%9d:%9d]", 0, qrb.cPos)
					}
					if qrb.curRegionSize > 0 {
						qrb.wrapPos = qrb.ePos
					}
				} else {
					qrb.cPos = 0
				}

				qrb.ePos = 0
				qrb.signalCond(qrb.condCollectorChange)

				// loop back from the start to re-evaluate the new ePos/cPos
				continue spaceWaitLoop
			}

			if qrb.statsEnabled {
				qrb.opts.Stats.CollectorYields++
				t0 = time.Now()
			}
			qrb.Unlock()
			select {
			case <-qrb.condEmitterChange:
				// just waiting, nothing to do
			case <-qrb.semStopCollector:
				qrb.Lock()
				if qrb.statsEnabled {
					qrb.opts.Stats.CollectorWaitNanoseconds += time.Since(t0).Nanoseconds()
				}
				qrb.errCondition = ErrCollectorStopped
				qrb.Unlock()
				return
			}
			qrb.Lock()
			if qrb.statsEnabled {
				qrb.opts.Stats.CollectorWaitNanoseconds += time.Since(t0).Nanoseconds()
			}
		}
		// INNER spaceWaitLoop END

		mustRead = qrb.opts.MinRead
		if qrb.streamRemaining > 0 {
			if int64(mustRead) > qrb.streamRemaining {
				mustRead = int(qrb.streamRemaining)
			}
			if int64(couldRead) > qrb.streamRemaining {
				couldRead = int(qrb.streamRemaining)
			}
		}

		// we may shrink the free range if it would save us from waiting
		couldRead = qrb.regionFreeWait(qrb.cPos, mustRead, couldRead)
		if qrb.errCondition != nil {
			qrb.Unlock()
			return
		}

		// one last stop-check before asking the reader for more
		select {
		case <-qrb.semStopCollector:
			qrb.errCondition = ErrCollectorStopped
			qrb.Unlock()
			return
		default:
			// proceed
		}

		if debugReservationsEnabled {
			debugReservations(" writing [%9d:%9d]", qrb.cPos, qrb.cPos+couldRead)
		}
		didRead, qrb.errCondition = qrb.reader.Read(
			qrb.buf[qrb.cPos : qrb.cPos+couldRead],
		)
		if debugReservationsEnabled {
			debugReservations("   wrote [%9d:%9d]", qrb.cPos, qrb.cPos+didRead)
		}
		if qrb.statsEnabled {
			qrb.opts.Stats.ReadCalls++
		}

		if didRead > 0 {
			qrb.cPos += didRead
			qrb.streamRemaining -= int64(didRead)
			qrb.signalCond(qrb.condCollectorChange)
		} else if qrb.errCondition == nil {
			log.Panic("zero-size read without a raised error")
		}

		if qrb.errCondition == nil && qrb.streamRemaining == 0 {
			qrb.errCondition = io.EOF
		} else if qrb.errCondition == io.EOF && qrb.streamRemaining > 0 {
			qrb.errCondition = io.ErrUnexpectedEOF
		}

		if qrb.errCondition != nil {
			qrb.Unlock()
			return
		}

		qrb.Unlock()
	}
}

// StopFill is used to shutdown the collector goroutine without having reached
// an io error on the underlying io.Reader. Calling this function blocks until
// the collector goroutine exits. Note that if the goroutine is currently busy
// performing a syscall as part of a Read(), it may take an arbitrarily long
// amount of time to return. In cases of a blocked system read you will need to
// arrange for a signal to be delivered to your process, interrupting all
// syscalls.
func (qrb *QuantizedRingBuffer) StopFill() (didResultInStop bool) {
	select {

	case <-qrb.semCollectorDone:
		// already stopped

	case <-qrb.semStopCollector:
		// wait for stop-in-progress
		<-qrb.semCollectorDone

	default:

		// issue stop
		// do so in a (possibly itself deadlock-prone) lock as the select is non-atomic
		qrb.Lock()
		select {
		case <-qrb.semStopCollector:
		default:
			close(qrb.semStopCollector)
			didResultInStop = true
		}
		qrb.Unlock()

		// wait for stop
		<-qrb.semCollectorDone
	}

	return
}

// StartFill is called at the start of every stream-cycle. If readLimit is
// 0: collector will continue until the underlying io.Reader returns io.EOF.
// If readLimit is a positive value, the collector will read exactly that many
// bytes before shutting down. If the underlying reader returns io.EOF before
// readLimit is reached, NextRegion() will return io.ErrUnexpectedEOF.
//
// Note that one can process the same io.Reader (using the same
// qringbuf/allocation) as multiple sub-streams, as long as the length of each
// sub-stream is known in advance. For instance, given a reader r, one can do
// something like:
//  qrb, err := qringbuf.NewFromReader( r, qringbuf.Config{…} )
//  for {
//    var nextSubstreamSize int64
//    err := binary.Read(
//      r,
//      binary.BigEndian,
//      &nextSubstreamSize,
//    )
//    qrb.StartFill( nextSubstreamSize )
//    for {
//      … // process sub-stream here, then repeat
//    }
//  }
func (qrb *QuantizedRingBuffer) StartFill(readLimit int64) error {
	// wait until shut down
	<-qrb.semCollectorDone

	qrb.Lock()
	defer qrb.Unlock()

	// wait until drained
	for qrb.curRegionSize > 0 {
		qrb.Unlock()
		<-qrb.condEmitterChange
		qrb.Lock()
	}

	if qrb.errCondition != io.EOF && qrb.errCondition != ErrCollectorStopped {
		// not starting anything - we are already in hard error for the Reader
		return qrb.errCondition
	} else if readLimit < 0 {
		qrb.errCondition = fmt.Errorf(
			"unexpected negative read-limit/stream-size %d",
			readLimit,
		)
		return qrb.errCondition
	} else if readLimit > impossibleStreamLimit {
		qrb.errCondition = fmt.Errorf(
			"read-limit/stream-size of %d bytes (%s) encountered",
			readLimit,
			impossibleStreamLimitText,
		)
		return qrb.errCondition
	}

	qrb.gen++
	qrb.ePos = 0
	qrb.cPos = 0
	qrb.wrapPos = 0
	qrb.errCondition = nil
	qrb.semStopCollector = make(chan struct{})
	qrb.semCollectorDone = make(chan struct{})
	if readLimit > 0 {
		qrb.streamRemaining = readLimit
	} else {
		qrb.streamRemaining = -1
	}

	// Terminates on its own at stream end / error, sets qrb.errCondition
	// also responds to qrb.Stop()
	// Direct ctx-based cancellation not implemented deliberately, as it would
	// be misleading: there is no cancellation support on most io.Reader's
	// Instead use the blocking Stop() which will not return unless the goroutine
	// is truly terminated
	// Also https://dave.cheney.net/2017/08/20/context-isnt-for-cancellation
	go qrb.collector()

	// // Deadlock infodump
	// done := qrb.semCollectorDone
	// go func() {
	// 	t := time.NewTimer(20 * time.Minute)
	// 	// t := time.NewTimer(10 * time.Second)
	// 	select {
	// 	case <-done:
	// 		t.Stop()
	// 	case <-t.C:
	// 		qrb.Lock()
	// 		fmt.Fprintf(os.Stderr,
	// 			"\n\ncollectorChan:%#v\nemitterChan:  %#v\n\n| -- %d -- @ E @ -- %d -- @ C @ -- %d |\nBufsize:%9d\nMinRead:%9d\nMinReg: %9d\nMaxCopy:%9d\n\n| -- [ %d curRegion %d ] --\ncurReg: %9d\n\n\n",
	// 			qrb.condCollectorChange,
	// 			qrb.condEmitterChange,
	// 			qrb.ePos,
	// 			qrb.cPos-qrb.ePos,
	// 			qrb.opts.BufferSize-qrb.cPos,
	// 			qrb.opts.BufferSize,
	// 			qrb.opts.MinRead,
	// 			qrb.opts.MinRegion,
	// 			qrb.opts.MaxCopy,
	// 			qrb.ePos, qrb.ePos+qrb.curRegionSize,
	// 			qrb.curRegionSize,
	// 		)
	// 		p, _ := os.FindProcess(os.Getpid())
	// 		p.Signal(syscall.SIGQUIT)
	// 	}
	// }()

	return nil
}

func (qrb *QuantizedRingBuffer) regionSectors(offset, size int) []*sectorState {
	if size == 0 {
		return nil
	}
	return qrb.reservationSectors[(offset / qrb.opts.SectorSize):((offset+size-1)/qrb.opts.SectorSize + 1)]
}

func (qrb *QuantizedRingBuffer) regionFreeWait(offset, min, max int) (available int) {
	if min == 0 && max == 0 {
		return 0
	} else if min > max {
		max = min
	}

	if debugReservationsEnabled {
		debugReservations(" waiting [%9d:%9d]\tmin:%d\tmax:%d", offset, offset+max, min, max)
		defer func() {
			debugReservations("  waited [%9d:%9d]\tmin:%d\tmax:%d", offset, offset+available, min, max)
		}()
	}

	var t0 time.Time
	var stopReceived bool

	// We do not refer to volatile parts of qrb.* in the loop
	// Remain unlocked for the duration of waiting on releases
	qrb.Unlock()
	defer func() {
		qrb.Lock()
		if qrb.statsEnabled {
			qrb.opts.Stats.CollectorWaitNanoseconds += time.Since(t0).Nanoseconds()
		}
		if stopReceived {
			qrb.errCondition = ErrCollectorStopped
		}
	}()
	if qrb.statsEnabled {
		t0 = time.Now()
	}

	// Ensure there is enough free space from offset, to satisfy
	// either the desired or at least the required size
	//
	// This codepath is entered by the collector only, which means
	// that when we are here the emitter is blocked.  Since we
	// already validated the activeRegion region does not overlap with
	// anything we will be examining, all we need to check is that
	// all reservation sectors have no users.
	//
	// It is safe to do this sequentially once per sector, as the
	// emitter won't advance past us
	for _, s := range qrb.regionSectors(offset, max) {

		for atomic.LoadInt32(&s.userCount) != 0 {

			// we are about to block - bail if we are happy
			if available >= min {
				return
			}

			select {
			// just block until release or stop
			case <-qrb.condReservationRelease:
			case <-qrb.semStopCollector:
				stopReceived = true
				return
			}

		}

		// sector is free for our purposes \o/
		if offset > s.thisSectorStart {
			available += (s.nextSectorStart - offset)
		} else {
			available += qrb.opts.SectorSize
		}

		if available > max {
			available = max
		}
	}
	return
}

/*

while true; do tmp/maintbin/dezstd $( ls maint/testdata/*repeat* | sort -R ) $( ls maint/testdata/*repeat* | sort -R ) \
| go run ./cmd/stream-dagger/ --multipart --ipfs-add-compatible-command="--cid-version=1 --chunker=rabin" \
--emit-stdout=none --emit-stderr=roots-jsonl,chunks-jsonl 2>e1 >s1
grep 'event.*root' e1 | grep -oE 'cid.*' | sort | md5sum | grep 759d2726a1a55acda87368565ebf8a1c || break
done

while true; do zstd -qdck maint/testdata/large_repeat_1GiB.zst \
| go run ./cmd/stream-dagger/ --ipfs-add-compatible-command="--cid-version=1 --chunker=rabin" \
--emit-stdout=none --emit-stderr=roots-jsonl,chunks-jsonl 2>e2 >s2
grep 'event.*root.*bafybeifdyy47lpatwhgie4otzn6rhjbjvrxpj3tlbze2j5xlsexpvca32y' e2 || break
done

while true; do zstd -qdck maint/testdata/repeat_0.04GiB_174.zst \
| go run ./cmd/stream-dagger/ --ipfs-add-compatible-command="--cid-version=1 --chunker=rabin" \
--emit-stdout=none --emit-stderr=roots-jsonl,chunks-jsonl 2>e3 >s3
grep 'event.*root.*bafybeid5puqcsobeg226l6dfvcwznyi4tq4ezjmphikdi2pszffw3stuym' e3 || break
done

*/
const debugReservationsEnabled bool = false

func debugReservations(f string, a ...interface{}) {
	fmt.Printf(
		"%-54s"+f+"\n",
		append(
			[]interface{}{time.Now()},
			a...,
		)...,
	)
}
