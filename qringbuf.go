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

var ErrStopReceived error = errors.New("stop received")

type Region struct {
	reserved int32
	gen      int
	offset   int
	size     int
	qrb      *QuantizedRingBuffer
}

func (r *Region) Size() int     { return r.size }
func (r *Region) Bytes() []byte { return r.qrb.buf[r.offset : r.offset+r.size] }
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

type Config struct {
	MinRegion  int // [1:…] Any result of NextRegion() is guaranteed to be at least that many bytes long, except at EOF
	MinRead    int // [1:MinRegion] Do not read data from io.Reader until buffer has space to store that many bytes
	BufferSize int // [MinRead+2*MinRegion:…] Size of the allocated buffer in bytes
	MaxCopy    int // [MinRegion:BufferSize/2] Delay "wrap around" until amount of data to copy falls under this threshold
	SectorSize int // [1:BufferSize/3]‖-1 Size of each occupancy sector for region.{Reserve|Release}() tracking
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
	//
	// The channels are initialized with len(1) and never closed
	// Signalling is achieved by send/receive of a single empty struct
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
	if cfg.SectorSize > cfg.BufferSize/3 ||
		cfg.SectorSize == 0 ||
		cfg.SectorSize < -1 {
		return nil, fmt.Errorf(
			"value of SectorSize '%d' out of range [1:%d] || -1",
			cfg.SectorSize,
			cfg.BufferSize/3,
		)
	}

	qrb := &QuantizedRingBuffer{
		reader:           reader,
		opts:             cfg,
		statsEnabled:     (cfg.Stats != nil),
		buf:              make([]byte, cfg.BufferSize),
		errCondition:     io.EOF,
		semStopCollector: make(chan struct{}),
		semCollectorDone: make(chan struct{}),

		// never close these, they replace sync.Cond's
		condReservationRelease: make(chan struct{}, 1),
		condCollectorChange:    make(chan struct{}, 1),
		condEmitterChange:      make(chan struct{}, 1),
	}
	close(qrb.semStopCollector)
	close(qrb.semCollectorDone)

	if cfg.SectorSize > 0 {
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
			(qrb.errCondition == io.EOF || qrb.errCondition == ErrStopReceived) {
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
				qrb.errCondition = ErrStopReceived
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
		} else {
			// one last check before asking the reader for more
			select {
			case <-qrb.semStopCollector:
				qrb.errCondition = ErrStopReceived
				qrb.Unlock()
				return
			default:
				// proceed
			}
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
		}

		if qrb.errCondition != nil {
			qrb.Unlock()
			return
		}

		qrb.Unlock()
	}
}

func (qrb *QuantizedRingBuffer) Stop() (didResultInStop bool) {
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

func (qrb *QuantizedRingBuffer) Restart(readLimit int64) error {

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

	if qrb.errCondition != io.EOF && qrb.errCondition != ErrStopReceived {
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
	if size == 0 || qrb.opts.SectorSize < 0 {
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

	// we are not tracking reservations
	if qrb.opts.SectorSize < 0 {
		return max
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
			qrb.errCondition = ErrStopReceived
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
	// all reserveation sectors have no users, and that we are not
	// stepping on the wrap-position if any
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
