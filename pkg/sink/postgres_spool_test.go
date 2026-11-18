package sink

import (
	"bufio"
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/replicase/pgcapture/internal/test"
	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/pb"
	"github.com/replicase/pgcapture/pkg/source"
)

// feedTx enqueues one source transaction consisting of size INSERT changes into
// the supplied channel, advancing lsn between messages.
func feedTx(ch chan source.Change, lsn *uint64, now *time.Time, schema, table string, size int) cursor.Checkpoint {
	*now = now.Add(time.Second)
	*lsn++
	beginLSN := *lsn
	ch <- source.Change{
		Checkpoint: cursor.Checkpoint{LSN: beginLSN, Data: []byte(now.Format(time.RFC3339Nano))},
		Message:    &pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{}}},
	}
	for i := 0; i < size; i++ {
		*now = now.Add(time.Second)
		*lsn++
		v := int32(*lsn)
		ch <- source.Change{
			Checkpoint: cursor.Checkpoint{LSN: *lsn, Data: []byte(now.Format(time.RFC3339Nano))},
			Message: &pb.Message{Type: &pb.Message_Change{Change: &pb.Change{
				Op:     pb.Change_INSERT,
				Schema: schema,
				Table:  table,
				New: []*pb.Field{
					{Name: "id", Oid: 23, Value: &pb.Field_Binary{Binary: encodeInt4(v)}},
					{Name: "v", Oid: 25, Value: &pb.Field_Binary{Binary: []byte("payload")}},
				},
			}}},
		}
	}
	*now = now.Add(time.Second)
	*lsn++
	commitCP := cursor.Checkpoint{LSN: *lsn, Data: []byte(now.Format(time.RFC3339Nano))}
	ts := now.Unix()*1000000 + int64(now.Nanosecond())/1000 - microsecFromUnixEpochToY2K
	ch <- source.Change{
		Checkpoint: commitCP,
		Message:    &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{CommitTime: uint64(ts)}}},
	}
	return commitCP
}

func encodeInt4(v int32) []byte {
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func countTraceMessages(t *testing.T, reader *bytes.Buffer) (flush, sync, begin, commit int) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.Contains(line, "F\tFlush"):
			flush++
		case strings.Contains(line, "F\tSync"):
			sync++
		case strings.Contains(line, "F\tParse") && strings.Contains(line, "BEGIN"):
			begin++
		case strings.Contains(line, "F\tParse") && strings.Contains(line, "COMMIT"):
			commit++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return flush, sync, begin, commit
}

// setupSpoolSink creates a sink with a tiny byte budget so a moderately large
// source transaction crosses the budget and triggers controlled streaming. The
// insert coalescing batch is shrunk as well so that several multi-row INSERT
// statements are produced within one source transaction.
func setupSpoolSink(t *testing.T, maxSpoolBytes int64) *PGXSink {
	t.Helper()
	s := &PGXSink{
		ConnStr:         test.GetPostgresURL(),
		SourceID:        "repl_spool_test",
		Renice:          0,
		BatchTXSize:     100,
		MaxSpoolBytes:   maxSpoolBytes,
		InsertBatchSize: 5,
	}
	if _, err := s.Setup(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Stop() })
	return s
}

// TestPGXSink_LargeTransactionSpooling verifies that a source transaction larger
// than the byte budget is streamed to the server in multiple pipeline flushes
// while remaining inside a single explicit SQL transaction: mid-batch flushes
// use Flush (no Sync, no implicit commit), and the data becomes visible only
// after the final COMMIT together with the checkpoint update.
func TestPGXSink_LargeTransactionSpooling(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, "DROP TABLE IF EXISTS spool_t"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, "CREATE TABLE spool_t (id int primary key, v text)"); err != nil {
		t.Fatal(err)
	}

	// a budget small enough that the rows cross it several times
	sink := setupSpoolSink(t, 512)

	changes := make(chan source.Change, 2000)
	var (
		lsn uint64 = 1000
		now        = time.Now()
	)
	const rowCount = 300
	commitCP := feedTx(changes, &lsn, &now, "public", "spool_t", rowCount)

	var trace bytes.Buffer
	sink.raw.Frontend().Trace(&trace, pgproto3.TracerOptions{SuppressTimestamps: true})

	committed := sink.Apply(changes)
	got := <-committed
	if got.LSN != commitCP.LSN || !bytes.Equal(got.Data, commitCP.Data) {
		t.Fatalf("unexpected checkpoint %v, want %v", got, commitCP)
	}
	if err = sink.Error(); err != nil {
		t.Fatalf("sink error: %v", err)
	}

	flush, sync, begin, commit := countTraceMessages(t, &trace)
	if begin != 1 || commit != 1 {
		t.Fatalf("expected exactly one BEGIN and one COMMIT, got begin=%d commit=%d", begin, commit)
	}
	if flush == 0 {
		t.Fatalf("expected at least one mid-transaction Flush from spool streaming")
	}
	// a single Sync is allowed, at the final pipeline close; every mid-budget
	// stream must have used Flush so the explicit transaction never hit a
	// synchronization point that could commit half of the source transaction.
	if sync != 1 {
		t.Fatalf("expected exactly one final Sync, got %d", sync)
	}
	if flush <= sync {
		t.Fatalf("expected mid-transaction flushes without Sync, flush=%d sync=%d", flush, sync)
	}

	var count int
	if err = conn.QueryRow(ctx, "select count(1) from spool_t").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != rowCount {
		t.Fatalf("expected %d rows committed atomically, got %d", rowCount, count)
	}

	// the checkpoint must be persisted alongside the data
	var srcCommit string
	if err = conn.QueryRow(ctx, "select commit from pgcapture.sources where id = $1", sink.SourceID).Scan(&srcCommit); err != nil {
		t.Fatal(err)
	}
	if srcCommit == "" {
		t.Fatal("checkpoint was not persisted with the committed transaction")
	}

	// no spool may remain in memory after commit
	if sink.spoolBytes != 0 || len(sink.pendingChanges) != 0 || sink.streamedSpoolBytes != 0 || sink.txOpen {
		t.Fatalf("spool state not reset after commit: spoolBytes=%d pending=%d streamed=%d txOpen=%v",
			sink.spoolBytes, len(sink.pendingChanges), sink.streamedSpoolBytes, sink.txOpen)
	}
}

// TestPGXSink_SmallTransactionNoStreaming verifies ordinary small transactions
// keep the original batching behavior: nothing is flushed mid-transaction, the
// spool never crosses the budget, and changes only leave the client at commit.
func TestPGXSink_SmallTransactionNoStreaming(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, "DROP TABLE IF EXISTS spool_small"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, "CREATE TABLE spool_small (id int primary key, v text)"); err != nil {
		t.Fatal(err)
	}

	// use the default (large) insert coalescing batch and the default byte
	// budget: three rows must coalesce into one insert and never stream mid-way.
	sink := setupSpoolSink(t, DefaultMaxSpoolBytes)
	sink.inserts.records = make([][]*pb.Field, defaultInsertBatchSize)
	// re-assert the production coalescing capacity that setupSpoolSink shrank.
	if cap(sink.inserts.records) < 3 {
		t.Fatalf("insert batch capacity too small")
	}

	changes := make(chan source.Change, 100)
	var (
		lsn uint64 = 2000
		now        = time.Now()
	)
	commitCP := feedTx(changes, &lsn, &now, "public", "spool_small", 3)

	var trace bytes.Buffer
	sink.raw.Frontend().Trace(&trace, pgproto3.TracerOptions{SuppressTimestamps: true})

	committed := sink.Apply(changes)
	got := <-committed
	if got.LSN != commitCP.LSN {
		t.Fatalf("unexpected checkpoint %v", got)
	}
	if err = sink.Error(); err != nil {
		t.Fatalf("sink error: %v", err)
	}

	flush, sync, begin, commit := countTraceMessages(t, &trace)
	if begin != 1 || commit != 1 {
		t.Fatalf("expected one BEGIN/COMMIT pair, got begin=%d commit=%d", begin, commit)
	}
	// the single Flush is the one emitted when the commit drains the small
	// spool immediately before the final Sync; no budget-driven streaming may
	// happen while the source transaction is still open.
	if flush > 1 {
		t.Fatalf("small transaction must not stream mid-way, saw %d Flush messages", flush)
	}
	if sync != 1 {
		t.Fatalf("expected one final Sync, got %d", sync)
	}
	// nothing was streamed before the commit, so the resident spool only
	// released at the final boundary
	if sink.streamedSpoolBytes != 0 {
		t.Fatalf("small transaction should not trigger budget streaming, streamed=%d", sink.streamedSpoolBytes)
	}

	var count int
	if err = conn.QueryRow(ctx, "select count(1) from spool_small").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("unexpected row count %d", count)
	}
}

// TestPGXSink_AtomicVisibilityDuringSpooling checks that while a large source
// transaction is being streamed, an independent observer sees the complete old
// state (zero rows), and after commit the complete new state (all rows). It also
// verifies the row count is never partially visible, which would indicate the
// stream had been split into independent transactions.
func TestPGXSink_AtomicVisibilityDuringSpooling(t *testing.T) {
	ctx := context.Background()
	observer, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close(ctx)
	if _, err = observer.Exec(ctx, "DROP TABLE IF EXISTS spool_visible"); err != nil {
		t.Fatal(err)
	}
	if _, err = observer.Exec(ctx, "CREATE TABLE spool_visible (id int primary key, v text)"); err != nil {
		t.Fatal(err)
	}

	sink := setupSpoolSink(t, 256)

	changes := make(chan source.Change, 5000)
	var (
		lsn uint64 = 3000
		now        = time.Now()
	)
	const rowCount = 500
	commitCP := feedTx(changes, &lsn, &now, "public", "spool_visible", rowCount)

	committed := sink.Apply(changes)

	// poll on a separate connection while the sink transaction is open; the
	// observer must always see the old state (0 rows) until commit lands.
	seenPartial := false
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		if err = observer.QueryRow(ctx, "select count(1) from spool_visible").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 && n != rowCount {
			seenPartial = true
		}
		if n == rowCount {
			break
		}
		select {
		case cp, ok := <-committed:
			if !ok {
				t.Fatal("committed channel closed early")
			}
			if cp.LSN != commitCP.LSN {
				t.Fatalf("unexpected checkpoint %v", cp)
			}
			goto done
		case <-time.After(20 * time.Millisecond):
		}
	}
done:
	if err = sink.Error(); err != nil {
		t.Fatalf("sink error: %v", err)
	}
	if seenPartial {
		t.Fatal("observer saw a partially applied transaction; the stream was split into independent transactions")
	}
	var n int
	if err = observer.QueryRow(ctx, "select count(1) from spool_visible").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != rowCount {
		t.Fatalf("expected complete new state of %d rows after commit, got %d", rowCount, n)
	}
}

// TestPGXSink_FailureReplayFromSafeCheckpoint verifies that when applying fails
// after some changes have already been streamed inside the open explicit
// transaction, nothing is committed, no checkpoint is acknowledged beyond the
// last safe one, and a fresh sink resumes from that checkpoint and can replay
// onto the clean (old) state.
func TestPGXSink_FailureReplayFromSafeCheckpoint(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, "DROP TABLE IF EXISTS spool_replay"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, "CREATE TABLE spool_replay (id int primary key, v text)"); err != nil {
		t.Fatal(err)
	}
	// spool_bad keeps a primary key so the sink's schema validation (which needs
	// identity keys) passes, but adds a CHECK constraint that a streamed row
	// deterministically violates server-side (SQLSTATE 23514) only once the
	// stream reaches the bad row. A run-unique name avoids recreating a
	// previously dropped relation, which trips an environment-specific
	// pgcapture event-trigger catalog error.
	now := time.Now()
	badTable := "spool_bad_" + strings.ReplaceAll(now.Format("150405.000000"), ".", "")
	if _, err = conn.Exec(ctx, "CREATE TABLE "+badTable+" (id int primary key, v text check (v <> 'BADROW'))"); err != nil {
		t.Fatal(err)
	}
	defer conn.Exec(ctx, "DROP TABLE "+badTable)

	sink := setupSpoolSink(t, 256)

	// unbuffered channel: every send blocks until the apply loop takes the
	// message, so when Commit is processed the channel is always empty and
	// sourceRemaining == 0, which forces the pipeline to close at each source
	// transaction boundary (mirroring how the real source feeds in lockstep).
	changes := make(chan source.Change)
	done := make(chan struct{})
	failedCPCh := make(chan cursor.Checkpoint, 1)

	// one producer owns all lsn/now state, so no value is shared unsynchronized
	// with the main goroutine. It gives up sending once done closes (the apply
	// loop stops consuming after the fatal error and BaseSink drains to close).
	go func() {
		var (
			lsn uint64 = 4000
			t          = now
		)
		send := func(c source.Change) {
			select {
			case changes <- c:
			case <-done:
			}
		}
		// transaction 1: commits successfully and establishes a safe checkpoint.
		feedTx(changes, &lsn, &t, "public", "spool_replay", 10)

		// transaction 2: enough valid rows to cross the byte budget and get
		// streamed inside the open explicit transaction, followed by a row that
		// violates the CHECK constraint. The error surfaces when the pipeline
		// drains at close; the explicit transaction rolls back entirely.
		t = t.Add(time.Second)
		lsn++
		send(source.Change{
			Checkpoint: cursor.Checkpoint{LSN: lsn, Data: []byte(t.Format(time.RFC3339Nano))},
			Message:    &pb.Message{Type: &pb.Message_Begin{Begin: &pb.Begin{}}},
		})
		for i := 0; i < 200; i++ {
			t = t.Add(time.Second)
			lsn++
			send(source.Change{
				Checkpoint: cursor.Checkpoint{LSN: lsn, Data: []byte(t.Format(time.RFC3339Nano))},
				Message: &pb.Message{Type: &pb.Message_Change{Change: &pb.Change{
					Op:     pb.Change_INSERT,
					Schema: "public",
					Table:  badTable,
					New: []*pb.Field{
						{Name: "id", Oid: 23, Value: &pb.Field_Binary{Binary: encodeInt4(int32(lsn))}},
						{Name: "v", Oid: 25, Value: &pb.Field_Binary{Binary: []byte("payload")}},
					},
				}}},
			})
		}
		t = t.Add(time.Second)
		lsn++
		send(source.Change{
			Checkpoint: cursor.Checkpoint{LSN: lsn, Data: []byte(t.Format(time.RFC3339Nano))},
			Message: &pb.Message{Type: &pb.Message_Change{Change: &pb.Change{
				Op:     pb.Change_INSERT,
				Schema: "public",
				Table:  badTable,
				New: []*pb.Field{
					{Name: "id", Oid: 23, Value: &pb.Field_Binary{Binary: encodeInt4(1)}},
					{Name: "v", Oid: 25, Value: &pb.Field_Binary{Binary: []byte("BADROW")}}, // violates CHECK
				},
			}}},
		})
		t = t.Add(time.Second)
		lsn++
		failedCP := cursor.Checkpoint{LSN: lsn, Data: []byte(t.Format(time.RFC3339Nano))}
		select {
		case failedCPCh <- failedCP:
		default:
		}
		send(source.Change{
			Checkpoint: failedCP,
			Message:    &pb.Message{Type: &pb.Message_Commit{Commit: &pb.Commit{CommitTime: uint64(t.Unix())}}},
		})
	}()

	committed := sink.Apply(changes)
	// the committed checkpoint is itself the safe resume position, read from the
	// channel rather than shared producer state.
	safeFirstCP := <-committed
	if safeFirstCP.LSN == 0 {
		t.Fatal("expected a non-zero checkpoint for the safe transaction")
	}
	if err = sink.Error(); err != nil {
		t.Fatalf("sink error: %v", err)
	}

	var safeCommit string
	if err = conn.QueryRow(ctx, "select commit from pgcapture.sources where id = $1", sink.SourceID).Scan(&safeCommit); err != nil {
		t.Fatal(err)
	}

	failedCP := <-failedCPCh

	// the apply loop stores the error and stops itself; wait for that.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && sink.Error() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	// BaseSink keeps draining its input channel after a fatal error until it is
	// closed; stop the producer and close the channel before calling Stop.
	close(done)
	close(changes)
	if sink.Error() == nil {
		t.Fatal("expected the sink to surface the server error")
	}
	if !sink.fatal {
		t.Fatal("the sink should be marked fatal after an unrecoverable pipeline failure")
	}
	if stopErr := sink.Stop(); stopErr == nil {
		t.Fatal("expected Stop to return the apply error")
	}

	// the checkpoint of the failed transaction must never have been acknowledged
	select {
	case cp := <-committed:
		if cp.LSN == failedCP.LSN {
			t.Fatal("failed transaction checkpoint was acknowledged")
		}
	default:
	}

	// none of the streamed rows exist: the open transaction was rolled back,
	// both tables keep their complete old state.
	var n, badN int
	if err = conn.QueryRow(ctx, "select count(1) from spool_replay").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("expected the complete old state of 10 rows after failure, got %d", n)
	}
	if err = conn.QueryRow(ctx, "select count(1) from "+badTable).Scan(&badN); err != nil {
		t.Fatal(err)
	}
	if badN != 0 {
		t.Fatalf("streamed rows of the failed transaction must have rolled back, got %d rows in %s", badN, badTable)
	}

	// the persisted checkpoint must still point at the first, safe transaction.
	var afterCommit string
	if err = conn.QueryRow(ctx, "select commit from pgcapture.sources where id = $1", sink.SourceID).Scan(&afterCommit); err != nil {
		t.Fatal(err)
	}
	if afterCommit != safeCommit {
		t.Fatalf("checkpoint advanced despite failed transaction: %s -> %s", safeCommit, afterCommit)
	}

	// a restarted sink resumes from the safe checkpoint.
	sink2 := &PGXSink{
		ConnStr:         test.GetPostgresURL(),
		SourceID:        "repl_spool_test",
		Renice:          0,
		BatchTXSize:     100,
		MaxSpoolBytes:   256,
		InsertBatchSize: 5,
	}
	resumeCP, err := sink2.Setup()
	if err != nil {
		t.Fatal(err)
	}
	if resumeCP.LSN != safeFirstCP.LSN || !bytes.Equal(resumeCP.Data, safeFirstCP.Data) {
		t.Fatalf("restart did not resume from the safe checkpoint %v, got %v", safeFirstCP, resumeCP)
	}

	// replay a valid transaction onto the clean old state and verify convergence
	replayLSN := uint64(6000)
	replayNow := time.Now()
	replay := make(chan source.Change, 500)
	replayCP := feedTx(replay, &replayLSN, &replayNow, "public", "spool_replay", 5)
	committed2 := sink2.Apply(replay)
	if cp := <-committed2; cp.LSN != replayCP.LSN {
		t.Fatalf("unexpected replay checkpoint %v", cp)
	}
	if err = sink2.Error(); err != nil {
		t.Fatalf("replay sink error: %v", err)
	}
	sink2.Stop()
	if err = conn.QueryRow(ctx, "select count(1) from spool_replay").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 15 {
		t.Fatalf("expected converged state of 15 rows after replay, got %d", n)
	}
}

// TestPGXSink_StreamSpoolReleasesMemory verifies that after a budget-driven
// stream, the queued changes no longer retain their argument buffers and the
// resident spool returns to zero even though the transaction stays open.
func TestPGXSink_StreamSpoolReleasesMemory(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS spool_t (id int primary key, v text)"); err != nil {
		t.Fatal(err)
	}

	sink := setupSpoolSink(t, 1) // budget of one byte forces a stream on every change
	changes := make(chan source.Change, 200)
	var (
		lsn uint64 = 5000
		now        = time.Now()
	)
	commitCP := feedTx(changes, &lsn, &now, "public", "spool_t", 20)
	committed := sink.Apply(changes)
	if cp := <-committed; cp.LSN != commitCP.LSN {
		t.Fatalf("unexpected checkpoint %v", cp)
	}
	if err := sink.Error(); err != nil {
		t.Fatalf("sink error: %v", err)
	}
	if len(sink.pendingChanges) != 0 || sink.spoolBytes != 0 {
		t.Fatalf("buffers retained after streaming: pending=%d spoolBytes=%d", len(sink.pendingChanges), sink.spoolBytes)
	}
}

// TestPGXSink_LargeTransactionManyFlushes drives a single source transaction
// large enough to cross the byte budget many times. It proves that repeated
// mid-transaction Flush streaming neither deadlocks (server responses are
// buffered until the final Sync drains them) nor lets the resident spool grow
// beyond the budget, while the whole transaction still commits atomically.
func TestPGXSink_LargeTransactionManyFlushes(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, test.GetPostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, "DROP TABLE IF EXISTS spool_many"); err != nil {
		t.Fatal(err)
	}
	if _, err = conn.Exec(ctx, "CREATE TABLE spool_many (id int primary key, v text)"); err != nil {
		t.Fatal(err)
	}

	const budget int64 = 1024
	sink := setupSpoolSink(t, budget)

	// 3000 rows with a 5-row coalescing batch and a 1KiB budget force hundreds of
	// budget crossings inside one source transaction.
	const rowCount = 3000
	changes := make(chan source.Change, rowCount+2)
	var (
		lsn uint64 = 9000
		now        = time.Now()
	)
	commitCP := feedTx(changes, &lsn, &now, "public", "spool_many", rowCount)
	committed := sink.Apply(changes)

	select {
	case cp := <-committed:
		if cp.LSN != commitCP.LSN {
			t.Fatalf("unexpected checkpoint %v", cp)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("large multi-flush transaction deadlocked or never committed")
	}
	if err = sink.Error(); err != nil {
		t.Fatalf("sink error: %v", err)
	}
	// the resident spool high-water (recorded inside the apply goroutine, read
	// only after it has stopped touching the sink) must stay close to the budget
	// rather than growing with the transaction size. One coalesced multi-row
	// insert can overshoot the budget by a single batch before it streams.
	highWater := sink.SpoolHighWaterBytes()
	if highWater == 0 {
		t.Fatal("expected a non-zero spool high-water mark")
	}
	if highWater > budget*8 {
		t.Fatalf("resident spool high-water %d far exceeds budget %d; memory is not bounded", highWater, budget)
	}

	var count int
	if err = conn.QueryRow(ctx, "select count(1) from spool_many").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != rowCount {
		t.Fatalf("expected %d rows committed atomically, got %d", rowCount, count)
	}
	// the resident spool must be empty after commit and no transaction left open
	if sink.spoolBytes != 0 || len(sink.pendingChanges) != 0 || sink.txOpen {
		t.Fatalf("spool not reset: spoolBytes=%d pending=%d txOpen=%v",
			sink.spoolBytes, len(sink.pendingChanges), sink.txOpen)
	}
	// the streamed volume must greatly exceed the budget, proving the budget
	// actually bounded the resident memory rather than holding everything
	if sink.streamedSpoolBytes != 0 {
		t.Fatalf("streamed counter should reset at commit, got %d", sink.streamedSpoolBytes)
	}
}
