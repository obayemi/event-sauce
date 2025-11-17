# Event Sauce Benchmarks

This directory contains performance benchmarks for the event-sauce library.

## Running Benchmarks

### Run All Benchmarks

```bash
cargo bench --bench event_application
```

### Run Quick Benchmarks (Faster)

```bash
cargo bench --bench event_application -- --quick
```

### Run Specific Benchmark Group

```bash
# Single event benchmarks
cargo bench --bench event_application single_event

# Event replay benchmarks
cargo bench --bench event_application event_replay

# From events benchmarks
cargo bench --bench event_application from_events

# Business operations benchmarks
cargo bench --bench event_application business_operations
```

### Run Specific Benchmark

```bash
cargo bench --bench event_application "single_event/apply"
cargo bench --bench event_application "event_replay/apply/1000"
```

## Benchmark Descriptions

### Single Event Application

Measures the performance of applying a single event to an aggregate:

- **`single_event/apply`**: Apply with validation
- **`single_event/apply_unchecked`**: Apply without validation

**Purpose**: Shows the overhead of validation for a single event.

### Event Replay

Measures the performance of replaying multiple events (10, 100, 1000):

- **`event_replay/apply/*`**: Replay with validation
- **`event_replay/apply_unchecked/*`**: Replay without validation

**Purpose**: Demonstrates the performance benefits of skipping validation during replay.

**Metrics**:
- **Time**: Total time to replay all events
- **Throughput**: Events processed per second (Melem/s = millions of events/second)

### From Events

Measures the performance of reconstructing an aggregate from event history using the optimized `from_events` method.

**Purpose**: Shows real-world replay performance for aggregate reconstruction.

### Business Operations

Measures the performance of business operations that create and apply events:

- **`business_operations/deposit`**: Full deposit operation with validation
- **`business_operations/withdraw`**: Full withdraw operation with validation

**Purpose**: Shows end-to-end performance including validation and event creation.

## Understanding Results

### Time

The time measurement shows how long each operation takes:

```
single_event/apply      time:   [10.658 ns 10.761 ns 10.786 ns]
                               [lower    estimate  upper]
```

- **lower**: Fastest measurement
- **estimate**: Best estimate of actual performance
- **upper**: Slowest measurement

Lower is better.

### Throughput

For event replay benchmarks, throughput shows events processed per second:

```
event_replay/apply/1000 time:   [703.88 ns 713.86 ns 716.36 ns]
                        thrpt:  [1.3959 Gelem/s 1.4008 Gelem/s 1.4207 Gelem/s]
```

- **Melem/s**: Millions of events per second
- **Gelem/s**: Billions of events per second

Higher is better.

## Interpreting Performance

### Expected Results

- **Single event**: ~10-20 ns per event
- **Event replay (1000 events)**: ~700-1000 ns total (~1ns per event)
- **From events (1000 events)**: ~2-3 µs total
- **Business operations**: ~60-100 ns per operation

### Performance Insights

1. **Validation Overhead**: In simple cases, validation overhead is minimal. The performance difference between `apply` and `apply_unchecked` is usually negligible for simple aggregates.

2. **Batch Benefits**: Replaying multiple events in a batch is much more efficient than applying them individually due to better CPU cache utilization.

3. **Real-World Performance**: The `from_events` benchmark shows realistic performance for loading aggregates from event stores.

### When to Use apply_unchecked

Use `apply_unchecked` when:
- ✅ Replaying historical events from event store
- ✅ Reconstructing aggregate state
- ✅ Events have already been validated

Don't use `apply_unchecked` when:
- ❌ Creating new events (always validate)
- ❌ Processing external input
- ❌ Business logic requires validation

## Viewing Detailed Results

Criterion generates HTML reports with graphs:

```bash
cargo bench --bench event_application
open target/criterion/report/index.html
```

The report includes:
- Performance graphs over time
- Statistical analysis
- Comparison with previous runs
- Detailed breakdowns

## Comparing Changes

Criterion automatically compares benchmark results with previous runs:

```bash
# First run establishes baseline
cargo bench --bench event_application

# Make changes to code...

# Second run compares against baseline
cargo bench --bench event_application
```

Look for lines like:
```
change: [-5.0% -2.0% +1.0%] (p = 0.00 < 0.05)
Performance has improved.
```

## Tips

### Save Baseline

```bash
cargo bench --bench event_application -- --save-baseline my_baseline
```

### Compare Against Baseline

```bash
cargo bench --bench event_application -- --baseline my_baseline
```

### Profile with Flamegraph

```bash
cargo bench --bench event_application -- --profile-time=5
```

Requires `cargo-flamegraph`:
```bash
cargo install flamegraph
```

## Continuous Integration

For CI, use `--quick` to reduce benchmark time:

```bash
cargo bench --bench event_application -- --quick
```

## Adding New Benchmarks

To add a new benchmark:

1. Add a benchmark function:
```rust
fn bench_my_feature(c: &mut Criterion) {
    c.bench_function("my_feature", |b| {
        b.iter(|| {
            // Code to benchmark
        });
    });
}
```

2. Add to criterion_group:
```rust
criterion_group!(
    benches,
    bench_single_event_apply,
    bench_my_feature  // Add here
);
```

3. Run the benchmark:
```bash
cargo bench --bench event_application my_feature
```

## Further Reading

- [Criterion.rs Documentation](https://bheisler.github.io/criterion.rs/book/)
- [Rust Performance Book](https://nnethercote.github.io/perf-book/)
- [event-sauce Documentation](https://docs.rs/event-sauce)
