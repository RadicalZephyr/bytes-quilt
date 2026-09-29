# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Why this exists

The quilt came out of a host that downloads a several-KB blob from a device in small pages (packets top out around 60 bytes), where the device doesn't promise to answer page requests in order, or at all. My first version assumed both, and its startup routine started over whenever a single page went missing. The quilt lets the host fire off page requests, drop each reply where it belongs as it arrives, and re-request only what's still missing.

The crate stands on its own, so describe that use case in those general terms. The history merged in at `ffc1c89` was pulled out of that other project with filter-repo, which is why old commits use older names (`OutOfOrderBytes`, `insert_at_offset`) and mention code that isn't here.

The API is shaped around that caller:

- `put_at(offset, bytes)` returns `Some(MissingSegment)` from the write that opens a gap, so the caller can re-request right away.
- `missing_segments()` lists the gaps, and `MissingSegment::offsets_for(frame_size)` turns one into the offsets to re-request (whole frames only).
- Once nothing's missing, `into_inner()` hands back the contiguous buffer.
- Duplicates are normal: after a retry, the late original and the retry can both arrive. `Err(WouldOverwrite)` is how the caller tells a duplicate from a new page, so overlap detection is on the main path.
- The quilt doesn't know how long the payload is. `missing_segments()` only sees gaps behind the furthest write, so a dropped final page is invisible to it; spotting the end is the caller's job.

## How it's put together

`BytesQuilt` is a sorted `Vec<Segment>` covering `[0, tail_offset)` back to back, plus `buffer_tail`, the open end where in-order writes append.

- A `Received` segment's buffer is full. A `Missing` segment's buffer is empty, and its capacity is the gap: that stretch of the allocation, held in place. `MissingSegment.length` is that capacity.
- A write into a gap splits the gap at the write's offset if it has to, `put`s into the empty buffer, and splits any leftover off as a new `Missing` segment.
- Every buffer is carved out of one allocation with `split`/`split_off`, and `into_inner` stitches them back together with `unsplit`. That's the whole zero-copy story.
- A run of in-order writes never gets internal boundaries. It's one span: the tail, and then the single `Received` segment the tail becomes when a gap opens after it. So whether an offset is already received can't be decided from span starts alone.
- The first segment always starts at offset 0 (`put_at` debug-asserts it), and the `segments[index - 1]` lookup after a missed binary search relies on that.

Sharp edges no signature tells you about:

- Size `with_capacity` for the whole payload. Opening a gap needs the gap's worth of unused capacity left in the tail, or `split_off` panics; `BytesQuilt::new()` has none, so its first write has to land at offset 0. Appending past capacity is fine, but it reallocates the tail and `unsplit` falls back to copying.
- `into_inner` with gaps still open panics in debug builds. In release it silently drops them and everything after shifts down. The TODO is to zero-fill them.
- The README's usage example is aspirational, and says so: `put_u8_at`, `put_bytes_at` and `put_u32` are wishlist, and the crate path is `bytes_quilt`. The rustdoc and tests are the real API.

## Working on it

- `cargo test`; filter by name or module path, e.g. `cargo test split_missing` or `cargo test missing_segment::`.
- Run tests in the default debug profile. Most of the invariants are `debug_assert!`s, which `--release` compiles out.
- `cargo clippy --all-targets` and `cargo fmt --check` are clean; keep them that way.
- 0.1.0 is on crates.io, published from `88da515` (untagged).
