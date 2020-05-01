q(uantized)ringbuf(fer)
=======================

> A thread-safe variant of a classic circular buffer with a double-twist

qringbuf is a circular buffer variant, similar to (but not a derivative of)
[Bip Buffer][1] and [spsc-bip-buffer/bbqueue][2]. It provides a
concurrency-friendly, zero-copy abstraction of [io.ReadAtLeast()][3] over a
pre-allocated ring-buffer, populated asynchronously by a standalone goroutine.
Refer to the [implementation-notes diagrams][4] to get a quick overview of
how this works in practice.

This library is primarily designed for processing a series of arbitrary
streams, each comprised of variable-length records. Refer to the [Examples][5]
and to the [StartFill(…)][6] / [NextRegion(…)][7] combo for a crash-course in
usage patterns.

## Documentation

https://pkg.go.dev/github.com/ipfs/go-qringbuf

## Lead Maintainer

[Peter Rabbitson](https://github.com/ribasushi)

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)

[1]: https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist
[2]: https://andrea.lattuada.me/blog/2019/the-design-and-implementation-of-a-lock-free-ring-buffer-with-contiguous-reservations.html
[3]: https://pkg.go.dev/io?tab=doc#ReadAtLeast
[4]: https://pkg.go.dev/github.com/ipfs/go-qringbuf#hdr-Implementation_notes
[5]: https://pkg.go.dev/github.com/ipfs/go-qringbuf?tab=doc#hdr-Examples
[6]: https://pkg.go.dev/github.com/ipfs/go-qringbuf?tab=doc#QuantizedRingBuffer.StartFill
[7]: https://pkg.go.dev/github.com/ipfs/go-qringbuf?tab=doc#QuantizedRingBuffer.NextRegion
