q(uantized)ringbuf(fer)
=======================

> A thread-safe variant of a classic circular buffer with a double-twist

qringbuf is a circular buffer variant, similar to (but not a derivative of)
[Bip Buffer][1] and [spsc-bip-buffer/bbqueue][2]. Use this when multiple
clients require protection from under-reads in addition to all reads being
contiguous byte ranges.

## Documentation

https://pkg.go.dev/github.com/ipfs/go-qringbuf

## Lead Maintainer

[Peter Rabbitson](https://github.com/ribasushi)

## License

[SPDX-License-Identifier: Apache-2.0 OR MIT](LICENSE.md)

[1]: https://www.codeproject.com/Articles/3479/The-Bip-Buffer-The-Circular-Buffer-with-a-Twist
[2]: https://andrea.lattuada.me/blog/2019/the-design-and-implementation-of-a-lock-free-ring-buffer-with-contiguous-reservations.html
