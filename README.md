# pglue

The glue for TypeScript to PostgreSQL.

## Overview

- 🌟 [High performance](#benchmarks), fully asynchronous, written in modern TypeScript
- 🐢 First class Deno support
- 💬 Automatic query parameterisation
- 🌧️ Automatic query pipelining
- 📣 Listen/notify support
- 📤 Connection pool support

## Installation

```ts
import pglue from "https://git.lua.re/luaneko/pglue/raw/tag/v0.3.3/mod.ts";
// ...or from github:
import pglue from "https://raw.githubusercontent.com/luaneko/pglue/refs/tags/v0.3.3/mod.ts";
```

## Documentation

TODO: Write the documentation in more detail here.

## Benchmarks

Performance is generally on par with [postgres.js][1] and up to **5x faster** than [deno-postgres][2]. Keep in mind that database driver benchmarks are largely dependent on the database performance itself and does not necessarily represent accurate real-world performance.

Tested on a 4 core, 2800 MHz, x86_64-pc-linux-gnu, QEMU VM, with Deno 2.1.4 and PostgreSQL 17.1 on localhost:

Query `select * from pg_type`:

```
    CPU | Common KVM Processor v2.0
Runtime | Deno 2.1.4 (x86_64-unknown-linux-gnu)

benchmark       time/iter (avg)        iter/s      (min … max)           p75      p99     p995
--------------- ----------------------------- --------------------- --------------------------

group select n=1
pglue                    8.8 ms         113.8 (  7.2 ms …  11.8 ms)   9.7 ms  11.8 ms  11.8 ms
postgres.js             10.8 ms          92.3 (  8.1 ms …  22.0 ms)  11.2 ms  22.0 ms  22.0 ms
deno-postgres           38.9 ms          25.7 ( 23.5 ms …  51.9 ms)  40.3 ms  51.9 ms  51.9 ms

summary
  pglue
     1.23x faster than postgres.js
     4.42x faster than deno-postgres

group select n=5
pglue                   40.1 ms          25.0 ( 36.1 ms …  48.2 ms)  40.7 ms  48.2 ms  48.2 ms
postgres.js             48.7 ms          20.5 ( 38.9 ms …  61.2 ms)  52.7 ms  61.2 ms  61.2 ms
deno-postgres          184.7 ms           5.4 (166.5 ms … 209.5 ms) 190.7 ms 209.5 ms 209.5 ms

summary
  pglue
     1.22x faster than postgres.js
     4.61x faster than deno-postgres

group select n=10
pglue                   80.7 ms          12.4 ( 73.5 ms …  95.4 ms)  82.2 ms  95.4 ms  95.4 ms
postgres.js             89.1 ms          11.2 ( 82.5 ms … 101.7 ms)  94.4 ms 101.7 ms 101.7 ms
deno-postgres          375.3 ms           2.7 (327.4 ms … 393.9 ms) 390.7 ms 393.9 ms 393.9 ms

summary
  pglue
     1.10x faster than postgres.js
     4.65x faster than deno-postgres
```

Query `insert into my_table (a, b, c) values (${a}, ${b}, ${c})`:

```
group insert n=1
pglue                  259.2 µs         3,858 (165.4 µs …   2.8 ms) 258.0 µs 775.4 µs   2.8 ms
postgres.js            235.9 µs         4,239 (148.8 µs …   1.2 ms) 250.3 µs 577.4 µs 585.6 µs
deno-postgres          306.7 µs         3,260 (198.8 µs …   1.3 ms) 325.9 µs   1.0 ms   1.3 ms

summary
  pglue
     1.10x slower than postgres.js
     1.18x faster than deno-postgres

group insert n=10
pglue                  789.7 µs         1,266 (553.2 µs …   2.7 ms) 783.4 µs   2.4 ms   2.7 ms
postgres.js            755.6 µs         1,323 (500.5 µs …   3.4 ms) 795.0 µs   2.8 ms   3.4 ms
deno-postgres            2.2 ms         458.1 (  1.6 ms …   5.2 ms)   2.3 ms   4.8 ms   5.2 ms

summary
  pglue
     1.04x slower than postgres.js
     2.76x faster than deno-postgres

group insert n=100
pglue                    5.8 ms         172.0 (  3.2 ms …   9.9 ms)   6.8 ms   9.9 ms   9.9 ms
postgres.js             13.0 ms          76.8 (  8.6 ms …  20.8 ms)  15.4 ms  20.8 ms  20.8 ms
deno-postgres           18.5 ms          54.1 ( 14.3 ms …  32.1 ms)  20.0 ms  32.1 ms  32.1 ms

summary
  pglue
     2.24x faster than postgres.js
     3.18x faster than deno-postgres

group insert n=200
pglue                    8.8 ms         113.4 (  6.0 ms …  14.1 ms)  10.0 ms  14.1 ms  14.1 ms
postgres.js             28.2 ms          35.5 ( 21.1 ms …  47.0 ms)  29.6 ms  47.0 ms  47.0 ms
deno-postgres           37.0 ms          27.0 ( 32.0 ms …  48.1 ms)  39.4 ms  48.1 ms  48.1 ms

summary
  pglue
     3.20x faster than postgres.js
     4.20x faster than deno-postgres
```

[1]: https://github.com/porsager/postgres
[2]: https://github.com/denodrivers/postgres
