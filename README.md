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
import pglue from "https://git.lua.re/luaneko/pglue/raw/tag/0.1.2/mod.ts";
// ...or from github:
import pglue from "https://raw.githubusercontent.com/luaneko/pglue/refs/tags/0.1.2/mod.ts";
```

## Documentation

TODO: Write the documentation in more detail here.

## Benchmarks

Performance is generally on par with [postgres-js][1] and up to **5x faster** than [deno-postgres][2]. Keep in mind that database driver benchmarks are largely dependent on the database performance itself and does not necessarily represent accurate real-world performance.

Tested on a 4 core, 2800 MHz, x86_64-pc-linux-gnu, QEMU VM, with Deno 2.1.4 and PostgreSQL 17.1 on localhost:

Query `select * from pg_type`:

```log
    CPU | Common KVM Processor v2.0
Runtime | Deno 2.1.4 (x86_64-unknown-linux-gnu)

benchmark       time/iter (avg)        iter/s      (min … max)           p75      p99     p995
--------------- ----------------------------- --------------------- --------------------------

group select n=1
pglue                    8.3 ms         120.4 (  7.2 ms …  14.4 ms)   8.5 ms  14.4 ms  14.4 ms
postgres-js             10.8 ms          92.3 (  8.1 ms …  26.5 ms)  10.7 ms  26.5 ms  26.5 ms
deno-postgres           37.1 ms          26.9 ( 33.4 ms …  41.3 ms)  38.5 ms  41.3 ms  41.3 ms

summary
  pglue
     1.30x faster than postgres-js
     4.47x faster than deno-postgres

group select n=5
pglue                   39.9 ms          25.1 ( 37.2 ms …  49.6 ms)  40.8 ms  49.6 ms  49.6 ms
postgres-js             42.4 ms          23.6 ( 36.5 ms …  61.8 ms)  44.2 ms  61.8 ms  61.8 ms
deno-postgres          182.5 ms           5.5 (131.9 ms … 211.8 ms) 193.4 ms 211.8 ms 211.8 ms

summary
  pglue
     1.06x faster than postgres-js
     4.57x faster than deno-postgres

group select n=10
pglue                   78.9 ms          12.7 ( 72.3 ms …  88.9 ms)  82.5 ms  88.9 ms  88.9 ms
postgres-js             92.0 ms          10.9 ( 77.6 ms … 113.6 ms) 101.2 ms 113.6 ms 113.6 ms
deno-postgres          326.6 ms           3.1 (208.8 ms … 406.0 ms) 388.8 ms 406.0 ms 406.0 ms

summary
  pglue
     1.17x faster than postgres-js
     4.14x faster than deno-postgres
```

Query `insert into my_table (a, b, c) values (${a}, ${b}, ${c})`:

```log
group insert n=1
pglue                  303.3 µs         3,297 (165.6 µs …   2.4 ms) 321.6 µs   1.1 ms   2.4 ms
postgres-js            260.4 µs         3,840 (132.9 µs …   2.7 ms) 276.4 µs   1.1 ms   2.7 ms
deno-postgres          281.6 µs         3,552 (186.1 µs …   1.5 ms) 303.8 µs 613.6 µs 791.8 µs

summary
  pglue
     1.17x slower than postgres-js
     1.08x slower than deno-postgres

group insert n=10
pglue                    1.1 ms         878.5 (605.5 µs …   3.2 ms)   1.1 ms   2.2 ms   3.2 ms
postgres-js            849.3 µs         1,177 (529.5 µs …  10.1 ms) 770.6 µs   3.0 ms  10.1 ms
deno-postgres            2.3 ms         439.4 (  1.4 ms …   4.9 ms)   2.5 ms   4.1 ms   4.9 ms

summary
  pglue
     1.34x slower than postgres-js
     2.00x faster than deno-postgres

group insert n=100
pglue                    8.3 ms         121.0 (  5.0 ms …  13.6 ms)   9.3 ms  13.6 ms  13.6 ms
postgres-js             13.0 ms          76.7 (  9.0 ms …  26.9 ms)  14.1 ms  26.9 ms  26.9 ms
deno-postgres           19.8 ms          50.5 ( 14.2 ms …  31.8 ms)  22.5 ms  31.8 ms  31.8 ms

summary
  pglue
     1.58x faster than postgres-js
     2.40x faster than deno-postgres

group insert n=200
pglue                   15.1 ms          66.2 (  9.4 ms …  21.1 ms)  16.8 ms  21.1 ms  21.1 ms
postgres-js             27.8 ms          36.0 ( 22.5 ms …  39.2 ms)  30.2 ms  39.2 ms  39.2 ms
deno-postgres           40.6 ms          24.6 ( 33.5 ms …  51.4 ms)  42.2 ms  51.4 ms  51.4 ms

summary
  pglue
     1.84x faster than postgres-js
     2.68x faster than deno-postgres
```

[1]: https://github.com/porsager/postgres
[2]: https://github.com/denodrivers/postgres
