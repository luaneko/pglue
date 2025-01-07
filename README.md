# pglue

## Performance

pglue implements automatic query pipelining which makes it especially performant with many queries concurrently executed on a single connection.

## Benchmarks

Performance is generally on par with [postgres.js][1] and up to **4x faster** than [deno-postgres][2]. Keep in mind that database driver benchmarks are largely dependent on the database performance itself and does not necessarily represent accurate real-world performance.

Tested on a 4 core 2800 MHz x86_64-pc-linux-gnu QEMU VM with Deno 2.1.4 and local PostgreSQL 17.1 installation connected via TCP on localhost:

```
    CPU | Common KVM Processor v2.0
Runtime | Deno 2.1.4 (x86_64-unknown-linux-gnu)

benchmark       time/iter (avg)        iter/s      (min … max)           p75      p99     p995
--------------- ----------------------------- --------------------- --------------------------

group select n=1
pglue                    9.9 ms         101.1 (  7.9 ms …  17.8 ms)  10.2 ms  17.8 ms  17.8 ms
postgres.js              8.8 ms         114.2 (  7.0 ms …   9.5 ms)   9.1 ms   9.5 ms   9.5 ms
deno-postgres           37.4 ms          26.7 ( 25.3 ms …  42.8 ms)  39.2 ms  42.8 ms  42.8 ms

summary
  pglue
     1.13x slower than postgres.js
     3.78x faster than deno-postgres

group select n=5
pglue                   48.2 ms          20.8 ( 41.9 ms …  68.5 ms)  50.3 ms  68.5 ms  68.5 ms
postgres.js             43.6 ms          22.9 ( 38.1 ms …  57.3 ms)  48.6 ms  57.3 ms  57.3 ms
deno-postgres          186.5 ms           5.4 (138.4 ms … 213.2 ms) 193.6 ms 213.2 ms 213.2 ms

summary
  pglue
     1.11x slower than postgres.js
     3.87x faster than deno-postgres

group select n=10
pglue                   97.8 ms          10.2 ( 90.2 ms … 105.0 ms) 104.0 ms 105.0 ms 105.0 ms
postgres.js             93.8 ms          10.7 ( 80.9 ms … 107.7 ms) 106.1 ms 107.7 ms 107.7 ms
deno-postgres          333.9 ms           3.0 (205.6 ms … 394.9 ms) 377.4 ms 394.9 ms 394.9 ms

summary
  pglue
     1.04x slower than postgres.js
     3.42x faster than deno-postgres

group insert n=1
pglue                  237.5 µs         4,210 (143.9 µs …   1.3 ms) 249.2 µs 953.3 µs   1.3 ms
postgres.js            242.5 µs         4,124 (137.4 µs … 886.4 µs) 263.4 µs 762.8 µs 865.5 µs
deno-postgres          295.1 µs         3,389 (163.8 µs … 899.3 µs) 340.0 µs 641.7 µs 899.3 µs

summary
  pglue
     1.02x faster than postgres.js
     1.24x faster than deno-postgres

group insert n=10
pglue                    1.1 ms         869.6 (610.1 µs …   2.1 ms)   1.2 ms   2.0 ms   2.1 ms
postgres.js            755.9 µs         1,323 (387.6 µs …   4.7 ms) 805.4 µs   2.8 ms   4.7 ms
deno-postgres            2.3 ms         434.4 (  1.6 ms …  10.6 ms)   2.4 ms   6.5 ms  10.6 ms

summary
  pglue
     1.52x slower than postgres.js
     2.00x faster than deno-postgres

group insert n=100
pglue                    9.2 ms         109.0 (  5.5 ms …  15.6 ms)  10.4 ms  15.6 ms  15.6 ms
postgres.js             14.8 ms          67.4 (  9.6 ms …  35.8 ms)  16.6 ms  35.8 ms  35.8 ms
deno-postgres           18.8 ms          53.1 ( 14.5 ms …  25.8 ms)  20.9 ms  25.8 ms  25.8 ms

summary
  pglue
     1.62x faster than postgres.js
     2.05x faster than deno-postgres

group insert n=200
pglue                   15.0 ms          66.6 ( 11.1 ms …  19.0 ms)  16.7 ms  19.0 ms  19.0 ms
postgres.js             28.1 ms          35.6 ( 22.8 ms …  40.0 ms)  29.1 ms  40.0 ms  40.0 ms
deno-postgres           35.9 ms          27.9 ( 29.7 ms …  46.5 ms)  37.2 ms  46.5 ms  46.5 ms

summary
  pglue
     1.87x faster than postgres.js
     2.39x faster than deno-postgres
```

[1]: https://github.com/porsager/postgres
[2]: https://github.com/denodrivers/postgres
