import * as pglue from "./mod.ts";
import postgres_js from "https://deno.land/x/postgresjs/mod.js";
import * as deno_postgres from "https://deno.land/x/postgres/mod.ts";

const c_pglue = await pglue.connect(`postgres://test:test@localhost:5432/test`);

const c_pgjs = await postgres_js(
  `postgres://test:test@localhost:5432/test`
).reserve();

const c_denopg = new deno_postgres.Client({
  user: "test",
  database: "test",
  hostname: "localhost",
  password: "test",
  port: 5432,
});

await c_denopg.connect();

async function bench_select(
  b: Deno.BenchContext,
  n: number,
  q: () => PromiseLike<unknown>
) {
  await q();
  b.start();

  const tasks = [];
  for (let i = 0; i < n; i++) tasks.push(q());

  await Promise.all(tasks);
  b.end();
}

async function bench_insert(
  b: Deno.BenchContext,
  n: number,
  q: (a: string, b: boolean, c: number) => PromiseLike<unknown>
) {
  await q("prepare", false, 0);
  b.start();

  const tasks = [];
  for (let i = 0; i < n; i++)
    tasks.push(q(i.toString(16).repeat(5), i % 3 === 0, i));

  await Promise.all(tasks);
  b.end();
}

for (const n of [1, 5, 10]) {
  Deno.bench({
    name: `pglue`,
    group: `select n=${n}`,
    baseline: true,
    async fn(b) {
      await bench_select(b, n, () => c_pglue.query`select * from pg_type`);
    },
  });

  Deno.bench({
    name: `postgres-js`,
    group: `select n=${n}`,
    async fn(b) {
      await bench_select(b, n, () => c_pgjs`select * from pg_type`);
    },
  });

  Deno.bench({
    name: `deno-postgres`,
    group: `select n=${n}`,
    async fn(b) {
      await bench_select(
        b,
        n,
        () => c_denopg.queryArray`select * from pg_type`
      );
    },
  });
}

for (const n of [1, 10, 100, 200]) {
  Deno.bench({
    name: `pglue`,
    group: `insert n=${n}`,
    baseline: true,
    async fn(b) {
      await using _tx = await c_pglue.begin();
      await c_pglue.query`create table my_table (a text not null, b boolean not null, c integer not null)`;
      await bench_insert(b, n, (a, b, c) =>
        c_pglue.query`insert into my_table (a, b, c) values (${a}, ${b}, ${c})`.execute()
      );
    },
  });

  Deno.bench({
    name: `postgres-js`,
    group: `insert n=${n}`,
    async fn(b) {
      await c_pgjs`begin`;
      try {
        await c_pgjs`create table my_table (a text not null, b boolean not null, c integer not null)`;
        await bench_insert(b, n, (a, b, c) =>
          c_pgjs`insert into my_table (a, b, c) values (${a}, ${b}, ${c})`.execute()
        );
      } finally {
        await c_pgjs`rollback`;
      }
    },
  });

  Deno.bench({
    name: `deno-postgres`,
    group: `insert n=${n}`,
    async fn(b) {
      const tx = c_denopg.createTransaction(`my_tx`);
      await tx.begin();
      try {
        await tx.queryArray`create table my_table (a text not null, b boolean not null, c integer not null)`;
        await bench_insert(
          b,
          n,
          (a, b, c) =>
            tx.queryArray`insert into my_table (a, b, c) values (${a}, ${b}, ${c})`
        );
      } finally {
        await tx.rollback();
      }
    },
  });
}
