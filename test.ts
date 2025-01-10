import pglue, { PostgresError, SqlTypeError } from "./mod.ts";
import { expect } from "jsr:@std/expect";
import { toText } from "jsr:@std/streams";

async function connect(params?: Record<string, string>) {
  const pg = await pglue.connect(`postgres://test:test@localhost:5432/test`, {
    runtime_params: { client_min_messages: "INFO", ...params },
  });

  return pg.on("log", (_level, ctx, msg) => {
    console.info(`${msg}`, ctx);
  });
}

Deno.test(`integers`, async () => {
  await using pg = await connect();
  await using _tx = await pg.begin();

  const [{ a, b, c }] = await pg.query`
    select
      ${"0x100"}::int2 as a,
      ${777}::int4 as b,
      ${{
        [Symbol.toPrimitive](hint: string) {
          expect(hint).toBe("number");
          return "1234";
        },
      }}::int8 as c
  `.first();

  expect(a).toBe(0x100);
  expect(b).toBe(777);
  expect(c).toBe(1234);

  const [{ large }] =
    await pg.query`select ${"10000000000000000"}::int8 as large`.first();

  expect(large).toBe(10000000000000000n);

  await expect(pg.query`select ${100000}::int2`).rejects.toThrow(SqlTypeError);
  await expect(pg.query`select ${"100000"}::text::int2`).rejects.toThrow(
    PostgresError
  );
});

Deno.test(`boolean`, async () => {
  await using pg = await connect();
  await using _tx = await pg.begin();

  const [{ a, b, c }] = await pg.query`
    select
      ${true}::bool as a,
      ${"n"}::bool as b,
      ${undefined}::bool as c
  `.first();

  expect(a).toBe(true);
  expect(b).toBe(false);
  expect(c).toBe(null);
});

Deno.test(`bytea`, async () => {
  await using pg = await connect();
  await using _tx = await pg.begin();

  const [{ string, array, buffer }] = await pg.query`
    select
      ${"hello, world"}::bytea as string,
      ${[1, 2, 3, 4, 5]}::bytea as array,
      ${Uint8Array.of(5, 4, 3, 2, 1)}::bytea as buffer
  `.first();

  expect(string).toEqual(new TextEncoder().encode("hello, world"));
  expect(array).toEqual(Uint8Array.of(1, 2, 3, 4, 5));
  expect(buffer).toEqual(Uint8Array.of(5, 4, 3, 2, 1));
});

Deno.test(`row`, async () => {
  await using pg = await connect();
  await using _tx = await pg.begin();

  expect(
    (
      await pg.query`create table my_table (a text not null, b text not null, c text not null)`
    ).tag
  ).toBe(`CREATE TABLE`);

  expect(
    (
      await pg.query`copy my_table from stdin`.stdin(
        `field a\tfield b\tfield c`
      )
    ).tag
  ).toBe(`COPY 1`);

  const [row] = await pg.query`select * from my_table`.first();
  {
    // columns by name
    const { a, b, c } = row;
    expect(a).toBe("field a");
    expect(b).toBe("field b");
    expect(c).toBe("field c");
  }
  {
    // columns by index
    const [a, b, c] = row;
    expect(a).toBe("field a");
    expect(b).toBe("field b");
    expect(c).toBe("field c");
  }

  const { readable, writable } = new TransformStream<Uint8Array>(
    {},
    new ByteLengthQueuingStrategy({ highWaterMark: 4096 }),
    new ByteLengthQueuingStrategy({ highWaterMark: 4096 })
  );
  await pg.query`copy my_table to stdout`.stdout(writable);
  expect(await toText(readable)).toBe(`field a\tfield b\tfield c\n`);
});

Deno.test(`sql injection`, async () => {
  await using pg = await connect();
  await using _tx = await pg.begin();

  const input = `injection'); drop table users; --`;

  expect((await pg.query`create table users (name text not null)`).tag).toBe(
    `CREATE TABLE`
  );

  expect((await pg.query`insert into users (name) values (${input})`).tag).toBe(
    `INSERT 0 1`
  );

  const [{ name }] = await pg.query<{ name: string }>`
    select name from users
  `.first();

  expect(name).toBe(input);
});

Deno.test(`listen/notify`, async () => {
  await using pg = await connect();
  const sent: string[] = [];

  await using ch = await pg.listen(`my channel`, (payload) => {
    expect(payload).toBe(sent.shift());
  });

  for (let i = 0; i < 5; i++) {
    const payload = `test payload ${i}`;
    sent.push(payload);
    await ch.notify(payload);
  }

  expect(sent.length).toBe(0);
});

Deno.test(`transactions`, async () => {
  await using pg = await connect();

  await pg.begin(async (pg) => {
    await pg.begin(async (pg, tx) => {
      await pg.query`create table my_table (field text not null)`;
      await tx.rollback();
    });

    await expect(pg.query`select * from my_table`).rejects.toThrow(
      PostgresError
    );
  });

  await expect(pg.query`select * from my_table`).rejects.toThrow(PostgresError);

  await pg.begin(async (pg) => {
    await pg.begin(async (pg, tx) => {
      await pg.begin(async (pg, tx) => {
        await pg.begin(async (pg) => {
          await pg.query`create table my_table (field text not null)`;
        });
        await tx.commit();
      });

      expect(await pg.query`select * from my_table`.count()).toBe(0);
      await tx.rollback();
    });

    await expect(pg.query`select * from my_table`).rejects.toThrow(
      PostgresError
    );
  });
});

Deno.test(`streaming`, async () => {
  await using pg = await connect();
  await using _tx = await pg.begin();

  await pg.query`create table my_table (field text not null)`;

  for (let i = 0; i < 20; i++) {
    await pg.query`insert into my_table (field) values (${i})`;
  }

  let i = 0;
  for await (const chunk of pg.query`select * from my_table`.chunked(5)) {
    expect(chunk.length).toBe(5);
    for (const row of chunk) expect(row.field).toBe(`${i++}`);
  }

  expect(i).toBe(20);
});

Deno.test(`simple`, async () => {
  await using pg = await connect();
  await using _tx = await pg.begin();

  const rows = await pg.query`
    create table my_table (field text not null);
    insert into my_table (field) values ('one'), ('two'), ('three');
    select * from my_table;
    select * from my_table where field = 'two';
  `.simple();

  expect(rows.length).toBe(4);

  const [{ field: a }, { field: b }, { field: c }, { field: d }] = rows;
  expect(a).toBe("one");
  expect(b).toBe("two");
  expect(c).toBe("three");
  expect(d).toBe("two");
});
