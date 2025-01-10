import postgres from "./mod.ts";

await using pool = postgres(`postgres://test:test@localhost:5432/test`, {
  runtime_params: { client_min_messages: "INFO" },
});

pool.on("log", (level, ctx, msg) => console.info(`${level}: ${msg}`, ctx));

await pool.begin(async (pg, tx) => {
  await pg.query`
    create table my_test (
      key integer primary key generated always as identity,
      data text not null
    )
  `;

  await pg.query`
    insert into my_test (data) values (${[1, 2, 3]}::bytea)
  `;

  console.log(await pg.query`select * from my_test`);
  await tx.rollback();
});
