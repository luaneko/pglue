import postgres from "./mod.ts";

await using pool = postgres(`postgres://test:test@localhost:5432/test`, {
  runtime_params: { client_min_messages: "INFO" },
});

pool.on("log", (level, ctx, msg) => console.info(`${level}: ${msg}`, ctx));

await pool.begin(async (pg) => {
  await pg.begin(async (pg) => {
    console.log(await pg.query`select * from pg_user`);
  });
});
