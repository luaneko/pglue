import pg_conn_string from "npm:pg-connection-string@^2.7.0";
import {
  type Infer,
  number,
  object,
  record,
  string,
  union,
  unknown,
} from "./valita.ts";
import { Pool, wire_connect, type LogLevel } from "./wire.ts";
import { type FromSql, type ToSql, from_sql, to_sql } from "./sql.ts";

export {
  WireError,
  PostgresError,
  type LogLevel,
  type Transaction,
  type Channel,
  type Parameters,
} from "./wire.ts";
export {
  type SqlFragment,
  type FromSql,
  type ToSql,
  SqlValue,
  sql,
  is_sql,
} from "./sql.ts";
export {
  Query,
  type Row,
  type CommandResult,
  type Result,
  type Results,
  type ResultStream,
} from "./query.ts";

export type Options = {
  host?: string;
  port?: number | string;
  user?: string;
  password?: string;
  database?: string | null;
  max_connections?: number;
  idle_timeout?: number;
  runtime_params?: Record<string, string>;
  from_sql?: FromSql;
  to_sql?: ToSql;
};

type ParsedOptions = Infer<typeof ParsedOptions>;
const ParsedOptions = object({
  host: string().optional(() => "localhost"),
  port: union(
    number(),
    string().map((s) => parseInt(s, 10))
  ).optional(() => 5432),
  user: string().optional(() => "postgres"),
  password: string().optional(() => "postgres"),
  database: string()
    .nullable()
    .optional(() => null),
  runtime_params: record(string()).optional(() => ({})),
  max_connections: number().optional(() => 10),
  idle_timeout: number().optional(() => 20),
  from_sql: unknown()
    .assert((s): s is FromSql => typeof s === "function")
    .optional(() => from_sql),
  to_sql: unknown()
    .assert((s): s is ToSql => typeof s === "function")
    .optional(() => to_sql),
});

function parse_opts(s: string, options: Options) {
  const {
    host,
    port,
    user,
    password,
    database,
    ssl: _ssl, // TODO:
    ...runtime_params
  } = pg_conn_string.parse(s);

  const { PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, USER } =
    Deno.env.toObject();

  return ParsedOptions.parse({
    ...options,
    host: options.host ?? host ?? PGHOST ?? undefined,
    port: options.port ?? port ?? PGPORT ?? undefined,
    user: options.user ?? user ?? PGUSER ?? USER ?? undefined,
    password: options.password ?? password ?? PGPASSWORD ?? undefined,
    database: options.database ?? database ?? PGDATABASE ?? undefined,
    runtime_params: { ...runtime_params, ...options.runtime_params },
  });
}

export default function postgres(s: string, options: Options = {}) {
  return new Postgres(parse_opts(s, options));
}

export function connect(s: string, options: Options = {}) {
  return wire_connect(parse_opts(s, options));
}

postgres.connect = connect;

export type PostgresEvents = {
  log(level: LogLevel, ctx: object, msg: string): void;
};

export class Postgres extends Pool {
  readonly #options;

  constructor(options: ParsedOptions) {
    super(options);
    this.#options = options;
  }

  async connect() {
    const wire = await wire_connect(this.#options);
    return wire.on("log", (l, c, s) => this.emit("log", l, c, s));
  }
}
