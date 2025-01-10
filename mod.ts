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
import { sql_types, type SqlType, type SqlTypeMap } from "./query.ts";

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
  type SqlType,
  type SqlTypeMap,
  SqlTypeError,
  sql,
  is_sql,
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
  types?: SqlTypeMap;
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
  types: record(unknown())
    .optional(() => ({}))
    .map((types): SqlTypeMap => ({ ...sql_types, ...types })),
});

function parse_opts(s: string, opts: Options) {
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
    ...opts,
    host: opts.host ?? host ?? PGHOST ?? undefined,
    port: opts.port ?? port ?? PGPORT ?? undefined,
    user: opts.user ?? user ?? PGUSER ?? USER ?? undefined,
    password: opts.password ?? password ?? PGPASSWORD ?? undefined,
    database: opts.database ?? database ?? PGDATABASE ?? undefined,
    runtime_params: { ...runtime_params, ...opts.runtime_params },
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
