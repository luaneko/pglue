import pg_conn_str from "npm:pg-connection-string@^2.7.0";
import type * as v from "./valita.ts";
import { Pool, PoolOptions, Wire, WireOptions } from "./wire.ts";

export {
  Wire,
  WireOptions,
  WireError,
  PostgresError,
  Pool,
  PoolOptions,
  type WireEvents,
  type PoolEvents,
  type LogLevel,
  type Parameters,
  type Transaction,
  type Channel,
  type ChannelEvents,
  type NotificationHandler,
} from "./wire.ts";
export {
  type SqlFragment,
  type SqlType,
  type SqlTypeMap,
  SqlTypeError,
  sql,
  sql_types,
  sql_format,
  is_sql,
  Query,
  type Result,
  type Row,
  type Rows,
  type RowStream,
} from "./query.ts";

export default function postgres(s: string, options: Partial<Options> = {}) {
  return new Postgres(Options.parse(parse_conn(s, options), { mode: "strip" }));
}

function parse_conn(s: string, options: Partial<WireOptions>) {
  const {
    host,
    port,
    user,
    password,
    database,
    ssl: _ssl, // TODO: ssl support
    ...runtime_params
  } = s ? pg_conn_str.parse(s) : {};

  return {
    ...options,
    host: options.host ?? host,
    port: options.port ?? port,
    user: options.user ?? user,
    password: options.password ?? password,
    database: options.database ?? database,
    runtime_params: { ...runtime_params, ...options.runtime_params },
  };
}

postgres.connect = connect;

export async function connect(s: string, options: Partial<WireOptions> = {}) {
  return await new Wire(
    WireOptions.parse(parse_conn(s, options), { mode: "strip" })
  ).connect();
}

export type Options = v.Infer<typeof Options>;
export const Options = PoolOptions;

export class Postgres extends Pool {
  readonly #options;

  constructor(options: Options) {
    super(options);
    this.#options = options;
  }

  async connect(options: Partial<WireOptions> = {}) {
    return await new Wire(
      WireOptions.parse({ ...this.#options, ...options }, { mode: "strip" })
    )
      .on("log", (l, c, s) => this.emit("log", l, c, s))
      .connect();
  }
}
