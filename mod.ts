import pg_conn_str from "npm:pg-connection-string@^2.7.0";
import { Pool, PoolOptions, Wire, WireOptions } from "./wire.ts";

export {
  Wire,
  WireOptions,
  WireError,
  Pool,
  PoolOptions,
  PostgresError,
  type Postgres,
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

export default function postgres(
  s: string,
  options: Partial<PoolOptions> = {}
) {
  return new Pool(PoolOptions.parse(parse_conn(s, options), { mode: "strip" }));
}

postgres.connect = connect;

export async function connect(s: string, options: Partial<WireOptions> = {}) {
  return await new Wire(
    WireOptions.parse(parse_conn(s, options), { mode: "strip" })
  ).connect();
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
