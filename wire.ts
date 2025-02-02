import * as v from "./valita.ts";
import { join } from "jsr:@std/path@^1.0.8";
import {
  type BinaryLike,
  buf_concat,
  buf_concat_fast,
  buf_eq,
  buf_xor,
  channel,
  from_base64,
  from_utf8,
  jit,
  type Receiver,
  semaphore,
  type Sender,
  to_base58,
  to_base64,
  to_hex,
  to_utf8,
  TypedEmitter,
} from "./lstd.ts";
import {
  array,
  byten,
  bytes_lp,
  bytes,
  char,
  cstring,
  type Encoder,
  object,
  type ObjectEncoder,
  type ObjectShape,
  oneof,
  ser_decode,
  ser_encode,
  i16,
  i32,
  i8,
  type EncoderType,
} from "./ser.ts";
import {
  format,
  is_sql,
  Query,
  type Result,
  type RowStream,
  type Row,
  sql,
  type SqlFragment,
  type SqlTypeMap,
  text,
  sql_types,
} from "./query.ts";

export class WireError extends Error {
  override get name() {
    return this.constructor.name;
  }
}

export class PostgresError extends WireError {
  readonly severity;
  readonly code;
  readonly detail;
  readonly hint;
  readonly position;
  readonly where;
  readonly schema;
  readonly table;
  readonly column;
  readonly data_type;
  readonly constraint;
  readonly file;
  readonly line;
  readonly routine;

  constructor(fields: Partial<Record<string, string>>) {
    // https://www.postgresql.org/docs/current/protocol-error-fields.html#PROTOCOL-ERROR-FIELDS
    const { S, V, C, M, D, H, P, W, s, t, c, d, n, F, L, R } = fields;
    super(M ?? "unknown error");
    this.severity = V ?? S ?? "ERROR";
    this.code = C ?? "XX000";
    this.detail = D ?? null;
    this.hint = H ?? null;
    this.position = P ?? null;
    this.where = W ?? null;
    this.schema = s ?? null;
    this.table = t ?? null;
    this.column = c ?? null;
    this.data_type = d ?? null;
    this.constraint = n ?? null;
    this.file = F ?? null;
    this.line = L ? parseInt(L, 10) : null;
    this.routine = R ?? null;
  }
}

function severity_log_level(s: string): LogLevel {
  switch (s) {
    case "DEBUG":
      return "debug";
    default:
    case "LOG":
    case "INFO":
    case "NOTICE":
      return "info";
    case "WARNING":
      return "warn";
    case "ERROR":
      return "error";
    case "FATAL":
    case "PANIC":
      return "fatal";
  }
}

interface MessageEncoder<T extends string, S extends ObjectShape>
  extends ObjectEncoder<S> {
  readonly type: T;
}

function msg<T extends string, S extends ObjectShape>(
  type: T,
  shape: S
): MessageEncoder<T, S> {
  const header_size = type !== "" ? 5 : 4;
  const ty = type !== "" ? oneof(char(i8), type) : null;
  const fields = object(shape);

  return {
    const_size: null,
    get type() {
      return type;
    },
    allocs(msg) {
      return header_size + fields.allocs(msg);
    },
    encode(buf, cur, msg) {
      ty?.encode(buf, cur, type);
      const { i } = cur;
      cur.i += 4;
      fields.encode(buf, cur, msg);
      i32.encode(buf, { i }, cur.i - i);
    },
    decode(buf, cur) {
      ty?.decode(buf, cur);
      const n = i32.decode(buf, cur) - 4;
      return fields.decode(buf.subarray(cur.i, (cur.i += n)), { i: 0 });
    },
  };
}

function msg_type({ 0: n }: Uint8Array) {
  return n === 0 ? "" : String.fromCharCode(n);
}

function msg_check_err(msg: Uint8Array) {
  if (msg_type(msg) === ErrorResponse.type) {
    const { fields } = ser_decode(ErrorResponse, msg);
    throw new PostgresError(fields);
  } else {
    return msg;
  }
}

// https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS
export const Header = object({
  type: char(i8),
  length: i32,
});

export const Authentication = msg("R", {
  status: i32,
});

export const AuthenticationOk = msg("R", {
  status: oneof(i32, 0 as const),
});

export const AuthenticationKerberosV5 = msg("R", {
  status: oneof(i32, 2 as const),
});

export const AuthenticationCleartextPassword = msg("R", {
  status: oneof(i32, 3 as const),
});

export const AuthenticationMD5Password = msg("R", {
  status: oneof(i32, 5 as const),
  salt: byten(4),
});

export const AuthenticationGSS = msg("R", {
  status: oneof(i32, 7 as const),
});

export const AuthenticationGSSContinue = msg("R", {
  status: oneof(i32, 8 as const),
  data: bytes,
});

export const AuthenticationSSPI = msg("R", {
  status: oneof(i32, 9 as const),
});

export const AuthenticationSASL = msg("R", {
  status: oneof(i32, 10 as const),
  mechanisms: {
    const_size: null,
    allocs(x) {
      let size = 1;
      for (const s of x) size += cstring.allocs(s);
      return size;
    },
    encode(buf, cur, x) {
      for (const s of x) cstring.encode(buf, cur, s);
      cstring.encode(buf, cur, "");
    },
    decode(buf, cur) {
      const x = [];
      for (let s; (s = cstring.decode(buf, cur)) !== ""; ) x.push(s);
      return x;
    },
  } satisfies Encoder<string[]>,
});

export const AuthenticationSASLContinue = msg("R", {
  status: oneof(i32, 11 as const),
  data: bytes,
});

export const AuthenticationSASLFinal = msg("R", {
  status: oneof(i32, 12 as const),
  data: bytes,
});

export const BackendKeyData = msg("K", {
  process_id: i32,
  secret_key: i32,
});

export const Bind = msg("B", {
  portal: cstring,
  statement: cstring,
  param_formats: array(i16, i16),
  param_values: array(i16, bytes_lp),
  column_formats: array(i16, i16),
});

export const BindComplete = msg("2", {});

export const CancelRequest = msg("", {
  code: oneof(i32, 80877102 as const),
  process_id: i32,
  secret_key: i32,
});

export const Close = msg("C", {
  which: oneof(char(i8), "S" as const, "P" as const),
  name: cstring,
});

export const CloseComplete = msg("3", {});
export const CommandComplete = msg("C", { tag: cstring });
export const CopyData = msg("d", { data: bytes });
export const CopyDone = msg("c", {});
export const CopyFail = msg("f", { cause: cstring });

export const CopyInResponse = msg("G", {
  format: i8,
  column_formats: array(i16, i16),
});

export const CopyOutResponse = msg("H", {
  format: i8,
  column_formats: array(i16, i16),
});

export const CopyBothResponse = msg("W", {
  format: i8,
  column_formats: array(i16, i16),
});

export const DataRow = msg("D", {
  column_values: array(i16, bytes_lp),
});

export const Describe = msg("D", {
  which: oneof(char(i8), "S" as const, "P" as const),
  name: cstring,
});

export const EmptyQueryResponse = msg("I", {});

const err_field = char(i8);
const err_fields: Encoder<Record<string, string>> = {
  const_size: null,
  allocs(x) {
    let size = 1;
    for (const { 0: key, 1: value } of Object.entries(x)) {
      size += err_field.allocs(key) + cstring.allocs(value);
    }
    return size;
  },
  encode(buf, cur, x) {
    for (const { 0: key, 1: value } of Object.entries(x)) {
      err_field.encode(buf, cur, key), cstring.encode(buf, cur, value);
    }
    err_field.encode(buf, cur, "");
  },
  decode(buf, cur) {
    const x: Record<string, string> = {};
    for (let key; (key = err_field.decode(buf, cur)) !== ""; ) {
      x[key] = cstring.decode(buf, cur);
    }
    return x;
  },
};

export const ErrorResponse = msg("E", {
  fields: err_fields,
});

export const Execute = msg("E", {
  portal: cstring,
  row_limit: i32,
});

export const Flush = msg("H", {});

export const FunctionCall = msg("F", {
  oid: i32,
  arg_formats: array(i16, i16),
  arg_values: array(i16, bytes_lp),
  result_format: i16,
});

export const FunctionCallResponse = msg("V", {
  result_value: bytes_lp,
});

export const NegotiateProtocolVersion = msg("v", {
  minor_ver: i32,
  bad_options: array(i32, cstring),
});

export const NoData = msg("n", {});

export const NoticeResponse = msg("N", {
  fields: err_fields,
});

export const NotificationResponse = msg("A", {
  process_id: i32,
  channel: cstring,
  payload: cstring,
});

export const ParameterDescription = msg("t", {
  param_types: array(i16, i32),
});

export const ParameterStatus = msg("S", {
  name: cstring,
  value: cstring,
});

export const Parse = msg("P", {
  statement: cstring,
  query: cstring,
  param_types: array(i16, i32),
});

export const ParseComplete = msg("1", {});

export const PasswordMessage = msg("p", {
  password: cstring,
});

export const PortalSuspended = msg("s", {});

export const QueryMessage = msg("Q", {
  query: cstring,
});

export const ReadyForQuery = msg("Z", {
  tx_status: oneof(char(i8), "I" as const, "T" as const, "E" as const),
});

export const RowDescription = msg("T", {
  columns: array(
    i16,
    object({
      name: cstring,
      table_oid: i32,
      table_column: i16,
      type_oid: i32,
      type_size: i16,
      type_modifier: i32,
      format: i16,
    })
  ),
});

export const SASLInitialResponse = msg("p", {
  mechanism: cstring,
  data: bytes_lp,
});

export const SASLResponse = msg("p", {
  data: bytes,
});

export const StartupMessage = msg("", {
  version: oneof(i32, 196608 as const),
  params: {
    const_size: null,
    allocs(x) {
      let size = 1;
      for (const { 0: key, 1: value } of Object.entries(x)) {
        size += cstring.allocs(key) + cstring.allocs(value);
      }
      return size;
    },
    encode(buf, cur, x) {
      for (const { 0: key, 1: value } of Object.entries(x)) {
        cstring.encode(buf, cur, key), cstring.encode(buf, cur, value);
      }
      i8.encode(buf, cur, 0);
    },
    decode(buf, cur) {
      const x: Record<string, string> = {};
      for (let key; (key = cstring.decode(buf, cur)) !== ""; ) {
        x[key] = cstring.decode(buf, cur);
      }
      return x;
    },
  } satisfies Encoder<Record<string, string>>,
});

export const Sync = msg("S", {});
export const Terminate = msg("X", {});

function getenv(name: string) {
  return Deno.env.get(name);
}

export type WireOptions = v.Infer<typeof WireOptions>;
export const WireOptions = v.object({
  host: v.string().optional(() => getenv("PGHOST") ?? "localhost"),
  port: v
    .union(v.string(), v.number())
    .optional(() => getenv("PGPORT") ?? 5432)
    .map(Number)
    .assert(Number.isSafeInteger, `invalid number`),
  user: v
    .string()
    .optional(() => getenv("PGUSER") ?? getenv("USER") ?? "postgres"),
  password: v.string().optional(() => getenv("PGPASSWORD") ?? "postgres"),
  database: v
    .string()
    .nullable()
    .optional(() => getenv("PGDATABASE") ?? null),
  runtime_params: v
    .record(v.string())
    .map((p) => ((p.application_name ??= "pglue"), p)),
  reconnect_delay: v
    .number()
    .optional(() => 5)
    .assert(Number.isSafeInteger, `invalid number`)
    .nullable(),
  types: v
    .record(v.unknown())
    .optional(() => ({}))
    .map((types): SqlTypeMap => ({ ...sql_types, ...types })),
  verbose: v.boolean().optional(() => false),
});

export type WireEvents = {
  log(level: LogLevel, ctx: object, msg: string): void;
  connect(): void;
  notice(notice: PostgresError): void;
  notify(channel: string, payload: string, process_id: number): void;
  parameter(name: string, value: string, prev: string | null): void;
  close(reason?: unknown): void;
};

export type LogLevel = "trace" | "debug" | "info" | "warn" | "error" | "fatal";

export interface Parameters extends Readonly<Partial<Record<string, string>>> {}

export interface Transaction extends Result, AsyncDisposable {
  readonly open: boolean;
  commit(): Promise<Result>;
  rollback(): Promise<Result>;
}

export type NotificationHandler = (payload: string, process_id: number) => void;
export type ChannelEvents = { notify: NotificationHandler };
export interface Channel
  extends TypedEmitter<ChannelEvents>,
    Result,
    AsyncDisposable {
  readonly name: string;
  readonly open: boolean;
  notify(payload: string): Promise<Result>;
  unlisten(): Promise<Result>;
}

export interface Postgres {
  query<T = Row>(sql: SqlFragment): Query<T>;
  query<T = Row>(s: TemplateStringsArray, ...xs: unknown[]): Query<T>;

  begin(): Promise<Transaction>;
  begin<T>(
    f: (pg: Postgres, tx: Transaction) => T | PromiseLike<T>
  ): Promise<T>;
}

export class Wire<V extends WireEvents = WireEvents>
  extends TypedEmitter<V>
  implements Postgres, Disposable
{
  readonly #options;
  readonly #params;
  readonly #connect;
  readonly #query;
  readonly #begin;
  readonly #listen;
  readonly #notify;
  readonly #close;

  get params() {
    return this.#params;
  }

  constructor(options: WireOptions) {
    super();
    ({
      params: this.#params,
      connect: this.#connect,
      query: this.#query,
      begin: this.#begin,
      listen: this.#listen,
      notify: this.#notify,
      close: this.#close,
    } = wire_impl(this, (this.#options = options)));
  }

  async connect() {
    return await this.#connect(), this;
  }

  query<T = Row>(sql: SqlFragment): Query<T>;
  query<T = Row>(s: TemplateStringsArray, ...xs: unknown[]): Query<T>;
  query(s: TemplateStringsArray | SqlFragment, ...xs: unknown[]) {
    return this.#query(is_sql(s) ? s : sql(s, ...xs));
  }

  begin(): Promise<Transaction>;
  begin<T>(f: (wire: this, tx: Transaction) => T | PromiseLike<T>): Promise<T>;
  async begin(f?: (wire: this, tx: Transaction) => unknown) {
    if (typeof f !== "undefined") {
      await using tx = await this.#begin();
      const value = await f(this, tx);
      if (tx.open) await tx.commit();
      return value;
    } else {
      return this.#begin();
    }
  }

  async listen(channel: string, ...fs: NotificationHandler[]) {
    const ch = await this.#listen(channel);
    for (const f of fs) ch.on("notify", f);
    return ch;
  }

  notify(channel: string, payload: string) {
    return this.#notify(channel, payload);
  }

  async current_setting(name: string) {
    return await this.query<
      [string]
    >`select current_setting(${name}::text, true)`
      .map(([x]) => x)
      .first_or(null);
  }

  async set_config(name: string, value: string, local = false) {
    return await this.query<
      [string]
    >`select set_config(${name}::text, ${value}::text, ${local}::boolean)`
      .map(([x]) => x)
      .first();
  }

  async cancel_backend(pid: number) {
    return await this.query<
      [boolean]
    >`select pg_cancel_backend(${pid}::integer)`
      .map(([x]) => x)
      .first();
  }

  async terminate_backend(pid: number, timeout = 0) {
    return await this.query<
      [boolean]
    >`select pg_terminate_backend(${pid}::integer, ${timeout}::bigint)`
      .map(([x]) => x)
      .first();
  }

  async inet() {
    return await this.query<{
      client_addr: string;
      client_port: number;
      server_addr: string;
      server_port: number;
    }>`
      select
        inet_client_addr() as client_addr,
        inet_client_port() as client_port,
        inet_server_addr() as server_addr,
        inet_server_port() as server_por
    `.first();
  }

  async listening_channels() {
    return await this.query<[string]>`select pg_listening_channels()`
      .map(([x]) => x)
      .collect();
  }

  async notification_queue_usage() {
    return await this.query<[number]>`select pg_notification_queue_usage()`
      .map(([x]) => x)
      .first();
  }

  async postmaster_start_time() {
    return await this.query<[Date]>`select pg_postmaster_start_time()`
      .map(([x]) => x)
      .first();
  }

  async current_wal() {
    return await this.query<{
      lsn: string;
      insert_lsn: string;
      flush_lsn: string;
    }>`
      select
        pg_current_wal_lsn() as lsn,
        pg_current_wal_insert_lsn() as insert_lsn,
        pg_current_wal_flush_lsn() as flush_lsn
    `.first();
  }

  async switch_wal() {
    return await this.query<[string]>`select pg_switch_wal()`
      .map(([x]) => x)
      .first();
  }

  async nextval(seq: string) {
    return await this.query<[number | bigint]>`select nextval(${seq}::regclass)`
      .map(([x]) => x)
      .first();
  }

  async setval(seq: string, value: number | bigint, is_called = true) {
    return await this.query<
      [number | bigint]
    >`select setval(${seq}::regclass, ${value}::bigint, ${is_called}::boolean)`
      .map(([x]) => x)
      .first();
  }

  async currval(seq: string) {
    return await this.query<[number]>`select currval(${seq}::regclass)`
      .map(([x]) => x)
      .first();
  }

  async lastval() {
    return await this.query<[number]>`select lastval()`.map(([x]) => x).first();
  }

  async validate_input(s: string, type: string) {
    return await this.query<{
      message: string | null;
      detail: string | null;
      hint: string | null;
      sql_error_code: string | null;
    }>`select * from pg_input_error_info(${s}::text, ${type}::text)`.first();
  }

  async current_xid() {
    return await this.query<[number | bigint]>`select pg_current_xact_id()`
      .map(([x]) => x)
      .first();
  }

  async current_xid_if_assigned() {
    return await this.query<
      [number | bigint | null]
    >`select pg_current_xact_id_if_assigned()`
      .map(([x]) => x)
      .first();
  }

  async xact_info(xid: number | bigint) {
    return await this.query<{
      status: "progress" | "committed" | "aborted";
      age: number;
      mxid_age: number;
    }>`
      select
        pg_xact_status(${xid}::xid8) as status,
        age(${xid}::xid) as age,
        mxid_age(${xid}::xid) as mxid_age
    `;
  }

  async version() {
    return await this.query<{
      postgres: string;
      unicode: string;
      icu_unicode: string | null;
    }>`
      select
        version() as postgres,
        unicode_version() as unicode,
        icu_unicode_version() as icu_unicode
    `.first();
  }

  close(reason?: unknown) {
    this.#close(reason);
  }

  [Symbol.dispose]() {
    this.close();
  }
}

function randstr(entropy: number) {
  return to_base58(crypto.getRandomValues(new Uint8Array(entropy)));
}

async function socket_connect(hostname: string, port: number) {
  if (hostname.startsWith("/")) {
    const path = join(hostname, `.s.PGSQL.${port}`);
    return await Deno.connect({ transport: "unix", path });
  } else {
    const socket = await Deno.connect({ transport: "tcp", hostname, port });
    return socket.setNoDelay(), socket.setKeepAlive(), socket;
  }
}

function wire_impl(
  wire: Wire,
  {
    host,
    port,
    user,
    database,
    password,
    runtime_params,
    reconnect_delay,
    types,
    verbose,
  }: WireOptions
) {
  const params: Parameters = Object.create(null);

  function log(level: LogLevel, ctx: object, msg: string) {
    wire.emit("log", level, ctx, msg);
  }

  let connected = false;
  let close_requested = false;
  let read_queue: Receiver<Uint8Array> | null = null;
  let write_queue: Sender<Uint8Array> | null = null;

  async function connect() {
    using _rlock = await rlock();
    using _wlock = await wlock();
    if (connected) return;
    else close_requested = false;
    let socket: Deno.Conn | undefined;
    let closed = false;

    try {
      const read = channel<Uint8Array>();
      const write = channel<Uint8Array>();
      socket = await socket_connect(host, port);
      read_queue?.close(), (read_queue = read.recv);
      write_queue?.close(), (write_queue = write.send);
      read_socket(socket, read.send).then(onclose, onclose);
      write_socket(socket, write.recv).then(onclose, onclose);
      await handle_auth(); // run auth with rw lock

      if (close_requested) throw new WireError(`close requested`);
      else (connected = true), wire.emit("connect");
    } catch (e) {
      throw (onclose(e), e);
    }

    function onclose(reason?: unknown) {
      if (closed) return;
      else closed = true;
      socket?.close();
      for (const name of Object.keys(params))
        delete (params as Record<string, string>)[name];
      st_cache.clear(), (st_ids = 0);
      (tx_status = "I"), (tx_stack.length = 0);
      connected &&= (wire.emit("close", reason), reconnect(), false);
    }
  }

  let reconnect_timer = -1;
  function reconnect() {
    if (close_requested || reconnect_delay === null) return;
    connect().catch((err) => {
      log("warn", err, `reconnect failed`);
      clearTimeout(reconnect_timer);
      reconnect_timer = setTimeout(reconnect, reconnect_delay);
    });
  }

  function close(reason?: unknown) {
    close_requested = true;
    clearTimeout(reconnect_timer);
    read_queue?.close(reason), (read_queue = null);
    write_queue?.close(reason), (write_queue = null);
  }

  async function read<T>(type: Encoder<T>) {
    const msg = read_queue !== null ? await read_queue() : null;
    if (msg !== null) return ser_decode(type, msg_check_err(msg));
    else throw new WireError(`connection closed`);
  }

  async function read_any() {
    const msg = read_queue !== null ? await read_queue() : null;
    if (msg !== null) return msg;
    else throw new WireError(`connection closed`);
  }

  async function read_socket(socket: Deno.Conn, send: Sender<Uint8Array>) {
    const header_size = 5;
    const read_buf = new Uint8Array(64 * 1024); // shared buffer for all socket reads
    let buf = new Uint8Array(); // concatenated messages read so far

    for (let read; (read = await socket.read(read_buf)) !== null; ) {
      buf = buf_concat_fast(buf, read_buf.subarray(0, read)); // push read bytes to buf
      while (buf.length >= header_size) {
        const size = ser_decode(Header, buf).length + 1;
        if (buf.length < size) break;
        const msg = buf.subarray(0, size); // shift one message from buf
        buf = buf.subarray(size);
        if (verbose)
          log("trace", {}, `RECV <- ${msg_type(msg)} ${to_hex(msg)}`);
        if (!handle_msg(msg)) send(msg);
      }
    }

    // there should be nothing left in buf if we gracefully exited
    if (buf.length !== 0) throw new WireError(`unexpected end of stream`);
  }

  function handle_msg(msg: Uint8Array) {
    switch (msg_type(msg)) {
      // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC
      case NoticeResponse.type: {
        const { fields } = ser_decode(NoticeResponse, msg);
        const notice = new PostgresError(fields);
        log(severity_log_level(notice.severity), notice, notice.message);
        wire.emit("notice", notice);
        return true;
      }

      case NotificationResponse.type: {
        const { channel, payload, process_id } = ser_decode(
          NotificationResponse,
          msg
        );
        wire.emit("notify", channel, payload, process_id);
        channels.get(channel)?.emit("notify", payload, process_id);
        return true;
      }

      case ParameterStatus.type: {
        const { name, value } = ser_decode(ParameterStatus, msg);
        const prev = params[name] ?? null;
        Object.defineProperty(params, name, {
          configurable: true,
          enumerable: true,
          value,
        });
        wire.emit("parameter", name, value, prev);
        return true;
      }

      default:
        return false;
    }
  }

  function write<T>(type: Encoder<T>, value: T) {
    if (write_queue !== null) write_queue(ser_encode(type, value));
    else throw new WireError(`connection closed`);
  }

  async function write_socket(socket: Deno.Conn, recv: Receiver<Uint8Array>) {
    for (let buf; (buf = await recv()) !== null; ) {
      const msgs = [buf]; // proactively dequeue more queued msgs synchronously, if any
      for (let i = 1, buf; (buf = recv.try()) !== null; ) msgs[i++] = buf;
      if (verbose) {
        for (const msg of msgs)
          log("trace", {}, `SEND -> ${msg_type(msg)} ${to_hex(msg)}`);
      }
      if (msgs.length !== 1) buf = buf_concat(msgs); // write queued msgs concatenated, reduce write syscalls
      for (let i = 0, n = buf.length; i < n; )
        i += await socket.write(buf.subarray(i));
    }
  }

  // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING
  const rlock = semaphore();
  const wlock = semaphore();

  function pipeline<T>(
    w: () => void | PromiseLike<void>,
    r: () => T | PromiseLike<T>
  ) {
    return new Promise<T>((res, rej) => {
      pipeline_write(w).catch(rej);
      pipeline_read(r).then(res, rej);
    });
  }

  async function pipeline_read<T>(r: () => T | PromiseLike<T>) {
    using _lock = await rlock();
    try {
      return await r();
    } finally {
      try {
        let msg;
        while (msg_type((msg = await read_any())) !== ReadyForQuery.type);
        ({ tx_status } = ser_decode(ReadyForQuery, msg));
      } catch {
        // ignored
      }
    }
  }

  async function pipeline_write<T>(w: () => T | PromiseLike<T>) {
    using _lock = await wlock();
    try {
      return await w();
    } finally {
      try {
        write(Sync, {});
      } catch {
        // ignored
      }
    }
  }

  // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-START-UP
  async function handle_auth() {
    // always run within rw lock (see connect())
    write(StartupMessage, {
      version: 196608,
      params: {
        application_name: "pglue",
        idle_session_timeout: "0",
        ...runtime_params,
        user,
        database: database ?? user,
        bytea_output: "hex",
        client_encoding: "utf8",
        DateStyle: "ISO",
      },
    });

    auth: for (;;) {
      const msg = msg_check_err(await read_any());
      switch (msg_type(msg)) {
        case NegotiateProtocolVersion.type: {
          const { bad_options } = ser_decode(NegotiateProtocolVersion, msg);
          log("info", { bad_options }, `unrecognised protocol options`);
          continue;
        }
      }

      const { status } = ser_decode(Authentication, msg);
      switch (status) {
        case 0: // AuthenticationOk
          break auth;

        case 2: // AuthenticationKerberosV5
          throw new WireError(`kerberos authentication is deprecated`);

        case 3: // AuthenticationCleartextPassword
          write(PasswordMessage, { password });
          continue;

        case 5: // AuthenticationMD5Password
          throw new WireError(
            `md5 password authentication is deprecated (prefer scram-sha-256 instead)`
          );

        case 7: // AuthenticationGSS
          throw new WireError(`gssapi authentication is not supported`);

        case 9: // AuthenticationSSPI
          throw new WireError(`sspi authentication is not supported`);

        // AuthenticationSASL
        case 10:
          await handle_auth_sasl();
          continue;

        default:
          throw new WireError(`invalid authentication status ${status}`);
      }
    }

    // wait for ready
    ready: for (;;) {
      const msg = msg_check_err(await read_any());
      switch (msg_type(msg)) {
        case BackendKeyData.type:
          continue; // ignored

        default:
          ser_decode(ReadyForQuery, msg);
          break ready;
      }
    }

    // re-listen previously registered channels
    await Promise.all(
      channels
        .keys()
        .map((name) => query(sql`listen ${sql.ident(name)}`).execute())
    );
  }

  // https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256
  // https://datatracker.ietf.org/doc/html/rfc5802
  async function handle_auth_sasl() {
    const bits = 256;
    const hash = `SHA-${bits}`;
    const mechanism = `SCRAM-${hash}`;

    async function hmac(key: Uint8Array, str: string | Uint8Array) {
      return new Uint8Array(
        await crypto.subtle.sign(
          "HMAC",
          await crypto.subtle.importKey(
            "raw",
            key,
            { name: "HMAC", hash },
            false,
            ["sign"]
          ),
          to_utf8(str)
        )
      );
    }

    async function h(str: string | Uint8Array) {
      return new Uint8Array(await crypto.subtle.digest(hash, to_utf8(str)));
    }

    async function hi(str: string | Uint8Array, salt: Uint8Array, i: number) {
      return new Uint8Array(
        await crypto.subtle.deriveBits(
          { name: "PBKDF2", hash, salt, iterations: i },
          await crypto.subtle.importKey("raw", to_utf8(str), "PBKDF2", false, [
            "deriveBits",
          ]),
          bits
        )
      );
    }

    function parse_attrs(s: string) {
      const attrs: Partial<Record<string, string>> = {};
      for (const entry of s.split(",")) {
        const { 0: name, 1: value = "" } = entry.split("=", 2);
        attrs[name] = value;
      }
      return attrs;
    }

    const gs2_cbind_flag = `n`;
    const gs2_header = `${gs2_cbind_flag},,`;
    const username = `n=*`;
    const cbind_data = ``;
    const cbind_input = `${gs2_header}${cbind_data}`;
    const channel_binding = `c=${to_base64(cbind_input)}`;
    const initial_nonce = `r=${randstr(20)}`;
    const client_first_message_bare = `${username},${initial_nonce}`;
    const client_first_message = `${gs2_header}${client_first_message_bare}`;
    write(SASLInitialResponse, { mechanism, data: client_first_message });

    const server_first_message_str = from_utf8(
      (await read(AuthenticationSASLContinue)).data
    );
    const server_first_message = parse_attrs(server_first_message_str);
    const nonce = `r=${server_first_message.r ?? ""}`;
    if (!nonce.startsWith(initial_nonce)) throw new WireError(`bad nonce`);
    const salt = from_base64(server_first_message.s ?? "");
    const iters = parseInt(server_first_message.i ?? "", 10) || 0;
    const salted_password = await hi(password, salt, iters);
    const client_key = await hmac(salted_password, "Client Key");
    const stored_key = await h(client_key);
    const client_final_message_without_proof = `${channel_binding},${nonce}`;
    const auth_message = `${client_first_message_bare},${server_first_message_str},${client_final_message_without_proof}`;
    const client_signature = await hmac(stored_key, auth_message);
    const client_proof = buf_xor(client_key, client_signature);
    const proof = `p=${to_base64(client_proof)}`;
    const client_final_message = `${client_final_message_without_proof},${proof}`;
    write(SASLResponse, { data: client_final_message });

    const server_key = await hmac(salted_password, "Server Key");
    const server_signature = await hmac(server_key, auth_message);
    const server_final_message = parse_attrs(
      from_utf8((await read(AuthenticationSASLFinal)).data)
    );

    if (!buf_eq(from_base64(server_final_message.v ?? ""), server_signature))
      throw new WireError(`SASL server signature mismatch`);
  }

  // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
  const st_cache = new Map<string, Statement>();
  let st_ids = 0;

  class Statement {
    readonly name = `__st${st_ids++}`;
    constructor(readonly query: string) {}

    #parse_task: Promise<{
      ser_params: ParameterSerializer;
      Row: RowConstructor;
    }> | null = null;

    parse() {
      return (this.#parse_task ??= this.#parse());
    }

    async #parse() {
      try {
        const { name, query } = this;
        return await pipeline(
          () => {
            write(Parse, { statement: name, query, param_types: [] });
            write(Describe, { which: "S", name });
          },
          async () => {
            await read(ParseComplete);
            const ser_params = make_param_ser(await read(ParameterDescription));

            const msg = msg_check_err(await read_any());
            const Row =
              msg_type(msg) === NoData.type
                ? EmptyRow
                : make_row_ctor(ser_decode(RowDescription, msg));

            return { ser_params, Row };
          }
        );
      } catch (e) {
        throw ((this.#parse_task = null), e);
      }
    }

    #portals = 0;
    portal() {
      return `${this.name}_${this.#portals++}`;
    }
  }

  type ParameterDescription = EncoderType<typeof ParameterDescription>;
  interface ParameterSerializer {
    (params: unknown[]): (string | null)[];
  }

  // makes function to serialize query parameters
  function make_param_ser({ param_types }: ParameterDescription) {
    return jit.compiled<ParameterSerializer>`function ser_params(xs) {
      return [
        ${jit.map(", ", param_types, (type_oid, i) => {
          const type = types[type_oid] ?? types[0] ?? text;
          return jit`${type}.output(xs[${i}])`;
        })}
      ];
    }`;
  }

  type RowDescription = EncoderType<typeof RowDescription>;
  interface RowConstructor {
    new (columns: (BinaryLike | null)[]): Row;
  }

  // makes function to create Row objects
  const EmptyRow = make_row_ctor({ columns: [] });
  function make_row_ctor({ columns }: RowDescription) {
    const Row = jit.compiled<RowConstructor>`function Row(xs) {
      ${jit.map(" ", columns, ({ name, type_oid }, i) => {
        const type = types[type_oid] ?? types[0] ?? text;
        return jit`this[${name}] = xs[${i}] === null ? null : ${type}.input(${from_utf8}(xs[${i}]));`;
      })}
    }`;

    Row.prototype = Object.create(null, {
      [Symbol.toStringTag]: {
        configurable: true,
        value: `Row`,
      },
      [Symbol.toPrimitive]: {
        configurable: true,
        value: function format() {
          return [...this].join("\t");
        },
      },
      [Symbol.iterator]: {
        configurable: true,
        value: jit.compiled`function* iter() {
          ${jit.map(" ", columns, ({ name }) => {
            return jit`yield this[${name}];`;
          })}
        }`,
      },
    });

    return Row;
  }

  async function read_rows(
    Row: RowConstructor,
    stdout: WritableStream<Uint8Array> | null
  ) {
    for (let rows = [], i = 0; ; ) {
      const msg = msg_check_err(await read_any());
      switch (msg_type(msg)) {
        default:
        case DataRow.type:
          rows[i++] = new Row(ser_decode(DataRow, msg).column_values);
          continue;

        case CommandComplete.type: {
          const { tag } = ser_decode(CommandComplete, msg);
          return { done: true as const, rows, tag };
        }

        case PortalSuspended.type:
          return { done: false as const, rows, tag: "" };

        case EmptyQueryResponse.type:
          return { done: true as const, rows, tag: "" };

        case RowDescription.type:
          Row = make_row_ctor(ser_decode(RowDescription, msg));
          continue;

        case CopyInResponse.type:
          continue;

        case CopyOutResponse.type:
        case CopyBothResponse.type:
          await read_copy_out(stdout), (stdout = null);
          continue;
      }
    }
  }

  async function read_copy_out(stream: WritableStream<Uint8Array> | null) {
    const writer = stream?.getWriter();
    try {
      copy: for (;;) {
        const msg = msg_check_err(await read_any());
        switch (msg_type(msg)) {
          default:
          case CopyData.type: {
            const { data } = ser_decode(CopyData, msg);
            console.log(`COPY OUT`, to_hex(data));
            await writer?.write(to_utf8(data));
            continue;
          }

          case CopyDone.type:
          case CommandComplete.type: // walsender sends 'C' to end of CopyBothResponse
            await writer?.close();
            break copy;
        }
      }
    } catch (e) {
      await writer?.abort(e);
      throw e;
    } finally {
      writer?.releaseLock();
    }
  }

  async function write_copy_in(stream: ReadableStream<Uint8Array> | null) {
    const reader = stream?.getReader();
    try {
      if (reader) {
        for (let next; !(next = await reader.read()).done; )
          write(CopyData, { data: next.value });
      }
      write(CopyDone, {});
    } catch (e) {
      write(CopyFail, { cause: String(e) });
      reader?.cancel(e);
      throw e;
    } finally {
      reader?.releaseLock();
    }
  }

  async function* execute_simple(
    query: string,
    stdin: ReadableStream<Uint8Array> | null,
    stdout: WritableStream<Uint8Array> | null
  ): RowStream<Row> {
    yield* await pipeline(
      () => {
        log("debug", { query }, `executing simple query`);
        write(QueryMessage, { query });
        return write_copy_in(stdin);
      },
      async () => {
        for (let chunks = [], err; ; ) {
          const msg = await read_any();
          switch (msg_type(msg)) {
            default:
            case ReadyForQuery.type:
              ser_decode(ReadyForQuery, msg);
              if (err) throw err;
              else return chunks;

            case RowDescription.type: {
              const Row = make_row_ctor(ser_decode(RowDescription, msg));
              const { rows } = await read_rows(Row, stdout);
              chunks.push(rows), (stdout = null);
              continue;
            }

            case EmptyQueryResponse.type:
            case CommandComplete.type:
            case CopyInResponse.type:
            case CopyDone.type:
              continue;

            case CopyOutResponse.type:
            case CopyBothResponse.type:
              await read_copy_out(stdout), (stdout = null);
              continue;

            case ErrorResponse.type: {
              const { fields } = ser_decode(ErrorResponse, msg);
              err = new PostgresError(fields);
              continue;
            }
          }
        }
      }
    );

    return { tag: "" };
  }

  async function* execute_fast(
    st: Statement,
    params: unknown[],
    stdin: ReadableStream<Uint8Array> | null,
    stdout: WritableStream<Uint8Array> | null
  ): RowStream<Row> {
    const { query, name: statement } = st;
    const { ser_params, Row } = await st.parse();
    const param_values = ser_params(params);
    const portal = st.portal();

    try {
      const { rows, tag } = await pipeline(
        async () => {
          log("debug", { query, statement, params }, `executing query`);
          write(Bind, {
            portal,
            statement: st.name,
            param_formats: [],
            param_values,
            column_formats: [],
          });
          write(Execute, { portal, row_limit: 0 });
          await write_copy_in(stdin);
          write(Close, { which: "P", name: portal });
        },
        async () => {
          await read(BindComplete);
          return read_rows(Row, stdout);
        }
      );

      if (rows.length) yield rows;
      return { tag };
    } catch (e) {
      try {
        await pipeline(
          () => write(Close, { which: "P", name: portal }),
          () => read(CloseComplete)
        );
      } catch {
        // ignored
      }

      throw e;
    }
  }

  async function* execute_chunked(
    st: Statement,
    params: unknown[],
    chunk_size: number,
    stdin: ReadableStream<Uint8Array> | null,
    stdout: WritableStream<Uint8Array> | null
  ): RowStream<Row> {
    const { query, name: statement } = st;
    const { ser_params, Row } = await st.parse();
    const param_values = ser_params(params);
    const portal = st.portal();

    try {
      let { done, rows, tag } = await pipeline(
        () => {
          log("debug", { query, statement, params }, `executing chunked query`);
          write(Bind, {
            portal,
            statement: st.name,
            param_formats: [],
            param_values,
            column_formats: [],
          });
          write(Execute, { portal, row_limit: chunk_size });
          return write_copy_in(stdin);
        },
        async () => {
          await read(BindComplete);
          return read_rows(Row, stdout);
        }
      );

      if (rows.length) yield rows;

      while (!done) {
        ({ done, rows, tag } = await pipeline(
          () => write(Execute, { portal, row_limit: chunk_size }),
          () => read_rows(Row, stdout)
        ));

        if (rows.length) yield rows;
      }

      return { tag };
    } finally {
      await pipeline(
        () => write(Close, { which: "P", name: portal }),
        () => read(CloseComplete)
      );
    }
  }

  function query(sql: SqlFragment) {
    return new Query(
      ({ simple = false, chunk_size = 0, stdin = null, stdout = null }) => {
        const { query, params } = format(sql);
        if (simple) {
          if (!params.length) return execute_simple(query, stdin, stdout);
          else throw new WireError(`simple query cannot be parameterised`);
        }

        let st = st_cache.get(query);
        if (!st) st_cache.set(query, (st = new Statement(query)));
        if (!chunk_size) return execute_fast(st, params, stdin, stdout);
        else return execute_chunked(st, params, chunk_size, stdin, stdout);
      }
    );
  }

  // https://www.postgresql.org/docs/current/sql-begin.html
  // https://www.postgresql.org/docs/current/sql-savepoint.html
  let tx_status: "I" | "T" | "E" = "I";
  const tx_stack: Transaction[] = [];
  const tx_begin = query(sql`begin`);
  const tx_commit = query(sql`commit`);
  const tx_rollback = query(sql`rollback`);
  const sp_name = sql.ident`__pglue_tx`;
  const sp_savepoint = query(sql`savepoint ${sp_name}`);
  const sp_release = query(sql`release ${sp_name}`);
  const sp_rollback_to = query(sql`rollback to ${sp_name}`);

  async function begin() {
    const tx = new Transaction(
      await (tx_stack.length ? sp_savepoint.execute() : tx_begin.execute())
    );
    return tx_stack.push(tx), tx;
  }

  const Transaction = class implements Transaction {
    readonly tag!: string;

    get open(): boolean {
      return tx_stack.indexOf(this) !== -1;
    }

    constructor(begin: Result) {
      Object.assign(this, begin);
    }

    async commit() {
      const i = tx_stack.indexOf(this);
      if (i === -1) throw new WireError(`transaction is not open`);
      else tx_stack.length = i;
      return await (i ? sp_release.execute() : tx_commit.execute());
    }

    async rollback() {
      const i = tx_stack.indexOf(this);
      if (i === -1) throw new WireError(`transaction is not open`);
      else tx_stack.length = i;
      if (i !== 0) {
        const res = await sp_rollback_to.execute();
        return await sp_release.execute(), res;
      } else {
        return await tx_rollback.execute();
      }
    }

    async [Symbol.asyncDispose]() {
      if (this.open) await this.rollback();
    }
  };

  // https://www.postgresql.org/docs/current/sql-listen.html
  // https://www.postgresql.org/docs/current/sql-notify.html
  const channels = new Map<string, Channel>();

  async function listen(channel: string) {
    let ch;
    if ((ch = channels.get(channel))) return ch;
    const res = await query(sql`listen ${sql.ident(channel)}`).execute();
    if (tx_status !== "I")
      log("warn", {}, `LISTEN executed inside transaction`);
    if ((ch = channels.get(channel))) return ch;
    return channels.set(channel, (ch = new Channel(channel, res))), ch;
  }

  async function notify(channel: string, payload: string) {
    return await query(
      sql`select pg_notify(${channel}::text, ${payload}::text)`
    ).execute();
  }

  const Channel = class extends TypedEmitter<ChannelEvents> implements Channel {
    readonly #name;
    readonly tag!: string;

    get name() {
      return this.#name;
    }

    get open(): boolean {
      return channels.get(this.#name) === this;
    }

    constructor(name: string, listen: Result) {
      super();
      Object.assign(this, listen);
      this.#name = name;
    }

    notify(payload: string) {
      return notify(this.#name, payload);
    }

    async unlisten() {
      const name = this.#name;
      if (channels.get(name) === this) channels.delete(name);
      else throw new WireError(`channel is not listening`);
      return await query(sql`unlisten ${sql.ident(name)}`).execute();
    }

    async [Symbol.asyncDispose]() {
      if (this.open) await this.unlisten();
    }
  };

  return { params, connect, query, begin, listen, notify, close };
}

export type PoolOptions = v.Infer<typeof PoolOptions>;
export const PoolOptions = WireOptions.extend({
  max_connections: v
    .number()
    .optional(() => 10)
    .assert(Number.isSafeInteger, `invalid number`),
  idle_timeout: v
    .number()
    .optional(() => 30)
    .assert(Number.isSafeInteger, `invalid number`),
});

export type PoolEvents = {
  log(level: LogLevel, ctx: object, msg: string): void;
};

export interface PoolWire<V extends WireEvents = WireEvents> extends Wire<V> {
  readonly connection_id: number;
  readonly borrowed: boolean;
  release(): void;
}

export interface PoolTransaction extends Transaction {
  readonly wire: PoolWire;
}

export class Pool<V extends PoolEvents = PoolEvents>
  extends TypedEmitter<V>
  implements Postgres, PromiseLike<PoolWire>, Disposable
{
  readonly #options;
  readonly #acquire;
  readonly #begin;
  readonly #close;

  constructor(options: PoolOptions) {
    super();
    ({
      acquire: this.#acquire,
      begin: this.#begin,
      close: this.#close,
    } = pool_impl(this, (this.#options = options)));
  }

  async connect(options: Partial<WireOptions> = {}) {
    return await new Wire(
      WireOptions.parse({ ...this.#options, ...options }, { mode: "strip" })
    )
      .on("log", (l, c, s) => (this as Pool).emit("log", l, c, s))
      .connect();
  }

  get(): Promise<PoolWire>;
  get<T>(f: (wire: PoolWire) => T | PromiseLike<T>): Promise<T>;
  async get(f?: (wire: PoolWire) => unknown) {
    if (typeof f !== "undefined") {
      using wire = await this.#acquire();
      return await f(wire);
    } else {
      return this.#acquire();
    }
  }

  query<T = Row>(sql: SqlFragment): Query<T>;
  query<T = Row>(s: TemplateStringsArray, ...xs: unknown[]): Query<T>;
  query(s: TemplateStringsArray | SqlFragment, ...xs: unknown[]) {
    s = is_sql(s) ? s : sql(s, ...xs);
    const acquire = this.#acquire;
    return new Query(async function* stream(options) {
      using wire = await acquire();
      return yield* wire.query(s).stream(options);
    });
  }

  begin(): Promise<PoolTransaction>;
  begin<T>(
    f: (wire: PoolWire, tx: PoolTransaction) => T | PromiseLike<T>
  ): Promise<T>;
  async begin(f?: (wire: PoolWire, tx: PoolTransaction) => unknown) {
    if (typeof f !== "undefined") {
      await using tx = await this.#begin();
      const value = await f(tx.wire, tx);
      if (tx.open) await tx.commit();
      return value;
    } else {
      return this.#begin();
    }
  }

  then<T = PoolWire, U = never>(
    f?: ((wire: PoolWire) => T | PromiseLike<T>) | null,
    g?: ((reason?: unknown) => U | PromiseLike<U>) | null
  ) {
    return this.get().then(f, g);
  }

  close() {
    this.#close();
  }

  [Symbol.dispose]() {
    this.close();
  }
}

function pool_impl(
  pool: Pool,
  { max_connections, idle_timeout: _, ...options }: PoolOptions
) {
  const lock = semaphore(max_connections);
  const all = new Set<PoolWire>();
  const free: PoolWire[] = [];
  let ids = 0;

  const PoolWire = class extends Wire implements PoolWire {
    readonly #id = ids++;

    get connection_id() {
      return this.#id;
    }

    get borrowed(): boolean {
      return free.indexOf(this) === -1;
    }

    release() {
      if (all.has(this) && free.indexOf(this) === -1)
        free.push(this), lock.release();
    }

    override [Symbol.dispose]() {
      this.release();
    }
  };

  const PoolTransaction = class implements Transaction {
    readonly #wire;
    readonly #tx;

    get wire() {
      return this.#wire;
    }

    get tag() {
      return this.#tx.tag;
    }

    get open() {
      return this.#tx.open;
    }

    constructor(wire: PoolWire, tx: Transaction) {
      this.#wire = wire;
      this.#tx = tx;
    }

    async commit() {
      const res = await this.#tx.commit();
      return this.#wire.release(), res;
    }

    async rollback() {
      const res = await this.#tx.rollback();
      return this.#wire.release(), res;
    }

    async [Symbol.asyncDispose]() {
      if (this.open) await this.rollback();
    }
  };

  async function connect() {
    const wire = new PoolWire({ ...options, reconnect_delay: null });
    const { connection_id } = wire
      .on("log", (l, c, s) => pool.emit("log", l, { ...c, connection_id }, s))
      .on("close", () => forget(wire));
    return await wire.connect(), all.add(wire), wire;
  }

  async function acquire() {
    await lock();
    try {
      return free.pop() ?? (await connect());
    } catch (e) {
      throw (lock.release(), e);
    }
  }

  function forget(wire: PoolWire) {
    if (all.delete(wire)) {
      const i = free.indexOf(wire);
      if (i !== -1) free.splice(i, 1);
      else lock.release();
    }
  }

  async function begin() {
    const wire = await acquire();
    try {
      return new PoolTransaction(wire, await wire.begin());
    } catch (e) {
      throw (wire.release(), e);
    }
  }

  function close() {
    for (const wire of all) wire.close();
    all.clear(), (free.length = 0), lock.reset(max_connections);
  }

  return { acquire, begin, close };
}
