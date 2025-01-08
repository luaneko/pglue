import {
  buf_concat_fast,
  buf_eq,
  buf_xor,
  channel,
  from_base64,
  from_utf8,
  semaphore,
  semaphore_fast,
  to_base64,
  to_utf8,
  TypedEmitter,
} from "./lstd.ts";
import {
  array,
  byten,
  byten_lp,
  byten_rest,
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
  sum_const_size,
} from "./ser.ts";
import {
  is_sql,
  sql,
  type FromSql,
  type SqlFragment,
  type ToSql,
} from "./sql.ts";
import {
  type CommandResult,
  Query,
  type ResultStream,
  type Row,
  row_ctor,
  type RowConstructor,
} from "./query.ts";
import { join } from "jsr:@std/path@^1.0.8";

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

function severity_level(s: string): LogLevel {
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
    const_size: sum_const_size(header_size, fields.const_size),
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
  data: byten_rest,
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
  data: byten_rest,
});

export const AuthenticationSASLFinal = msg("R", {
  status: oneof(i32, 12 as const),
  data: byten_rest,
});

export const BackendKeyData = msg("K", {
  process_id: i32,
  secret_key: i32,
});

export const Bind = msg("B", {
  portal: cstring,
  statement: cstring,
  param_formats: array(i16, i16),
  param_values: array(i16, byten_lp),
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
export const CopyData = msg("d", { data: byten_rest });
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
  column_values: array(i16, byten_lp),
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
  arg_values: array(i16, byten_lp),
  result_format: i16,
});

export const FunctionCallResponse = msg("V", {
  result_value: byten_lp,
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
  data: byten_lp,
});

export const SASLResponse = msg("p", {
  data: byten_rest,
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

export type LogLevel = "debug" | "info" | "warn" | "error" | "fatal";

export interface Parameters extends Readonly<Partial<Record<string, string>>> {}

export interface WireOptions {
  readonly host: string;
  readonly port: number;
  readonly user: string;
  readonly password: string;
  readonly database: string | null;
  readonly runtime_params: Record<string, string>;
  readonly from_sql: FromSql;
  readonly to_sql: ToSql;
}

export type WireEvents = {
  log(level: LogLevel, ctx: object, msg: string): void;
  notice(notice: PostgresError): void;
  parameter(name: string, value: string, prev: string | null): void;
  notify(channel: string, payload: string, process_id: number): void;
  close(reason?: unknown): void;
};

export interface Transaction extends CommandResult, AsyncDisposable {
  readonly open: boolean;
  commit(): Promise<CommandResult>;
  rollback(): Promise<CommandResult>;
}

export type ChannelEvents = { notify: NotificationHandler };
export type NotificationHandler = (payload: string, process_id: number) => void;
export interface Channel
  extends TypedEmitter<ChannelEvents>,
    CommandResult,
    AsyncDisposable {
  readonly name: string;
  readonly open: boolean;
  notify(payload: string): Promise<CommandResult>;
  unlisten(): Promise<CommandResult>;
}

export async function wire_connect(options: WireOptions) {
  const { host, port } = options;
  const wire = new Wire(await socket_connect(host, port), options);
  return await wire.connected, wire;
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

export class Wire extends TypedEmitter<WireEvents> implements Disposable {
  readonly #socket;
  readonly #params;
  readonly #auth;
  readonly #connected;
  readonly #query;
  readonly #begin;
  readonly #listen;
  readonly #notify;
  readonly #close;

  get socket() {
    return this.#socket;
  }

  get params() {
    return this.#params;
  }

  get connected() {
    return this.#connected;
  }

  constructor(socket: Deno.Conn, options: WireOptions) {
    super();
    ({
      params: this.#params,
      auth: this.#auth,
      query: this.#query,
      begin: this.#begin,
      listen: this.#listen,
      notify: this.#notify,
      close: this.#close,
    } = wire_impl(this, socket, options));
    this.#socket = socket;
    (this.#connected = this.#auth()).catch(close);
  }

  query(sql: SqlFragment): Query;
  query(s: TemplateStringsArray, ...xs: unknown[]): Query;
  query(s: TemplateStringsArray | SqlFragment, ...xs: unknown[]) {
    return this.#query(is_sql(s) ? s : sql(s, ...xs));
  }

  begin(): Promise<Transaction>;
  begin<T>(f: (wire: this, tx: Transaction) => T | PromiseLike<T>): Promise<T>;
  async begin(f?: (wire: this, tx: Transaction) => unknown) {
    if (typeof f !== "undefined") {
      await using tx = await this.#begin();
      const value = await f(this, tx);
      return await tx.commit(), value;
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

  async get(param: string, missing_null = true) {
    return (
      await this.query`select current_setting(${param}, ${missing_null})`
        .map(([s]) => String(s))
        .first_or(null)
    )[0];
  }

  async set(param: string, value: string, local = false) {
    return await this
      .query`select set_config(${param}, ${value}, ${local})`.execute();
  }

  close(reason?: unknown) {
    this.#close(reason);
  }

  [Symbol.dispose]() {
    this.close();
  }
}

function wire_impl(
  wire: Wire,
  socket: Deno.Conn,
  { user, database, password, runtime_params, from_sql, to_sql }: WireOptions
) {
  const params: Parameters = Object.create(null);

  function log(level: LogLevel, ctx: object, msg: string) {
    wire.emit("log", level, ctx, msg);
  }

  async function read<T>(type: Encoder<T>) {
    const msg = await read_recv();
    if (msg === null) throw new WireError(`connection closed`);
    else return ser_decode(type, msg_check_err(msg));
  }

  async function read_raw() {
    const msg = await read_recv();
    if (msg === null) throw new WireError(`connection closed`);
    else return msg;
  }

  async function* read_socket() {
    const buf = new Uint8Array(64 * 1024);
    for (let n; (n = await socket.read(buf)) !== null; )
      yield buf.subarray(0, n);
  }

  const read_recv = channel.receiver<Uint8Array>(async function read(send) {
    try {
      let buf = new Uint8Array();
      for await (const chunk of read_socket()) {
        buf = buf_concat_fast(buf, chunk);

        for (let n; (n = ser_decode(Header, buf).length + 1) <= buf.length; ) {
          const msg = buf.subarray(0, n);
          buf = buf.subarray(n);

          switch (msg_type(msg)) {
            // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC
            case NoticeResponse.type: {
              const { fields } = ser_decode(NoticeResponse, msg);
              const notice = new PostgresError(fields);
              log(severity_level(notice.severity), notice, notice.message);
              wire.emit("notice", notice);
              continue;
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
              continue;
            }

            case NotificationResponse.type: {
              const { channel, payload, process_id } = ser_decode(
                NotificationResponse,
                msg
              );
              wire.emit("notify", channel, payload, process_id);
              channels.get(channel)?.emit("notify", payload, process_id);
              continue;
            }
          }

          send(msg);
        }
      }

      if (buf.length !== 0) throw new WireError(`unexpected end of stream`);
      wire.emit("close");
    } catch (e) {
      wire.emit("close", e);
    }
  });

  function write<T>(type: Encoder<T>, value: T) {
    return write_raw(ser_encode(type, value));
  }

  async function write_raw(buf: Uint8Array) {
    for (let i = 0, n = buf.length; i < n; )
      i += await socket.write(buf.subarray(i));
  }

  function close(reason?: unknown) {
    socket.close(), read_recv.close(reason);
  }

  // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-PIPELINING
  const rlock = semaphore_fast();
  const wlock = semaphore_fast();

  function pipeline<T>(
    w: () => void | PromiseLike<void>,
    r: () => T | PromiseLike<T>
  ) {
    return new Promise<T>((res, rej) => {
      pipeline_write(w).catch(rej);
      pipeline_read(r).then(res, rej);
    });
  }

  function pipeline_read<T>(r: () => T | PromiseLike<T>) {
    return rlock(async () => {
      try {
        return await r();
      } finally {
        let msg;
        while (msg_type((msg = await read_raw())) !== ReadyForQuery.type);
        ({ tx_status } = ser_decode(ReadyForQuery, msg));
      }
    });
  }

  function pipeline_write<T>(w: () => T | PromiseLike<T>) {
    return wlock(async () => {
      try {
        return await w();
      } finally {
        await write(Sync, {});
      }
    });
  }

  // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-START-UP
  async function auth() {
    await write(StartupMessage, {
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
      const msg = msg_check_err(await read_raw());
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
          await write(PasswordMessage, { password });
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
          await auth_sasl();
          continue;

        default:
          throw new WireError(`invalid authentication status ${status}`);
      }
    }

    ready: for (;;) {
      const msg = msg_check_err(await read_raw());
      switch (msg_type(msg)) {
        case BackendKeyData.type:
          continue; // ignored

        default:
          ser_decode(ReadyForQuery, msg);
          break ready;
      }
    }
  }

  // https://www.postgresql.org/docs/current/sasl-authentication.html#SASL-SCRAM-SHA-256
  // https://datatracker.ietf.org/doc/html/rfc5802
  async function auth_sasl() {
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
    const initial_nonce = `r=${to_base64(
      crypto.getRandomValues(new Uint8Array(18))
    )}`;
    const client_first_message_bare = `${username},${initial_nonce}`;
    const client_first_message = `${gs2_header}${client_first_message_bare}`;
    await write(SASLInitialResponse, { mechanism, data: client_first_message });

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
    await write(SASLResponse, { data: client_final_message });

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

  function st_get(query: string, param_types: number[]) {
    const key = JSON.stringify({ q: query, p: param_types });
    let st = st_cache.get(key);
    if (!st) st_cache.set(key, (st = new Statement(query, param_types)));
    return st;
  }

  class Statement {
    readonly name = `__st${st_ids++}`;

    constructor(
      readonly query: string,
      readonly param_types: number[]
    ) {}

    parse_task: Promise<RowConstructor> | null = null;
    parse() {
      return (this.parse_task ??= this.#parse());
    }

    async #parse() {
      try {
        const { name, query, param_types } = this;
        return row_ctor(
          from_sql,
          await pipeline(
            async () => {
              await write(Parse, { statement: name, query, param_types });
              await write(Describe, { which: "S", name });
            },
            async () => {
              await read(ParseComplete);
              await read(ParameterDescription);

              const msg = msg_check_err(await read_raw());
              if (msg_type(msg) === NoData.type) return [];
              else return ser_decode(RowDescription, msg).columns;
            }
          )
        );
      } catch (e) {
        throw ((this.parse_task = null), e);
      }
    }

    portals = 0;
    portal() {
      return `${this.name}_${this.portals++}`;
    }
  }

  async function read_rows(
    Row: RowConstructor,
    stdout: WritableStream<Uint8Array> | null
  ) {
    for (let rows = [], i = 0; ; ) {
      const msg = msg_check_err(await read_raw());
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

        case CopyInResponse.type:
          continue;

        case CopyOutResponse.type:
          await read_copy_out(stdout);
          continue;
      }
    }
  }

  async function read_copy_out(stream: WritableStream<Uint8Array> | null) {
    if (stream !== null) {
      const writer = stream.getWriter();
      try {
        for (let msg; msg_type((msg = await read_raw())) !== CopyDone.type; ) {
          const { data } = ser_decode(CopyData, msg_check_err(msg));
          await writer.write(to_utf8(data));
        }
      } finally {
        writer.releaseLock();
      }
    } else {
      while (msg_type(msg_check_err(await read_raw())) !== CopyDone.type);
    }
  }

  async function write_copy_in(stream: ReadableStream<Uint8Array> | null) {
    if (stream !== null) {
      const reader = stream.getReader();
      let err;
      try {
        try {
          for (let next; !(next = await reader.read()).done; )
            await write(CopyData, { data: next.value });
        } catch (e) {
          err = e;
        } finally {
          if (typeof err === "undefined") await write(CopyDone, {});
          else await write(CopyFail, { cause: String(err) });
        }
      } finally {
        reader.releaseLock();
      }
    } else {
      await write(CopyDone, {});
    }
  }

  async function* execute_fast(
    st: Statement,
    params: { types: number[]; values: (string | null)[] },
    stdin: ReadableStream<Uint8Array> | null,
    stdout: WritableStream<Uint8Array> | null
  ): ResultStream<Row> {
    log(
      "debug",
      { query: st.query, statement: st.name, params },
      `executing query`
    );

    const Row = await st.parse();
    const portal = st.portal();

    try {
      const { rows, tag } = await pipeline(
        async () => {
          await write(Bind, {
            portal,
            statement: st.name,
            param_formats: [],
            param_values: params.values,
            column_formats: [],
          });
          await write(Execute, { portal, row_limit: 0 });
          await write_copy_in(stdin);
          await write(Close, { which: "P" as const, name: portal });
        },
        async () => {
          await read(BindComplete);
          return read_rows(Row, stdout);
        }
      );

      if (rows.length) yield rows;
      return { tag };
    } catch (e) {
      await pipeline(
        () => write(Close, { which: "P" as const, name: portal }),
        () => read(CloseComplete)
      );

      throw e;
    }
  }

  async function* execute_chunked(
    st: Statement,
    params: { types: number[]; values: (string | null)[] },
    chunk_size: number,
    stdin: ReadableStream<Uint8Array> | null,
    stdout: WritableStream<Uint8Array> | null
  ): ResultStream<Row> {
    log(
      "debug",
      { query: st.query, statement: st.name, params },
      `executing chunked query`
    );

    const Row = await st.parse();
    const portal = st.portal();

    try {
      let { done, rows, tag } = await pipeline(
        async () => {
          await write(Bind, {
            portal,
            statement: st.name,
            param_formats: [],
            param_values: params.values,
            column_formats: [],
          });
          await write(Execute, { portal, row_limit: chunk_size });
          await write_copy_in(stdin);
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
        () => write(Close, { which: "P" as const, name: portal }),
        () => read(CloseComplete)
      );
    }
  }

  function query(s: SqlFragment) {
    const { query, params } = sql.format(s, to_sql);
    const st = st_get(query, params.types);

    return new Query(({ chunk_size = 0, stdin = null, stdout = null }) =>
      chunk_size !== 0
        ? execute_chunked(st, params, chunk_size, stdin, stdout)
        : execute_fast(st, params, stdin, stdout)
    );
  }

  // https://www.postgresql.org/docs/current/sql-begin.html
  // https://www.postgresql.org/docs/current/sql-savepoint.html
  let tx_status: "I" | "T" | "E" = "I";
  const tx_stack: Transaction[] = [];
  const tx_begin = query(sql`begin`);
  const tx_commit = query(sql`commit`);
  const tx_rollback = query(sql`rollback`);
  const sp_savepoint = query(sql`savepoint __tx`);
  const sp_release = query(sql`release __tx`);
  const sp_rollback_to = query(sql`rollback to __tx`);

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

    constructor(begin: CommandResult) {
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
    return await query(sql`select pg_notify(${channel}, ${payload})`).execute();
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

    constructor(name: string, listen: CommandResult) {
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

  return { params, auth, query, begin, listen, notify, close };
}

export interface PoolOptions extends WireOptions {
  max_connections: number;
  idle_timeout: number;
}

export type PoolEvents = {
  log(level: LogLevel, ctx: object, msg: string): void;
};

export interface PoolWire extends Wire {
  readonly connection_id: number;
  readonly borrowed: boolean;
  release(): void;
}

export interface PoolTransaction extends Transaction {
  readonly wire: PoolWire;
}

export class Pool
  extends TypedEmitter<PoolEvents>
  implements PromiseLike<PoolWire>, Disposable
{
  readonly #acquire;
  readonly #begin;
  readonly #close;

  constructor(options: PoolOptions) {
    super();
    ({
      acquire: this.#acquire,
      begin: this.#begin,
      close: this.#close,
    } = pool_impl(this, options));
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

  query(sql: SqlFragment): Query;
  query(s: TemplateStringsArray, ...xs: unknown[]): Query;
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
      return await tx.commit(), value;
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
    const { host, port } = options;
    const wire = new PoolWire(await socket_connect(host, port), options);
    await wire.connected, all.add(wire);
    const { connection_id } = wire;
    return wire
      .on("log", (l, c, s) => pool.emit("log", l, { ...c, connection_id }, s))
      .on("close", () => forget(wire));
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
