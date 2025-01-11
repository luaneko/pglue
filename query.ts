import type * as v from "./valita.ts";
import { from_hex, to_hex, to_utf8 } from "./lstd.ts";

export const sql_format = Symbol.for(`re.lua.pglue.sql_format`);

export interface SqlFragment {
  [sql_format](f: SqlFormatter): void;
}

export interface SqlFormatter {
  query: string;
  params: unknown[];
}

export function is_sql(x: unknown): x is SqlFragment {
  return typeof x === "object" && x !== null && sql_format in x;
}

export function sql(
  { raw: s }: TemplateStringsArray,
  ...xs: unknown[]
): SqlFragment {
  return {
    [sql_format](fmt) {
      for (let i = 0, n = s.length; i < n; i++) {
        if (i !== 0) fmt_format(fmt, xs[i - 1]);
        fmt.query += s[i];
      }
    },
  };
}

export function fmt_write(fmt: SqlFormatter, s: string | SqlFragment) {
  is_sql(s) ? s[sql_format](fmt) : (fmt.query += s);
}

export function fmt_format(fmt: SqlFormatter, x: unknown) {
  is_sql(x) ? x[sql_format](fmt) : fmt_enclose(fmt, x);
}

export function fmt_enclose(fmt: SqlFormatter, x: unknown) {
  const { params } = fmt;
  params.push(x), (fmt.query += `$` + params.length);
}

sql.format = format;
sql.raw = raw;
sql.ident = ident;
sql.fragment = fragment;
sql.map = map;
sql.array = array;
sql.row = row;

export function format(sql: SqlFragment) {
  const fmt: SqlFormatter = { query: "", params: [] };
  return sql[sql_format](fmt), fmt;
}

export function raw(s: string): SqlFragment;
export function raw(s: TemplateStringsArray, ...xs: unknown[]): SqlFragment;
export function raw(
  s: TemplateStringsArray | string,
  ...xs: unknown[]
): SqlFragment {
  s = typeof s === "string" ? s : String.raw(s, ...xs);
  return {
    [sql_format](fmt) {
      fmt.query += s;
    },
  };
}

export function ident(s: string): SqlFragment;
export function ident(s: TemplateStringsArray, ...xs: unknown[]): SqlFragment;
export function ident(s: TemplateStringsArray | string, ...xs: unknown[]) {
  s = typeof s === "string" ? s : String.raw(s, ...xs);
  return raw`"${s.replaceAll('"', '""')}"`;
}

export function fragment(
  sep: string | SqlFragment,
  ...xs: unknown[]
): SqlFragment {
  return {
    [sql_format](fmt) {
      for (let i = 0, n = xs.length; i < n; i++) {
        if (i !== 0) fmt_write(fmt, sep);
        fmt_format(fmt, xs[i]);
      }
    },
  };
}

export function map<T>(
  sep: string | SqlFragment,
  xs: Iterable<T>,
  f: (value: T, index: number) => unknown
) {
  return fragment(sep, ...Iterator.from(xs).map(f));
}

export function array(...xs: unknown[]) {
  return sql`array[${fragment(", ", ...xs)}]`;
}

export function row(...xs: unknown[]) {
  return sql`row(${fragment(", ", ...xs)})`;
}

export interface SqlType {
  input(value: string): unknown;
  output(value: unknown): string | null;
}

export interface SqlTypeMap {
  readonly [oid: number]: SqlType | undefined;
}

export class SqlTypeError extends TypeError {
  override get name() {
    return this.constructor.name;
  }
}

export const bool: SqlType = {
  input(s) {
    return s !== "f";
  },
  output(x) {
    if (typeof x === "undefined" || x === null) return null;
    const b = bool_names[String(x).toLowerCase()];
    if (typeof b === "boolean") return b ? "t" : "f";
    else throw new SqlTypeError(`invalid bool output '${x}'`);
  },
};

const bool_names: Partial<Record<string, boolean>> = {
  // https://www.postgresql.org/docs/current/datatype-boolean.html#DATATYPE-BOOLEAN
  t: true,
  tr: true,
  tru: true,
  true: true,
  y: true,
  ye: true,
  yes: true,
  on: true,
  1: true,
  f: false,
  fa: false,
  fal: false,
  fals: false,
  false: false,
  n: false,
  no: false,
  of: false,
  off: false,
  0: false,
};

export const text: SqlType = {
  input(s) {
    return s;
  },
  output(x) {
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "string") return x;
    else return String(x);
  },
};

export const char: SqlType = {
  input(c) {
    const n = c.charCodeAt(0);
    if (c.length === 1 && 0 <= n && n <= 255) return c;
    throw new SqlTypeError(`invalid char input '${c}'`);
  },
  output(x) {
    let c: string;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number") c = String.fromCharCode(x);
    else c = String(x);
    const n = c.charCodeAt(0);
    if (c.length === 1 && 0 <= n && n <= 255) return c;
    else throw new SqlTypeError(`invalid char output '${x}'`);
  },
};

export const int2: SqlType = {
  input(s) {
    const n = Number(s);
    if (Number.isInteger(n) && -32768 <= n && n <= 32767) return n;
    else throw new SqlTypeError(`invalid int2 input '${s}'`);
  },
  output(x) {
    let n: number;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number") n = x;
    else n = Number(x);
    if (Number.isInteger(n) && -32768 <= n && n <= 32767) return n.toString();
    else throw new SqlTypeError(`invalid int2 output '${x}'`);
  },
};

export const int4: SqlType = {
  input(s) {
    const n = Number(s);
    if (Number.isInteger(n) && -2147483648 <= n && n <= 2147483647) return n;
    else throw new SqlTypeError(`invalid int4 input '${s}'`);
  },
  output(x) {
    let n: number;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number") n = x;
    else n = Number(x);
    if (Number.isInteger(n) && -2147483648 <= n && n <= 2147483647)
      return n.toString();
    else throw new SqlTypeError(`invalid int4 output '${x}'`);
  },
};

export const uint4: SqlType = {
  input(s) {
    const n = Number(s);
    if (Number.isInteger(n) && 0 <= n && n <= 4294967295) return n;
    else throw new SqlTypeError(`invalid uint4 input '${s}'`);
  },
  output(x) {
    let n: number;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number") n = x;
    else n = Number(x);
    if (Number.isInteger(n) && 0 <= n && n <= 4294967295) return n.toString();
    else throw new SqlTypeError(`invalid uint4 output '${x}'`);
  },
};

export const int8: SqlType = {
  input(s) {
    const n = BigInt(s);
    if (-9007199254740991n <= n && n <= 9007199254740991n) return Number(n);
    else if (-9223372036854775808n <= n && n <= 9223372036854775807n) return n;
    else throw new SqlTypeError(`invalid int8 input '${s}'`);
  },
  output(x) {
    let n: number | bigint;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number" || typeof x === "bigint") n = x;
    else if (typeof x === "string") n = BigInt(x);
    else n = Number(x);
    if (
      (typeof n === "number" && Number.isSafeInteger(n)) ||
      (typeof n === "bigint" &&
        -9223372036854775808n <= n &&
        n <= 9223372036854775807n)
    ) {
      return n.toString();
    } else throw new SqlTypeError(`invalid int8 output '${x}'`);
  },
};

export const uint8: SqlType = {
  input(s) {
    const n = BigInt(s);
    if (0n <= n && n <= 9007199254740991n) return Number(n);
    else if (0n <= n && n <= 18446744073709551615n) return n;
    else throw new SqlTypeError(`invalid uint8 input '${s}'`);
  },
  output(x) {
    let n: number | bigint;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number" || typeof x === "bigint") n = x;
    else if (typeof x === "string") n = BigInt(x);
    else n = Number(x);
    if (
      (typeof n === "number" && Number.isSafeInteger(n) && 0 <= n) ||
      (typeof n === "bigint" && 0n <= n && n <= 18446744073709551615n)
    ) {
      return n.toString();
    } else throw new SqlTypeError(`invalid uint8 output '${x}'`);
  },
};

export const float4: SqlType = {
  input(s) {
    return Math.fround(Number(s));
  },
  output(x) {
    let n: number;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number") n = x;
    else {
      n = Number(x);
      if (Number.isNaN(n))
        throw new SqlTypeError(`invalid float4 output '${x}'`);
    }
    return Math.fround(n).toString();
  },
};

export const float8: SqlType = {
  input(s) {
    return Number(s);
  },
  output(x) {
    let n: number;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "number") n = x;
    else {
      n = Number(x);
      if (Number.isNaN(n))
        throw new SqlTypeError(`invalid float8 output '${x}'`);
    }
    return n.toString();
  },
};

export const timestamptz: SqlType = {
  input(s) {
    const t = Date.parse(s);
    if (!Number.isNaN(t)) return new Date(t);
    else throw new SqlTypeError(`invalid timestamptz input '${s}'`);
  },
  output(x) {
    let t: Date;
    if (typeof x === "undefined" || x === null) return null;
    else if (x instanceof Date) t = x;
    else if (typeof x === "number" || typeof x === "bigint")
      t = new Date(Number(x) * 1000); // unix epoch seconds
    else t = new Date(String(x));
    if (Number.isFinite(t.getTime())) return t.toISOString();
    else throw new SqlTypeError(`invalid timestamptz output '${x}'`);
  },
};

export const bytea: SqlType = {
  input(s) {
    if (s.startsWith(`\\x`)) return from_hex(s.slice(2));
    else throw new SqlTypeError(`invalid bytea input '${s}'`);
  },
  output(x) {
    let buf: Uint8Array;
    if (typeof x === "undefined" || x === null) return null;
    else if (typeof x === "string") buf = to_utf8(x);
    else if (x instanceof Uint8Array) buf = x;
    else if (x instanceof ArrayBuffer || x instanceof SharedArrayBuffer)
      buf = new Uint8Array(x);
    else if (Array.isArray(x) || x instanceof Array) buf = Uint8Array.from(x);
    else throw new SqlTypeError(`invalid bytea output '${x}'`);
    return `\\x` + to_hex(buf);
  },
};

export const json: SqlType = {
  input(s) {
    return JSON.parse(s);
  },
  output(x) {
    return typeof x === "undefined" ? null : JSON.stringify(x);
  },
};

export const sql_types: SqlTypeMap = {
  0: text,
  16: bool, // bool
  17: bytea, // bytea
  18: char, // char
  19: text, // name
  20: int8, // int8
  21: int2, // int2
  23: int4, // int4
  25: text, // text
  26: uint4, // oid
  28: uint4, // xid
  29: uint4, // cid
  114: json, // json
  700: float4, // float4
  701: float8, // float8
  1082: timestamptz, // date
  1114: timestamptz, // timestamp
  1184: timestamptz, // timestamptz
  3802: json, // jsonb
  5069: uint8, // xid8
};

sql.types = sql_types;

export interface Result {
  readonly tag: string;
}

export interface Rows<T> extends Result, ReadonlyArray<T> {
  readonly rows: ReadonlyArray<T>;
}

export interface RowStream<T> extends AsyncIterable<T[], Result, void> {}

export interface Row extends Iterable<unknown, void, void> {
  [column: string]: unknown;
}

export interface QueryOptions {
  readonly simple: boolean;
  readonly chunk_size: number;
  readonly stdin: ReadableStream<Uint8Array> | null;
  readonly stdout: WritableStream<Uint8Array> | null;
}

export class Query<T = Row> implements PromiseLike<Rows<T>>, RowStream<T> {
  readonly #f;

  constructor(f: (options: Partial<QueryOptions>) => RowStream<T>) {
    this.#f = f;
  }

  simple(simple = true) {
    const f = this.#f;
    return new Query((o) => f({ simple, ...o }));
  }

  chunked(chunk_size = 1) {
    const f = this.#f;
    return new Query((o) => f({ chunk_size, ...o }));
  }

  stdin(stdin: ReadableStream<Uint8Array> | string | null) {
    if (typeof stdin === "string") stdin = str_to_stream(stdin);
    const f = this.#f;
    return new Query((o) => f({ stdin, ...o }));
  }

  stdout(stdout: WritableStream<Uint8Array> | null) {
    const f = this.#f;
    return new Query((o) => f({ stdout, ...o }));
  }

  map<S>(f: (row: T, index: number) => S) {
    // deno-lint-ignore no-this-alias
    const q = this;
    return new Query<S>(async function* map(o) {
      const iter = q.#f(o)[Symbol.asyncIterator]();
      let i, next;
      for (i = 0; !(next = await iter.next()).done; ) {
        const { value: from } = next;
        const to = [];
        for (let j = 0, n = (to.length = from.length); j < n; j++) {
          to[j] = f(from[j], i++);
        }
        yield to;
      }
      return next.value;
    });
  }

  filter<S extends T>(f: (row: T, index: number) => row is S) {
    // deno-lint-ignore no-this-alias
    const q = this;
    return new Query<S>(async function* filter(o) {
      const iter = q.#f(o)[Symbol.asyncIterator]();
      let i, next;
      for (i = 0; !(next = await iter.next()).done; ) {
        const { value: from } = next;
        const to = [];
        for (let j = 0, k = 0, n = from.length; j < n; j++) {
          const x = from[j];
          if (f(x, i++)) to[k++] = x;
        }
        yield to;
      }
      return next.value;
    });
  }

  parse<S extends v.ObjectType>(
    type: S,
    { mode = "strip" }: { mode?: "passthrough" | "strict" | "strip" } = {}
  ) {
    return this.map(function parse(row) {
      return type.parse(row, { mode });
    });
  }

  stream(options: Partial<QueryOptions> = {}) {
    return this.#f(options);
  }

  async first(): Promise<T> {
    const rows = await this.collect(1);
    if (rows.length !== 0) return rows[0];
    else throw new TypeError(`expected one row, got none instead`);
  }

  async first_or<S>(value: S): Promise<T | S> {
    const rows = await this.collect(1);
    return rows.length !== 0 ? rows[0] : value;
  }

  async collect(count = Number.POSITIVE_INFINITY): Promise<Rows<T>> {
    const iter = this[Symbol.asyncIterator]();
    let next;
    const rows = [];
    for (let i = 0; !(next = await iter.next()).done; ) {
      const chunk = next.value;
      for (let j = 0, n = chunk.length; i < count && j < n; )
        rows[i++] = chunk[j++];
    }
    return Object.assign(rows, next.value, { rows });
  }

  async execute() {
    const iter = this[Symbol.asyncIterator]();
    let next;
    while (!(next = await iter.next()).done);
    return next.value;
  }

  async count() {
    const iter = this[Symbol.asyncIterator]();
    let n = 0;
    for (let next; !(next = await iter.next()).done; ) n += next.value.length;
    return n;
  }

  then<S = Rows<T>, U = never>(
    f?: ((rows: Rows<T>) => S | PromiseLike<S>) | null,
    g?: ((reason?: unknown) => U | PromiseLike<U>) | null
  ) {
    return this.collect().then(f, g);
  }

  [Symbol.asyncIterator]() {
    return this.stream()[Symbol.asyncIterator]();
  }
}

function str_to_stream(s: string) {
  return new ReadableStream({
    type: "bytes",
    start(c) {
      if (s.length !== 0) c.enqueue(to_utf8(s));
      c.close();
    },
  });
}
