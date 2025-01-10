import type { ObjectType } from "./valita.ts";
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
    if (Number.isInteger(n)) {
      if (-9007199254740991 <= n && n <= 9007199254740991) return n.toString();
      else throw new SqlTypeError(`unsafe int8 output '${x}'`);
    } else if (typeof n === "bigint") {
      if (-9223372036854775808n <= n && n <= 9223372036854775807n)
        return n.toString();
    }
    throw new SqlTypeError(`invalid int8 output '${x}'`);
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
  16: bool, // bool
  25: text, // text
  21: int2, // int2
  23: int4, // int4
  20: int8, // int8
  26: int8, // oid
  700: float4, // float4
  701: float8, // float8
  1082: timestamptz, // date
  1114: timestamptz, // timestamp
  1184: timestamptz, // timestamptz
  17: bytea, // bytea
  114: json, // json
  3802: json, // jsonb
};

sql.types = sql_types;

type ReadonlyTuple<T extends readonly unknown[]> = readonly [...T];

export interface CommandResult {
  readonly tag: string;
}

export interface Result<T> extends CommandResult, ReadonlyTuple<[T]> {
  readonly row: T;
}

export interface Results<T> extends CommandResult, ReadonlyArray<T> {
  readonly rows: ReadonlyArray<T>;
}

export interface ResultStream<T>
  extends AsyncIterable<T[], CommandResult, void> {}

export interface Row extends Iterable<unknown, void, void> {
  [column: string]: unknown;
}

export interface QueryOptions {
  readonly chunk_size: number;
  readonly stdin: ReadableStream<Uint8Array> | null;
  readonly stdout: WritableStream<Uint8Array> | null;
}

export class Query<T = Row>
  implements PromiseLike<Results<T>>, ResultStream<T>
{
  readonly #f;

  constructor(f: (options: Partial<QueryOptions>) => ResultStream<T>) {
    this.#f = f;
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

  parse<S extends ObjectType>(
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

  async first(): Promise<Result<T>> {
    const { rows, tag } = await this.collect(1);
    if (!rows.length) throw new TypeError(`expected one row, got none instead`);
    const row = rows[0];
    return Object.assign([row] as const, { row: rows[0], tag });
  }

  async first_or<S>(value: S): Promise<Result<T | S>> {
    const { rows, tag } = await this.collect(1);
    const row = rows.length ? rows[0] : value;
    return Object.assign([row] as const, { row: rows[0], tag });
  }

  async collect(count = Number.POSITIVE_INFINITY): Promise<Results<T>> {
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

  then<S = Results<T>, U = never>(
    f?: ((rows: Results<T>) => S | PromiseLike<S>) | null,
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
