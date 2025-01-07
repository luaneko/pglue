import { from_hex, to_hex } from "./lstd.ts";

export const sql_format = Symbol.for(`re.lua.pglue.sql_format`);

export interface SqlFragment {
  [sql_format](f: SqlFormatter): void;
}

export function is_sql(x: unknown): x is SqlFragment {
  return typeof x === "object" && x !== null && sql_format in x;
}

export interface FromSql {
  (x: SqlValue): unknown;
}

export interface ToSql {
  (x: unknown): SqlFragment;
}

export const from_sql = function from_sql(x) {
  const { type, value } = x;
  if (value === null) return null;

  switch (type) {
    case 16: // boolean
      return boolean.parse(value);
    case 25: // text
      return text.parse(value);
    case 21: // int2
      return int2.parse(value);
    case 23: // int4
      return int4.parse(value);
    case 20: // int8
    case 26: // oid
      return int8.parse(value);
    case 700: // float4
      return float4.parse(value);
    case 701: // float8
      return float8.parse(value);
    case 1082: // date
    case 1114: // timestamp
    case 1184: // timestamptz
      return timestamptz.parse(value);
    case 17: // bytea
      return bytea.parse(value);
    case 114: // json
    case 3802: // jsonb
      return json.parse(value);
    default:
      return x;
  }
} as FromSql;

export const to_sql = function to_sql(x) {
  switch (typeof x) {
    case "undefined":
      return nil();
    case "boolean":
      return boolean(x);
    case "number":
      return float8(x);
    case "bigint":
      return int8(x);
    case "string":
    case "symbol":
    case "function":
      return text(x);
  }

  switch (true) {
    case x === null:
      return nil();

    case is_sql(x):
      return x;

    case Array.isArray(x):
      return array(...(x instanceof Array ? x : Array.from(x)));

    case x instanceof Date:
      return timestamptz(x);

    case x instanceof Uint8Array:
    case x instanceof ArrayBuffer:
    case x instanceof SharedArrayBuffer:
      return bytea(x);
  }

  throw new TypeError(`cannot convert input '${x}' to sql`);
} as ToSql;

export class SqlValue implements SqlFragment {
  constructor(
    readonly type: number,
    readonly value: string | null
  ) {}

  [sql_format](f: SqlFormatter) {
    f.write_param(this.type, this.value);
  }

  [Symbol.toStringTag]() {
    return `${this.constructor.name}<${this.type}>`;
  }

  [Symbol.toPrimitive]() {
    return this.value;
  }

  toString() {
    return String(this.value);
  }

  toJSON() {
    return this.value;
  }
}

export function value(type: number, x: unknown) {
  const s = x === null || typeof x === "undefined" ? null : String(x);
  return new SqlValue(type, s);
}

export class SqlFormatter {
  readonly #ser;
  #query = "";
  #params = {
    types: [] as number[],
    values: [] as (string | null)[],
  };

  get query() {
    return this.#query.trim();
  }

  get params() {
    return this.#params;
  }

  constructor(serializer: ToSql) {
    this.#ser = serializer;
  }

  write(s: string | SqlFragment) {
    if (is_sql(s)) s[sql_format](this);
    else this.#query += s;
  }

  write_param(type: number, s: string | null) {
    const { types, values } = this.#params;
    types.push(type), values.push(s), this.write(`$` + values.length);
  }

  format(x: unknown) {
    this.write(is_sql(x) ? x : this.#ser(x));
  }
}

export function format(sql: SqlFragment, serializer = to_sql) {
  const fmt = new SqlFormatter(serializer);
  return fmt.write(sql), fmt;
}

export function sql(
  { raw: s }: TemplateStringsArray,
  ...xs: unknown[]
): SqlFragment {
  return {
    [sql_format](f) {
      for (let i = 0, n = s.length; i < n; i++) {
        if (i !== 0) f.format(xs[i - 1]);
        f.write(s[i]);
      }
    },
  };
}

sql.value = value;
sql.format = format;
sql.raw = raw;
sql.ident = ident;
sql.fragment = fragment;
sql.map = map;
sql.array = array;
sql.row = row;
sql.null = nil;
sql.boolean = boolean;
sql.text = text;
sql.int2 = int2;
sql.int4 = int4;
sql.int8 = int8;
sql.float4 = float4;
sql.float8 = float8;
sql.timestamptz = timestamptz;
sql.bytea = bytea;
sql.json = json;

export function raw(s: TemplateStringsArray, ...xs: unknown[]): SqlFragment;
export function raw(s: string): SqlFragment;
export function raw(
  s: TemplateStringsArray | string,
  ...xs: unknown[]
): SqlFragment {
  s = typeof s === "string" ? s : String.raw(s, ...xs);
  return {
    [sql_format](f) {
      f.write(s);
    },
  };
}

export function ident(s: TemplateStringsArray, ...xs: unknown[]): SqlFragment;
export function ident(s: string): SqlFragment;
export function ident(s: TemplateStringsArray | string, ...xs: unknown[]) {
  s = typeof s === "string" ? s : String.raw(s, ...xs);
  return raw`"${s.replaceAll('"', '""')}"`;
}

export function fragment(
  sep: string | SqlFragment,
  ...xs: unknown[]
): SqlFragment {
  return {
    [sql_format](f) {
      for (let i = 0, n = xs.length; i < n; i++) {
        if (i !== 0) f.write(sep);
        f.format(xs[i]);
      }
    },
  };
}

export function map<T>(
  sep: string | SqlFragment,
  xs: Iterable<T>,
  f: (value: T, index: number) => unknown
): SqlFragment {
  return fragment(sep, ...Iterator.from(xs).map(f));
}

export function array(...xs: unknown[]): SqlFragment {
  return sql`array[${fragment(", ", ...xs)}]`;
}

export function row(...xs: unknown[]): SqlFragment {
  return sql`row(${fragment(", ", ...xs)})`;
}

boolean.oid = 16 as const;
text.oid = 25 as const;
int2.oid = 21 as const;
int4.oid = 23 as const;
int8.oid = 20 as const;
float4.oid = 700 as const;
float8.oid = 701 as const;
timestamptz.oid = 1184 as const;
bytea.oid = 17 as const;
json.oid = 114 as const;

export function nil() {
  return value(0, null);
}

Object.defineProperty(nil, "name", { configurable: true, value: "null" });

export function boolean(x: unknown) {
  return value(
    boolean.oid,
    x === null || typeof x === "undefined" ? null : x ? "t" : "f"
  );
}

boolean.parse = function parse_boolean(s: string) {
  return s === "t";
};

export function text(x: unknown) {
  return value(text.oid, x);
}

text.parse = function parse_text(s: string) {
  return s;
};

const i2_min = -32768;
const i2_max = 32767;

export function int2(x: unknown) {
  return value(int2.oid, x);
}

int2.parse = function parse_int2(s: string) {
  const n = Number(s);
  if (Number.isInteger(n) && i2_min <= n && n <= i2_max) return n;
  else throw new TypeError(`input '${s}' is not a valid int2 value`);
};

const i4_min = -2147483648;
const i4_max = 2147483647;

export function int4(x: unknown) {
  return value(int4.oid, x);
}

int4.parse = function parse_int4(s: string) {
  const n = Number(s);
  if (Number.isInteger(n) && i4_min <= n && n <= i4_max) return n;
  else throw new TypeError(`input '${s}' is not a valid int4 value`);
};

const i8_min = -9223372036854775808n;
const i8_max = 9223372036854775807n;

export function int8(x: unknown) {
  return value(int8.oid, x);
}

function to_int8(n: number | bigint) {
  if (typeof n === "bigint") return i8_min <= n && n <= i8_max ? n : null;
  else return Number.isSafeInteger(n) ? BigInt(n) : null;
}

int8.parse = function parse_int8(s: string) {
  const n = to_int8(BigInt(s));
  if (n !== null) return to_float8(n) ?? n;
  else throw new TypeError(`input '${s}' is not a valid int8 value`);
};

const f8_min = -9007199254740991n;
const f8_max = 9007199254740991n;

export function float4(x: unknown) {
  return value(float4.oid, x);
}

export function float8(x: unknown) {
  return value(float8.oid, x);
}

function to_float8(n: number | bigint) {
  if (typeof n === "bigint")
    return f8_min <= n && n <= f8_max ? Number(n) : null;
  else return Number.isNaN(n) ? null : n;
}

float4.parse = float8.parse = function parse_float8(s: string) {
  const n = to_float8(Number(s));
  if (n !== null) return n;
  else throw new TypeError(`input '${s}' is not a valid float8 value`);
};

export function timestamptz(x: unknown) {
  if (x instanceof Date) x = x.toISOString();
  else if (typeof x === "number" || typeof x === "bigint")
    x = new Date(Number(x) * 1000).toISOString(); // unix epoch
  return value(timestamptz.oid, x);
}

timestamptz.parse = function parse_timestamptz(s: string) {
  const t = Date.parse(s);
  if (!Number.isNaN(t)) return new Date(t);
  else throw new TypeError(`input '${s}' is not a valid timestamptz value`);
};

export function bytea(x: Uint8Array | ArrayBufferLike | Iterable<number>) {
  let buf;
  if (x instanceof Uint8Array) buf = x;
  else if (x instanceof ArrayBuffer || x instanceof SharedArrayBuffer)
    buf = new Uint8Array(x);
  else buf = Uint8Array.from(x);
  return value(bytea.oid, `\\x` + to_hex(buf));
}

bytea.parse = function parse_bytea(s: string) {
  if (s.startsWith(`\\x`)) return from_hex(s.slice(2));
  else throw new TypeError(`input is not a valid bytea value`);
};

export function json(x: unknown) {
  return value(json.oid, JSON.stringify(x) ?? null);
}

json.parse = function parse_json(s: string): unknown {
  return JSON.parse(s);
};
