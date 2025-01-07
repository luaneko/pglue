import type { ObjectType } from "./valita.ts";
import { from_utf8, jit, to_utf8 } from "./lstd.ts";
import { type FromSql, SqlValue } from "./sql.ts";

export interface Row extends Iterable<unknown, void, void> {
  [column: string]: unknown;
}

export interface RowConstructor {
  new (columns: (Uint8Array | string | null)[]): Row;
}

export interface RowDescription extends ReadonlyArray<ColumnDescription> {}

export interface ColumnDescription {
  readonly name: string;
  readonly table_oid: number;
  readonly table_column: number;
  readonly type_oid: number;
  readonly type_size: number;
  readonly type_modifier: number;
}

export function row_ctor(from_sql: FromSql, columns: RowDescription) {
  function parse(s: Uint8Array | string | null | undefined) {
    if (!s && s !== "") return null;
    else return from_utf8(s);
  }

  const Row = jit.compiled<RowConstructor>`function Row(xs) {
    ${jit.map(" ", columns, ({ name, type_oid }, i) => {
      return jit`this[${jit.literal(name)}] = ${from_sql}(
        new ${SqlValue}(${jit.literal(type_oid)}, ${parse}(xs[${jit.literal(i)}]))
      );`;
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
          return jit`yield this[${jit.literal(name)}];`;
        })}
      }`,
    },
  });

  return Row;
}

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
    if (!rows.length) throw new Error(`expected one row, got none instead`);
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
      const { value: c } = next;
      for (let j = 0, n = c.length; i < count && j < n; ) rows[i++] = c[j++];
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
      c.enqueue(to_utf8(s)), c.close();
    },
  });
}
