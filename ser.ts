import { encode_utf8, from_utf8, jit } from "./lstd.ts";

export class EncoderError extends Error {
  override get name() {
    return this.constructor.name;
  }
}

export interface Cursor {
  i: number;
}

export function ser_encode<T>(type: Encoder<T>, x: T) {
  const buf = new Uint8Array(type.const_size ?? type.allocs(x));
  const cur: Cursor = { i: 0 };
  return type.encode(buf, cur, x), buf.subarray(0, cur.i);
}

export function ser_decode<T>(type: Encoder<T>, buf: Uint8Array) {
  return type.decode(buf, { i: 0 });
}

export interface Encoder<T> {
  readonly const_size: number | null;
  allocs(value: T): number;
  encode(buf: Uint8Array, cur: Cursor, value: T): void;
  decode(buf: Uint8Array, cur: Cursor): T;
}

export type EncoderType<E extends Encoder<unknown>> =
  E extends Encoder<infer T> ? T : never;

export function sum_const_size(...ns: (number | null)[]) {
  let sum = 0;
  for (const n of ns) {
    if (n !== null) sum += n;
    else return null;
  }
  return sum;
}

// https://www.postgresql.org/docs/current/protocol-message-types.html#PROTOCOL-MESSAGE-TYPES
export const u8: Encoder<number> = {
  const_size: 1,
  allocs() {
    return 1;
  },
  encode(buf, cur, n) {
    buf[cur.i++] = n & 0xff;
  },
  decode(buf, cur) {
    return buf[cur.i++];
  },
};

export const u16: Encoder<number> = {
  const_size: 2,
  allocs() {
    return 2;
  },
  encode(buf, cur, n) {
    let { i } = cur;
    buf[i++] = (n >>> 8) & 0xff;
    buf[i++] = n & 0xff;
    cur.i = i;
  },
  decode(buf, cur) {
    let { i } = cur;
    const n = (buf[i++] << 8) + buf[i++];
    return (cur.i = i), n;
  },
};

export const i32: Encoder<number> = {
  const_size: 4,
  allocs() {
    return 4;
  },
  encode(buf, cur, n) {
    let { i } = cur;
    buf[i++] = (n >>> 24) & 0xff;
    buf[i++] = (n >>> 16) & 0xff;
    buf[i++] = (n >>> 8) & 0xff;
    buf[i++] = n & 0xff;
    cur.i = i;
  },
  decode(buf, cur) {
    let { i } = cur;
    const n = (buf[i++] << 24) + (buf[i++] << 16) + (buf[i++] << 8) + buf[i++];
    return (cur.i = i), n;
  },
};

export function char(type: Encoder<number>) {
  return map(type, {
    from(n: number) {
      return n === 0 ? "" : String.fromCharCode(n);
    },
    to(s: string) {
      return s === "" ? 0 : s.charCodeAt(0);
    },
  });
}

export function byten(n: number): Encoder<Uint8Array> {
  return {
    const_size: n,
    allocs() {
      return n;
    },
    encode(buf, cur, s) {
      if (s.length === n) buf.set(s, cur.i), (cur.i += n);
      else throw new EncoderError(`buffer size must be ${n}`);
    },
    decode(buf, cur) {
      return buf.subarray(cur.i, (cur.i += n));
    },
  };
}

export const byten_lp: Encoder<Uint8Array | string | null> = {
  const_size: null,
  allocs(s) {
    let size = 4;
    if (typeof s === "string") size += s.length * 3;
    else if (s !== null) size += s.length;
    return size;
  },
  encode(buf, cur, s) {
    if (s === null) {
      i32.encode(buf, cur, -1);
    } else {
      const n = encode_utf8(s, buf.subarray(cur.i + 4));
      i32.encode(buf, cur, n), (cur.i += n);
    }
  },
  decode(buf, cur) {
    const n = i32.decode(buf, cur);
    return n === -1 ? null : buf.subarray(cur.i, (cur.i += n));
  },
};

export const byten_rest: Encoder<Uint8Array | string> = {
  const_size: null,
  allocs(s) {
    if (typeof s === "string") return s.length * 3;
    else return s.length;
  },
  encode(buf, cur, s) {
    cur.i += encode_utf8(s, buf.subarray(cur.i));
  },
  decode(buf, cur) {
    return buf.subarray(cur.i, (cur.i = buf.length));
  },
};

export const cstring: Encoder<string> = {
  const_size: null,
  allocs(s) {
    return s.length * 3 + 1;
  },
  encode(buf, cur, s) {
    if (s.indexOf("\0") !== -1)
      throw new EncoderError(`cstring must not contain a null byte`);
    cur.i += encode_utf8(s, buf.subarray(cur.i)) + 1;
  },
  decode(buf, cur) {
    const end = buf.indexOf(0, cur.i);
    if (end === -1) throw new EncoderError(`unexpected end of cstring`);
    return from_utf8(buf.subarray(cur.i, (cur.i = end + 1) - 1));
  },
};

export function map<T, U>(
  type: Encoder<T>,
  { from, to }: { from: (value: T) => U; to: (value: U) => T }
): Encoder<U> {
  return {
    const_size: type.const_size,
    allocs(x) {
      return type.allocs(to(x));
    },
    encode(buf, cur, x) {
      type.encode(buf, cur, to(x));
    },
    decode(buf, cur) {
      return from(type.decode(buf, cur));
    },
  };
}

export function oneof<T, C extends readonly T[]>(
  type: Encoder<T>,
  ...xs: C
): Encoder<C[number]> {
  const set = new Set(xs);
  const exp = xs.map((c) => `'${c}'`).join(", ");
  return map(type, {
    from(x) {
      if (set.has(x)) return x;
      else throw new EncoderError(`expected ${exp}, got '${x}' instead`);
    },
    to(x) {
      if (set.has(x)) return x;
      else throw new EncoderError(`expected ${exp}, got '${x}' instead`);
    },
  });
}

export interface ArrayEncoder<T> extends Encoder<Array<T>> {}

export function array<T>(
  len_type: Encoder<number>,
  type: Encoder<T>
): ArrayEncoder<T> {
  const { const_size } = type;
  return {
    const_size,
    allocs:
      const_size !== null
        ? function allocs(xs: T[]) {
            const n = xs.length;
            return len_type.allocs(n) + n * const_size;
          }
        : function allocs(xs: T[]) {
            const n = xs.length;
            let size = len_type.allocs(n);
            for (let i = 0; i < n; i++) size += type.allocs(xs[i]);
            return size;
          },
    encode(buf, cur, xs) {
      const n = xs.length;
      len_type.encode(buf, cur, n);
      for (let i = 0; i < n; i++) type.encode(buf, cur, xs[i]);
    },
    decode(buf, cur) {
      const xs = [];
      for (let i = 0, n = (xs.length = len_type.decode(buf, cur)); i < n; i++)
        xs[i] = type.decode(buf, cur);
      return xs;
    },
  };
}

export type ObjectShape = Record<string | symbol | number, Encoder<unknown>>;
export interface ObjectEncoder<S extends ObjectShape>
  extends Encoder<{ [K in keyof S]: EncoderType<S[K]> }> {}

export function object<S extends ObjectShape>(shape: S): ObjectEncoder<S> {
  const keys = Object.keys(shape);
  return jit.compiled`{
    const_size: ${jit.literal(sum_const_size(...keys.map((k) => shape[k].const_size)))},
    allocs(x) {
      return 0${jit.map("", keys, (k) => {
        return jit` + ${shape[k]}.allocs(x[${jit.literal(k)}])`;
      })};
    },
    encode(buf, cur, x) {
      ${jit.map(" ", keys, (k) => {
        return jit`${shape[k]}.encode(buf, cur, x[${jit.literal(k)}]);`;
      })}
    },
    decode(buf, cur) {
      return {
        ${jit.map(", ", keys, (k) => {
          return jit`[${jit.literal(k)}]: ${shape[k]}.decode(buf, cur)`;
        })}
      };
    },
  }`;
}
