// https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.2
const rg_octal = /^\\([0-7]{1,3})/;
const rg_hex = /^\\x([0-9a-fA-F]{1,2})/;

export function copy_fmt(cols: readonly string[]) {
  let s = "";
  for (let i = 0, n = cols.length; i < n; i++) {
    if (i !== 0) s += "\t";
    s += copy_fmt_escape(cols[i]);
  }
  return s;
}

export function copy_fmt_escape(s: string) {
  return s
    .replaceAll("\\", "\\\\")
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\t", "\\t")
    .replaceAll("\0", "\\000");
}
