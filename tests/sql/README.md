## SQL Test Format

Each `.test` file is a plain-text script with sections separated by blank lines. Supported directives:

- `require <feature>`: ignored by the runner; keep for tooling parity.
- `statement ok|error`: follow with one or more SQL lines. An empty line ends the block. `ok` expects success; `error` expects failure.
- `query <types|error>`: follow with SQL lines until a line containing only `----`; then list expected rows until a blank line. Use tab (`\t`) between columns; the runner will also accept whitespace and normalize to tabs. The `types` token is currently ignored.

Example:
```
statement ok
CREATE TABLE demo (id INT, name TEXT);

statement ok
INSERT INTO demo VALUES (1, 'Alice');

query II
SELECT id, name FROM demo
----
1	Alice
```
