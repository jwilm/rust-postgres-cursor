rust-postgres-cursor
====================

A cursor type for use with PostgreSQL.

## Example

```rust
extern crate postgres;
extern crate postgres_cursor;

use postgres::{Client, NoTls};
use postgres_cursor::Cursor;

// First, establish a connection with postgres
let mut client = Client::connect("postgres://jwilm@127.0.0.1/foo", NoTls)
    .expect("connect");

// Build the cursor
let mut cursor = Cursor::build(&mut client)
    // Batch size determines rows returned in each FETCH call
    .batch_size(10)
    // Query is the statement to build a cursor for
    .query("SELECT id FROM products")
    // Finalize turns this builder into a cursor
    .finalize()
    .expect("cursor creation succeeded");

// Iterate over batches of rows
for result in &mut cursor {
    // Each item returned from the iterator is a Result<Vec<Row>, postgres::Error>.
    // This is because each call to `next()` makes a query
    // to the database.
    let rows = result.unwrap();

    // After handling errors, rows returned in this iteration
    // can be iterated over.
    for row in &rows {
        println!("{:?}", row);
    }
}
```

## Features

The library is mostly complete since its only intent is providing a cursor
abstraction. However, there are some small planned features worth noting:

- [ ] Support query parameters (eg. `"SELECT foo FROM bar WHERE quux = $1"`) in
      cursor query string.
