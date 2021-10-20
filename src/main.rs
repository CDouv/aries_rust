

use anyhow::Error;
use odbc_api::{buffers::TextRowSet,Cursor,Environment};
use std:: {
    ffi::CStr,
    io::{stdout, Write},
    path::PathBuf,
};

const BATCH_SIZE: usize = 5000;
fn main() -> Result<(), Error> {
//Write csv to standard out
let out = stdout();
let mut writer = csv::Writer::from_writer(out);

let environment = Environment::new()?;

let conn_str =r#"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ= C:\projects\aries_rust\demo-2.accdb;"# ;



//Establishing the connection
let mut connection = environment.connect_with_connection_string(conn_str)?;


//Execute a query
match connection.execute("SELECT * FROM AC_ECONOMIC", ())? {
    Some(cursor) => {
        // Write the column names to stdout
        let mut headline: Vec<String> = cursor.column_names()?.collect::<Result<_,_>>()?;
        writer.write_record(headline)?;

        //Use schema in cursor to initialize a text buffer large enough to hold the largest
        //possible strings for each column up to an upper limit of 4kb
        let mut buffers = TextRowSet::for_cursor(BATCH_SIZE,&cursor,Some(4096))?;
        // Bind the buffer to the cursor. It is now being filled with every call to fetch.
        let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

        //Iterate over batches
        while let Some(batch) = row_set_cursor.fetch()? {
            // Within a batch, iterate over every row
            for row_index in 0..batch.num_rows() {
                //Within a row iterate over every column
                let record = (0..batch.num_cols()).map(|col_index| {
                    batch
                        .at(col_index, row_index)
                        .unwrap_or(&[])
                });
                //Writes row as csv
                writer.write_record(record)?;
            }
        }
    }
    None => {
        eprintln!(
            "Query came back empty. No output has been created."
        );
    }
}
Ok(())
}