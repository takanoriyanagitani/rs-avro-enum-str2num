use std::collections::BTreeMap;
use std::env;
use std::io;
use std::io::Read;

use apache_avro::schema::EnumSchema;
use apache_avro::schema::RecordField;
use apache_avro::schema::RecordSchema;
use apache_avro::schema::Schema;

use crate::bind;
use crate::lift;

pub fn str2num(s: &str, m: &BTreeMap<String, usize>) -> Result<usize, io::Error> {
    let ou: Option<usize> = m.get(s).copied();
    ou.ok_or_else(|| io::Error::other("invalid enum string"))
}

pub fn symbols2map(s: Vec<String>) -> BTreeMap<String, usize> {
    let mapd = s.into_iter().enumerate().map(|pair| {
        let (ix, name) = pair;
        (name, ix)
    });
    BTreeMap::from_iter(mapd)
}

pub fn enum2map(e: EnumSchema) -> BTreeMap<String, usize> {
    symbols2map(e.symbols)
}

pub fn fields2map(
    ename: &str,
    fields: Vec<RecordField>,
) -> Result<BTreeMap<String, usize>, io::Error> {
    let mut filtered = fields.into_iter().filter(|r| {
        let name: &str = &r.name;
        ename == name
    });
    let orf: Option<RecordField> = filtered.next();
    let rf: RecordField = orf.ok_or_else(|| io::Error::other("invalid schema"))?;
    let s: Schema = rf.schema;
    let es: EnumSchema = match s {
        Schema::Enum(e) => Ok(e),
        _ => Err(io::Error::other("invalid schema")),
    }?;
    Ok(enum2map(es))
}

pub fn record2map(ename: &str, r: RecordSchema) -> Result<BTreeMap<String, usize>, io::Error> {
    let fields: Vec<RecordField> = r.fields;
    fields2map(ename, fields)
}

pub fn schema2map(ename: Option<String>, s: Schema) -> Result<BTreeMap<String, usize>, io::Error> {
    match s {
        Schema::Enum(e) => Ok(enum2map(e)),
        Schema::Record(r) => match ename {
            None => Err(io::Error::other("column name missing")),
            Some(s) => record2map(&s, r),
        },
        _ => Err(io::Error::other("invalid schema")),
    }
}

pub fn enum_name2schema2map(
    ename: Option<String>,
) -> impl FnOnce(Schema) -> Result<BTreeMap<String, usize>, io::Error> {
    move |s: Schema| schema2map(ename, s)
}

/// `(env var name) -> IO(env var value)`
pub fn getenv(key: &'static str) -> impl FnMut() -> Result<String, io::Error> {
    move || env::var(key).map_err(io::Error::other)
}

/// `IO(enum string to be converted to enum number)`
pub fn enum_string() -> Result<String, io::Error> {
    getenv("ENV_ENUM_STRING")()
}

/// `(optional) enum column name`
pub fn enum_column_name() -> Option<String> {
    getenv("ENV_ENUM_COLUMN")().ok()
}

pub const SCHEMA_SIZE_MAX_DEFAULT: u64 = 1048576;

pub fn reader2string_limited<R>(reader: R, limit: u64) -> Result<String, io::Error>
where
    R: Read,
{
    let mut taken = reader.take(limit);
    let mut buf: String = String::new();
    taken.read_to_string(&mut buf)?;
    Ok(buf)
}

pub fn stdin2string_limited(limit: u64) -> Result<String, io::Error> {
    let i = io::stdin();
    let il = i.lock();
    reader2string_limited(il, limit)
}

pub fn schema_string2schema(s: String) -> Result<Schema, io::Error> {
    Schema::parse_str(s.as_str()).map_err(io::Error::other)
}

/// `(schema size max) -> IO(schema content)`
pub fn stdin2schema_string_limited(limit: u64) -> impl FnMut() -> Result<String, io::Error> {
    move || stdin2string_limited(limit)
}

/// `(schema size max) -> IO(schema)`
pub fn stdin2schema_limited(limit: u64) -> impl FnMut() -> Result<Schema, io::Error> {
    bind!(
        stdin2schema_string_limited(limit),
        lift!(schema_string2schema)
    )
}

/// `IO(schema)`
pub fn stdin2schema_limited_default() -> Result<Schema, io::Error> {
    stdin2schema_limited(SCHEMA_SIZE_MAX_DEFAULT)()
}

/// `IO(BTreeMap<String, usize>)`
pub fn stdin2schema2map_default() -> Result<BTreeMap<String, usize>, io::Error> {
    bind!(
        stdin2schema_limited_default,
        lift!(enum_name2schema2map(enum_column_name()))
    )()
}

/// `IO(converted enum number)`
pub fn converted_number() -> Result<usize, io::Error> {
    bind!(stdin2schema2map_default, |b: BTreeMap<String, usize>| {
        bind!(
            enum_string,
            lift!(|es: String| { str2num(es.as_str(), &b) })
        )
    })()
}

/// `(converted number) -> IO()`
pub fn number2stdout(num: usize) -> impl FnMut() -> Result<(), io::Error> {
    move || {
        println!("{num}");
        Ok(())
    }
}

/// `IO()`
pub fn converted2stdout() -> Result<(), io::Error> {
    bind!(converted_number, number2stdout)()
}
