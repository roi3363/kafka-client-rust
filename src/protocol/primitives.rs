
pub struct ValueAndLength<T> (T, isize);

#[derive(Debug, Clone, Copy)]
pub enum DataTypes {
    Int8,
    Int16,
    Int32,
    Int64,
}