#[derive(Debug, Fail)]
pub enum DashPipeError{
    #[fail(display = "InitializeError: {}", _0)]
    InitializeError(String),
}
