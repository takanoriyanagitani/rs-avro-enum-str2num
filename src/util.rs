#[macro_export]
macro_rules! bind {
    ($io: expr, $mapper: expr) => {
        move || {
            let t = $io()?;
            $mapper(t)()
        }
    };
}

#[macro_export]
macro_rules! lift {
    ($pure: expr) => {
        move |t| move || $pure(t)
    };
}
