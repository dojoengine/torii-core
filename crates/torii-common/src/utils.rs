pub trait ElementsInto<T> {
    fn elements_into(self) -> Vec<T>;
}

pub trait ElementsFrom<T> {
    fn elements_from(vec: Vec<T>) -> Self;
}

impl<T, U> ElementsInto<U> for Vec<T>
where
    T: Into<U>,
{
    fn elements_into(self) -> Vec<U> {
        self.into_iter().map(T::into).collect()
    }
}

impl<T, U> ElementsFrom<T> for Vec<U>
where
    U: From<T>,
{
    fn elements_from(vec: Vec<T>) -> Self {
        vec.into_iter().map(U::from).collect()
    }
}
