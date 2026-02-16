use dojo_introspect_types::events::ModelRegistered;
use torii::etl::Envelope;
use torii_introspect::events::CreateTable;

pub trait DojoEventTrait
where
    Self: Sized,
{
    type Message;

    fn into_message(self) -> Self::Message;
}
