use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, marker::PhantomData};
use tonic::{
    codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
    Status,
};

/// A gRPC codec that uses bincode for serialization
#[derive(Debug, Clone)]
pub struct BincodeCodec<T, U> {
    _pd: PhantomData<(T, U)>,
}

impl<T, U> Default for BincodeCodec<T, U> {
    fn default() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T, U> BincodeCodec<T, U> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T, U> Codec for BincodeCodec<T, U>
where
    T: Serialize + Send + 'static,
    U: for<'de> Deserialize<'de> + Send + 'static,
{
    type Encode = T;
    type Decode = U;

    type Encoder = BincodeEncoder<T>;
    type Decoder = BincodeDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        BincodeEncoder::new()
    }

    fn decoder(&mut self) -> Self::Decoder {
        BincodeDecoder::new()
    }
}

/// Bincode encoder
#[derive(Debug, Clone, Default)]
pub struct BincodeEncoder<T> {
    _pd: PhantomData<T>,
}

impl<T> BincodeEncoder<T> {
    pub fn new() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T> Encoder for BincodeEncoder<T>
where
    T: Serialize,
{
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let encoded =
            bincode::serialize(&item).map_err(|e| Status::internal(format!("Failed to encode with bincode: {}", e)))?;

        dst.put_slice(&encoded);
        Ok(())
    }
}

/// Bincode decoder
#[derive(Debug, Clone, Default)]
pub struct BincodeDecoder<U> {
    _pd: PhantomData<U>,
}

impl<U> BincodeDecoder<U> {
    pub fn new() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<U> Decoder for BincodeDecoder<U>
where
    U: for<'de> Deserialize<'de>,
{
    type Item = U;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        if !src.has_remaining() {
            return Ok(None);
        }

        let len = src.remaining();
        let bytes = src.copy_to_bytes(len);

        let decoded = bincode::deserialize(&bytes)
            .map_err(|e| Status::internal(format!("Failed to decode with bincode: {}", e)))?;

        Ok(Some(decoded))
    }
}
