// Copyright 2015 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under (1) the MaidSafe.net Commercial License,
// version 1.0 or later, or (2) The General Public License (GPL), version 3, depending on which
// licence you accepted on initial access to the Software (the "Licences").
//
// By contributing code to the SAFE Network Software, or to this project generally, you agree to be
// bound by the terms of the MaidSafe Contributor Agreement, version 1.0.  This, along with the
// Licenses can be found in the root directory of this project at LICENSE, COPYING and CONTRIBUTOR.
//
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//
// Please review the Licences for the specific language governing permissions and limitations
// relating to use of the SAFE Network Software.

/// Errors that can occur during `ChunkStore::put`.
#[derive(Debug)]
pub enum ChunkStorePutError {
    /// There was insufficient space to save the chunk.
    StorageLimitHit,
    /// There was an IO error occured during the put.
    InternalError(ChunkStoreInternalError),
}

impl ::std::fmt::Display for ChunkStorePutError {
    fn fmt(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            ChunkStorePutError::StorageLimitHit
                => write!(formatter, "The chunk store storage limit was hit"),
            ChunkStorePutError::InternalError(ref e)
                => write!(formatter, "chunk store internal error: {}", e),
        }
    }
}

impl ::std::error::Error for ChunkStorePutError {
    fn description(&self) -> &str {
        match *self {
            ChunkStorePutError::StorageLimitHit => "The chunk store storage limit was hit",
            ChunkStorePutError::InternalError(_) => "chunk store internal error",
        }
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        match *self {
            ChunkStorePutError::StorageLimitHit => None,
            ChunkStorePutError::InternalError(ref e) => Some(e),
        }
    }
}

/// Errors that can occur when interacting with physical storage medium.
#[derive(Debug)]
pub enum ChunkStoreInternalError {
    /// Report Input/Output error.
    Io(::std::io::Error),
}

impl From<::std::io::Error> for ChunkStoreInternalError {
    fn from(error: ::std::io::Error) -> ChunkStoreInternalError {
        ChunkStoreInternalError::Io(error)
    }
}

impl ::std::fmt::Display for ChunkStoreInternalError {
    fn fmt(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match self {
            &ChunkStoreInternalError::Io(ref error) => write!(formatter, "ChunkStoreInternalError::Io: {}", error),
        }
    }
}

impl ::std::error::Error for ChunkStoreInternalError {
    fn description(&self) -> &str {
        match *self {
            ChunkStoreInternalError::Io(_) => "IO error",
        }
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        match *self {
            ChunkStoreInternalError::Io(ref error) => Some(error),
        }
    }
}

