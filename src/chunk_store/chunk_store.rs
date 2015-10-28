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

use ::error::{ChunkStoreInternalError, ChunkStorePutError};

/// ChunkStore is a collection for holding all data chunks.
/// Implements a maximum disk usage to restrict storage.
pub struct ChunkStore {
    tempdir: ::tempdir::TempDir,
    max_disk_usage: usize,
    current_disk_usage: usize,
}

impl ChunkStore {
    /// Create new chunkstore with `max_disk_usage` allowed disk usage.
    pub fn new(max_disk_usage: usize) -> Result<ChunkStore, ChunkStoreInternalError> {
        let tempdir = try!(::tempdir::TempDir::new("safe_vault"));
        Ok(ChunkStore {
            tempdir: tempdir,
            max_disk_usage: max_disk_usage,
            current_disk_usage: 0,
        })
    }

    pub fn put(&mut self, name: &::routing::NameType, value: Vec<u8>) -> Result<(), ChunkStorePutError> {
        use ::std::io::Write;

        if !self.has_disk_space(value.len()) {
            warn!("Not enough space in ChunkStore.");
            return Err(ChunkStorePutError::StorageLimitHit);
        }

        let hex_name = name.as_hex();
        let path_name = ::std::path::Path::new(&hex_name);
        let path = self.tempdir.path().join(path_name);

        // If a file with name 'name' already exists, delete it.
        if let Err(e) = self.delete(name) {
            error!("ChunkStore failed to delete possibly preexisting file {:?}: {}", path, e);
            return Err(ChunkStorePutError::InternalError(e));
        }

        let mut file = match ::std::fs::File::create(&path) {
            Ok(f)   => f,
            Err(e)  => {
                error!("ChunkStore failed to create chunk file {:?}: {}", path, e);
                return Err(ChunkStorePutError::InternalError(From::from(e)));
            }
        };
        let size = match file.write(&value[..]).and_then(|s| file.sync_all().map(|()| s)) {
            Ok(s)   => s,
            Err(e)  => {
                error!("ChunkStore failed to write chunk file {:?}: {}", path, e);
                if let Err(e) = ::std::fs::remove_file(&path) {
                    error!("ChunkStore failed to remove invalid chunk file {:?}: {}", path, e);
                }
                return Err(ChunkStorePutError::InternalError(From::from(e)));
            },
        };
        self.current_disk_usage += size;
        Ok(())
    }

    pub fn delete(&mut self, name: &::routing::NameType) -> Result<(), ChunkStoreInternalError> {
        match try!(self.dir_entry(name)) {
            None        => Ok(()),
            Some(entry) => {
                let metadata = match entry.metadata() {
                    Ok(m)  => m,
                    Err(e) => {
                        error!("ChunkStore failed to get metadata for {:?}: {}", entry.path(), e);
                        return Err(From::from(e));
                    }
                };
                match ::std::fs::remove_file(entry.path()) {
                    Ok(()) => (),
                    Err(e) => {
                        error!("ChunkStore failed to remove {:?}: {}", entry.path(), e);
                        return Err(From::from(e));
                    },
                };
                self.current_disk_usage -= metadata.len() as usize;
                Ok(())
            },
        }
    }

    pub fn get(&self, name: &::routing::NameType) -> Result<Option<Vec<u8>>, ChunkStoreInternalError> {
        use ::std::io::Read;
        match try!(self.dir_entry(name)) {
            None        => Ok(None),
            Some(entry) => {
                let mut file = try!(::std::fs::File::open(&entry.path()));
                let mut contents = Vec::<u8>::new();
                let _ = try!(file.read_to_end(&mut contents));
                Ok(Some(contents))
            }
        }
    }

    pub fn max_disk_usage(&self) -> usize {
        self.max_disk_usage
    }

    pub fn current_disk_usage(&self) -> usize {
        self.current_disk_usage
    }

    pub fn has_chunk(&self, name: &::routing::NameType) -> Result<bool, ChunkStoreInternalError> {
        Ok(try!(self.dir_entry(name)).is_some())
    }

    pub fn has_disk_space(&self, required_space: usize) -> bool {
        self.current_disk_usage + required_space <= self.max_disk_usage
    }

    /// Create an iterator that iterates over all the chunks in the chunks store.
    pub fn chunks(&self) -> Result<Chunks, ChunkStoreInternalError> {
        let dir_entries = try!(::std::fs::read_dir(&self.tempdir.path()));
        Ok(Chunks {
            dir_entries: dir_entries,
        })
    }

    fn dir_entry(&self, name: &::routing::NameType) -> Result<Option<::std::fs::DirEntry>, ChunkStoreInternalError> {
        let hex_name = name.as_hex();
        for dir_entry in try!(::std::fs::read_dir(&self.tempdir.path())) {
            let entry = try!(dir_entry);
            if entry.file_name().as_os_str() == ::std::ffi::OsStr::new(&hex_name[..]) {
                return Ok(Some(entry))
            }
        }
        Ok(None)
    }
}

struct ChunkReader {
    path: ::std::path::PathBuf,
}

impl ChunkReader {
    pub fn read(self) -> Result<Vec<u8>, ChunkStoreInternalError> {
        use ::std::io::Read;
        let mut file = try!(::std::fs::File::open(self.path));
        let mut data = Vec::new();
        let _ = try!(file.read_to_end(&mut data));
        Ok(data)
    }
}

struct Chunks {
    dir_entries: ::std::fs::ReadDir,
}

impl Iterator for Chunks {
    type Item = Result<(::routing::NameType, ChunkReader), ChunkStoreInternalError>;

    fn next(&mut self) -> Option<Self::Item> {
        let dir_entries = &mut self.dir_entries;
        for dir_entry in dir_entries {
            match dir_entry {
                Err(e)    => return Some(Err(From::from(e))),
                Ok(entry) => {
                    match entry.file_type().map(|ft| ft.is_file()) {
                        Ok(true)  => (),
                        Ok(false) => continue,
                        Err(e)    => return Some(Err(From::from(e))),
                    };
                    let path = entry.path();
                    let name_type = {
                        let name = match path.file_name().and_then(|name| name.to_str()) {
                            Some(name) => name,
                            None       => continue, // Ignore file name which contains invalid utf-8.
                        };
                        match ::routing::NameType::from_hex(name) {
                            Ok(name_type) => name_type,
                            Err(_)   => continue,   // Ignore file name which is not a valid NameType.
                        }
                    };
                    return Some(Ok((name_type, ChunkReader {
                        path: path,
                    })));
                }
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (_, h) = self.dir_entries.size_hint();
        (0, h)
    }
}

