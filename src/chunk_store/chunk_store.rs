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
enum PutError {
    /// There was insufficient space to save the chunk.
    StorageLimitHit,
    /// There was an IO error occured during the put.
    IoError(::std::io::Error),
}

impl ::std::fmt::Display for PutError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        match *self {
            PutError::StorageLimitHit => "The chunk store storage limit was hit".fmt(f),
            PutError::IoError(ref e)  => e.fmt(f),
        }
    }
}

impl ::std::error::Error for PutError {
    fn description(&self) -> &str {
        match *self {
            PutError::StorageLimitHit => "The chunk store storage limit was hit",
            PutError::IoError(_)      => "I/O error",
        }
    }

    fn cause(&self) -> Option<&::std::error::Error> {
        match *self {
            PutError::StorageLimitHit => None,
            PutError::IoError(ref e)  => Some(e),
        }
    }
}

/// ChunkStore is a collection for holding all data chunks.
/// Implements a maximum disk usage to restrict storage.
pub struct ChunkStore {
    tempdir: ::tempdir::TempDir,
    max_disk_usage: usize,
    current_disk_usage: usize,
}

impl ChunkStore {
    /// Create new chunkstore with `max_disk_usage` allowed disk usage.
    pub fn new(max_disk_usage: usize) -> ::std::io::Result<ChunkStore> {
        let tempdir = try!(::tempdir::TempDir::new("safe_vault"));
        Ok(ChunkStore {
            tempdir: tempdir,
            max_disk_usage: max_disk_usage,
            current_disk_usage: 0,
        })
    }

    pub fn put(&mut self, name: &::routing::NameType, value: Vec<u8>) -> Result<(), PutError> {
        use ::std::io::Write;

        if !self.has_disk_space(value.len()) {
            warn!("Not enough space in ChunkStore.");
            return Err(PutError::StorageLimitHit);
        }

        let hex_name = name.as_hex();
        let path_name = ::std::path::Path::new(&hex_name);
        let path = self.tempdir.path().join(path_name);

        // If a file with name 'name' already exists, delete it.
        if let Err(e) = self.delete(name) {
            error!("ChunkStore failed to delete possibly preexisting file {:?}: {}", path, e);
            return Err(PutError::IoError(e));
        }

        let mut file = match ::std::fs::File::create(&path) {
            Ok(f)   => f,
            Err(e)  => {
                error!("ChunkStore failed to create chunk file {:?}: {}", path, e);
                return Err(PutError::IoError(e));
            }
        };
        let size = match file.write(&value[..]).and_then(|s| file.sync_all().map(|()| s)) {
            Ok(s)   => s,
            Err(e)  => {
                error!("ChunkStore failed to write chunk file {:?}: {}", path, e);
                if let Err(e) = ::std::fs::remove_file(&path) {
                    error!("ChunkStore failed to remove invalid chunk file {:?}: {}", path, e);
                }
                return Err(PutError::IoError(e));
            },
        };
        self.current_disk_usage += size;
        Ok(())
    }

    pub fn delete(&mut self, name: &::routing::NameType) -> ::std::io::Result<()> {
        match try!(self.dir_entry(name)) {
            None        => Ok(()),
            Some(entry) => {
                let metadata = match entry.metadata() {
                    Ok(m)  => m,
                    Err(e) => {
                        error!("ChunkStore failed to get metadata for {:?}: {}", entry.path(), e);
                        return Err(e);
                    }
                };
                match ::std::fs::remove_file(entry.path()) {
                    Ok(()) => (),
                    Err(e) => {
                        error!("ChunkStore failed to remove {:?}: {}", entry.path(), e);
                        return Err(e);
                    },
                };
                self.current_disk_usage -= metadata.len() as usize;
                Ok(())
            },
        }
    }

    pub fn get(&self, name: &::routing::NameType) -> ::std::io::Result<Option<Vec<u8>>> {
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

    pub fn has_chunk(&self, name: &::routing::NameType) -> ::std::io::Result<bool> {
        Ok(try!(self.dir_entry(name)).is_some())
    }

    pub fn has_disk_space(&self, required_space: usize) -> bool {
        self.current_disk_usage + required_space <= self.max_disk_usage
    }

    /// Create an iterator that iterates over all the chunks in the chunks store.
    pub fn chunks(&self) -> ::std::io::Result<Chunks> {
        let dir_entries = try!(::std::fs::read_dir(&self.tempdir.path()));
        Ok(Chunks {
            dir_entries: dir_entries,
        })
    }

    fn dir_entry(&self, name: &::routing::NameType) -> ::std::io::Result<Option<::std::fs::DirEntry>> {
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
    pub fn read(self) -> ::std::io::Result<Vec<u8>> {
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
    type Item = ::std::io::Result<(::routing::NameType, ChunkReader)>;

    fn next(&mut self) -> Option<Self::Item> {
        let dir_entries = &mut self.dir_entries;
        for dir_entry in dir_entries {
            match dir_entry {
                Err(e)    => return Some(Err(e)),
                Ok(entry) => {
                    match entry.file_type().map(|ft| ft.is_file()) {
                        Ok(true)  => (),
                        Ok(false) => continue,
                        Err(e)    => return Some(Err(e)),
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

