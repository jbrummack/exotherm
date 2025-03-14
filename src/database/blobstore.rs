use foundationdb::options::StreamingMode;
use uuid::Uuid;

use crate::error::SResult;

pub struct ShardedBlob(pub Vec<u8>);
#[derive(Debug, rkyv::Serialize, rkyv::Archive, rkyv::Deserialize)]
pub struct BlobKey(String, Uuid);

#[allow(unused_variables)]
impl BlobKey {
    pub fn build_key(&self, tenant: &str) -> Vec<u8> {
        todo!()
    }
}
#[allow(dead_code)]
impl ShardedBlob {
    pub async fn load(txn: &foundationdb::Transaction, key: Uuid) -> SResult<Self> {
        //let key = Uuid::new_v4();
        let mut start = key.as_bytes().to_vec();
        let mut end = key.as_bytes().to_vec();
        for b in usize::MIN.to_be_bytes() {
            start.push(b);
        }
        for b in usize::MAX.to_be_bytes() {
            end.push(b);
        }
        let mut opt = foundationdb::RangeOption::from((start, end));
        opt.mode = StreamingMode::Serial;
        let range = txn.get_range(&opt, 1000, true).await?;
        let mut blob = Vec::<u8>::new();
        for kv in range {
            for byte in kv.value() {
                blob.push(*byte);
            }
        }
        Ok(ShardedBlob(blob))
    }
    pub async fn store(&self, txn: &foundationdb::Transaction, key: Uuid) -> SResult<()> {
        let shards = self.shard();
        for (key_id, shard) in shards.into_iter().enumerate() {
            let mut combined_key = key.as_bytes().to_vec();
            for byte in key_id.to_be_bytes() {
                combined_key.push(byte);
            }
            txn.set(&combined_key, &shard);
        }
        Ok(())
    }
    pub fn shard(&self) -> Vec<Vec<u8>> {
        let ShardedBlob(data) = self;

        let chunk_size = 50 * 1024; // 50 KB

        let chunks: Vec<Vec<u8>> = data
            .chunks(chunk_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        //println!("Total chunks: {}", chunks.len());
        chunks
    }
    pub fn unshard(shards: Vec<&[u8]>) -> Self {
        let mut buf = Vec::<u8>::new();

        for shard in shards {
            for byte in shard {
                buf.push(*byte);
            }
        }
        ShardedBlob(buf)
    }
}
