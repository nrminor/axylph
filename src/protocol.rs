use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};

use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use tonic::Status;

use crate::{Limits, Progress};

pub const MESSAGE_BYTES: usize = 4 * 1024 * 1024;

pub fn schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    Arc::clone(SCHEMA.get_or_init(|| {
        let fields = vec![
            Field::new("id", DataType::Binary, false),
            Field::new("sequence", DataType::Binary, false),
            Field::new("quality", DataType::Binary, true),
        ];
        let metadata = HashMap::from([("axylph.version".to_owned(), "0".to_owned())]);
        let schema = Schema::new_with_metadata(fields, metadata);
        Arc::new(schema)
    }))
}

impl Progress {
    pub(crate) fn record<'a>(
        &mut self,
        batch: &'a RecordBatch,
        limits: Limits,
    ) -> Result<&'a BinaryArray, Status> {
        if batch.schema() != schema() {
            return Err(Status::invalid_argument(
                "expected the axylph version 0 read schema",
            ));
        }
        if batch.num_rows() > limits.batch_reads {
            return Err(Status::resource_exhausted("too many reads in one batch"));
        }
        let [ids, sequences, qualities] = batch.columns() else {
            return Err(Status::invalid_argument("expected three read columns"));
        };
        let [Some(ids), Some(sequences), Some(qualities)] =
            [ids, sequences, qualities].map(|column| column.as_any().downcast_ref::<BinaryArray>())
        else {
            return Err(Status::invalid_argument("expected binary read columns"));
        };
        if ids.null_count() != 0 || sequences.null_count() != 0 {
            return Err(Status::invalid_argument(
                "identifiers and sequences must not be null",
            ));
        }
        let mut bytes = 0usize;
        let mut bases = 0u64;
        for ((id, sequence), quality) in ids.iter().zip(sequences.iter()).zip(qualities.iter()) {
            let (Some(id), Some(sequence)) = (id, sequence) else {
                return Err(Status::invalid_argument("missing read data"));
            };
            if id.is_empty()
                || sequence.is_empty()
                || !sequence
                    .iter()
                    .all(|base| b"ACGTURYSWKMBDHVNacgturyswkmbdhvn".contains(base))
            {
                return Err(Status::invalid_argument(
                    "reads need an identifier and nonempty IUPAC nucleotide letters",
                ));
            }
            if let Some(quality) = quality
                && (quality.len() != sequence.len()
                    || !quality.iter().all(|q| (33..=126).contains(q)))
            {
                return Err(Status::invalid_argument(
                    "quality must have one printable Phred+33 byte per base",
                ));
            }
            for length in [id.len(), sequence.len(), quality.map_or(0, <[u8]>::len)] {
                bytes = bytes
                    .checked_add(length)
                    .ok_or_else(|| Status::resource_exhausted("batch size overflow"))?;
            }
            bases = bases
                .checked_add(
                    u64::try_from(sequence.len())
                        .map_err(|_| Status::resource_exhausted("sequence too long"))?,
                )
                .ok_or_else(|| Status::resource_exhausted("base count overflow"))?;
        }
        if bytes > limits.batch_bytes {
            return Err(Status::resource_exhausted(
                "too many decoded bytes in one batch",
            ));
        }
        let reads = self
            .reads
            .checked_add(
                u64::try_from(batch.num_rows())
                    .map_err(|_| Status::resource_exhausted("read count overflow"))?,
            )
            .ok_or_else(|| Status::resource_exhausted("read count overflow"))?;
        let bases = self
            .bases
            .checked_add(bases)
            .ok_or_else(|| Status::resource_exhausted("base count overflow"))?;
        self.reads = reads;
        self.bases = bases;
        Ok(sequences)
    }
}
