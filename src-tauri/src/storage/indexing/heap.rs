use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use byteorder::{LittleEndian, ReadBytesExt};
use crate::catalog::table::Table;
use crate::types::data_types::DataValue;

pub struct HeapFile {
    table_info: Table,
    writer: BufWriter<File>,
    reader: BufReader<File>,
}

impl HeapFile {
    pub fn new(table_info: Table, file: File) -> Self {
        let writer = BufWriter::new(file.try_clone().unwrap());
        let reader = BufReader::new(file.try_clone().unwrap());
        HeapFile { table_info, writer, reader }
    }
    pub fn insert_row(&mut self, data: Vec<DataValue>) -> Result<(), String> {
        let values = data
            .into_iter()
            .zip(self.table_info.get_tab_columns().iter());

        for (value, col) in values {
            let bytes = value.to_bytes(col.get_att_len());
            self.writer
                .write_all(&bytes)
                .map_err(|e| format!("I/O error writing to heap file: {}", e))?;
        }
        Ok(())
    }
    pub fn finalize(mut self) -> Result<(), String> {
        self.writer
            .flush()
            .map_err(|e| format!("I/O error flushing heap file: {}", e))
    }
    pub fn get_row(&mut self, id: &str) {}
    pub fn get_all_rows(&mut self) -> Result<String, ()> {
        self.reader.seek(SeekFrom::Start(0)).unwrap();
        let mut result = String::new();
        let columns = self.table_info.get_tab_columns();
        
        loop {
            let mut row_data: Vec<DataValue> = Vec::new();
            let mut row_complete = true;
            
            for col in columns {
                let att_len = col.get_att_len();
                let mut buffer = vec![0u8; att_len as usize];

                // Intentar leer los bytes para esta columna
                match self.reader.read_exact(&mut buffer) {
                    Ok(_) => {
                        // Convertir los bytes a un DataValue
                        let data_value = DataValue::from_bytes(&buffer, col.get_att_type_id(), att_len);
                        row_data.push(data_value);
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        row_complete = false;
                        break;
                    },
                    _ => {}
                }
            }
            if !row_complete {
                break;
            }
            let mut row_str = String::new();
            for (i, (value, col)) in row_data.iter().zip(columns.iter()).enumerate() {
                if i > 0 {
                    row_str.push_str(", ");
                }
                row_str.push_str(&format!("{}={}", col.get_att_name(), value.to_string()));
            }

            result.push_str(&row_str);
            result.push('\n');
        }
        Ok(result)
    }
    pub fn update_row(&mut self, id: &str, data: &[u8]) {}
    pub fn delete_row(&mut self, id: &str) {}
}

