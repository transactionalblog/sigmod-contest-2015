// SIGMOD Programming Contest 2015 - stub implementation
//
// This code is intended to illustrate the given challenge, it
// is not particular efficient. It is not guaranteed to be correct!
//---------------------------------------------------------------------------
// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <http://unlicense.org/>
//---------------------------------------------------------------------------

 #![feature(io,collections,core)]
use std::old_io::stdio::{stdin,stdout_raw};
use std::old_io::BufReader;
use std::iter::range;
use std::collections::HashMap;

fn expect<A,B>(r: Result<A,B>) -> A {
   r.unwrap_or_else(|_| panic!("Could not parse body"))
}

#[derive(Clone)]
struct Row {
   columns: Vec<u64>,
   transaction: u64
}

struct Relation {
   columns: u32,
   inserted: HashMap<u64,Row>,
   deleted: Vec<Row>
}

impl Relation {
   fn new(num_columns: u32) -> Relation {
      Relation {
         columns: num_columns,
         inserted: HashMap::new(),
         deleted: vec![]
      }
   }
}

struct Validator {
   relations: Vec<Relation>,
   validations: Vec<(u64,bool)>,
   last_forget: u64
}

impl Validator {
   fn new() -> Validator {
      Validator { 
         relations: vec![],
         validations: vec![],
         last_forget: 0 
      }
   }

   fn define_schema(&mut self, b: &mut BufReader) {
      // Read number of relations
      let count = expect(b.read_le_u32());
      // Read column count of each relation and create them
      self.relations = range(0,count).map(|_| {
         let num_columns = expect(b.read_le_u32());
         Relation::new(num_columns)
      }).collect();
   }

   #[inline(never)]
   fn transaction(&mut self, b: &mut BufReader) {
      // Read transaction id
      let transaction = expect(b.read_le_u64());
      let deletions = expect(b.read_le_u32());
      let insertions = expect(b.read_le_u32());

      // Delete rows by primary key
      for _ in range(0, deletions) {
         let relation = expect(b.read_le_u32());
         let rows = expect(b.read_le_u32());

         for _ in range(0, rows) {
            let primary_key = expect(b.read_le_u64());
            self.delete_row(transaction, relation, primary_key);
         }
      }

      // Insert rows with columns
      for _ in range(0, insertions) {
         let relation = expect(b.read_le_u32());
         let rows = expect(b.read_le_u32());

         for _ in range(0, rows) {
            self.insert_row(transaction, relation, b)
         }
      }
   }

   fn flush(&mut self, b: &mut BufReader) {
      writeln!(&mut std::old_io::stdio::stderr(), "Flushing").unwrap();
      let reference = expect(b.read_le_u64());

      self.validations.retain(|&(validation, result)| {
         if validation<=reference {
            match result {
               true => stdout_raw().write_char('0').unwrap(),
               false => stdout_raw().write_char('1').unwrap()
            };
            false
         } else {
            true
         }
      });
   }

   #[inline(never)]
   fn forget(&mut self, b: &mut BufReader) {
      let reference = expect(b.read_le_u64());
      self.last_forget = reference;
      // TODO: Implement
   }

   #[inline(never)]
   fn validate(&mut self, b: &mut BufReader) {
      let validation = expect(b.read_le_u64());
      let from_tx = expect(b.read_le_u64());
      let to_tx = expect(b.read_le_u64());
      let queries = expect(b.read_le_u32());
      let mut predicates_list = vec![];

      let result = range(0, queries).all(|_| {
         let relation = expect(b.read_le_u32());
         let predicates = expect(b.read_le_u32());
         predicates_list.clear();

         // Read predicates
         if predicates==0 {
            predicates_list.push((0, 6, 0));
         } else {
            predicates_list.extend(range(0, predicates).map(|_| {
               let column = expect(b.read_le_u32());
               let op = expect(b.read_le_u32());
               let reference = expect(b.read_le_u64());
               (column,op,reference)
            }));
         };

         // Test predicates against transaction history
         self.validate_predicates(from_tx, to_tx, relation, &predicates_list)
      });

      self.validations.push((validation, result));
   }

   #[inline(never)]
   fn validate_predicates(&self, from_tx: u64, to_tx: u64, relation_id: u32, predicates: &Vec<(u32,u32,u64)>) -> bool {
      let relation = &self.relations[relation_id as usize];

      let rows = relation.inserted.values()
         .chain(relation.deleted.iter())
         .filter(|row| row.transaction>=from_tx && row.transaction<=to_tx);

      for row in rows {
         let result = predicates.iter().all(|&(column,op,reference)| {
            match op {
               0 => row.columns[column as usize]==reference,
               1 => row.columns[column as usize]!=reference,
               2 => row.columns[column as usize]<reference,
               3 => row.columns[column as usize]<=reference,
               4 => row.columns[column as usize]>reference,
               5 => row.columns[column as usize]>=reference,
               6 => true,
               _ => panic!("Invalid operation")
            }
         });

         if result {
            return false;
         }
      }

      true
   }

   #[inline(never)]
   fn delete_row(&mut self, transaction: u64, relation_id: u32, primary_key: u64) {
      let relation = &mut self.relations[relation_id as usize];

      match relation.inserted.remove(&primary_key) {
         Some(row) => {
            relation.deleted.push(row.clone());
            let mut deleted = row.clone();
            deleted.transaction = transaction;
            relation.deleted.push(deleted);
         }
         None => {}
      }
   }

   fn insert_row(&mut self, transaction: u64, relation: u32, b: &mut BufReader) {
      let row : Vec<_> = range(0, self.relations[relation as usize].columns)
         .map(|_| expect(b.read_le_u64()))
         .collect();
      self.relations[relation as usize].inserted.insert(row[0], Row { transaction: transaction, columns: row });
   }
}

// Read requests from stdin and redirect to corresponding functions.
fn read_blocks() {
   let mut validator = Validator::new();
   let mut reader = stdin();
   let mut buffer = vec![];

   loop {
      let mlen = reader.read_le_u32().map(|x| x as usize);
      let mtype = reader.read_le_u32();

      match (mlen, mtype) {
         (Ok(l), Ok(t)) => {
            if l > buffer.len() { buffer.resize(l,0); }
            expect(reader.read_at_least(l, &mut buffer[0 .. l]));
            let mut buffer_reader = BufReader::new(&buffer);
            
            match t {
               0 => break,
               1 => validator.define_schema(&mut buffer_reader),
               2 => validator.transaction(&mut buffer_reader),
               3 => validator.validate(&mut buffer_reader),
               4 => validator.flush(&mut buffer_reader),
               5 => validator.forget(&mut buffer_reader),
               _ => panic!("Invalid message type")
            }
         },
         _ => panic!("Invalid input stream")
      }
   }
}

fn main() {
   read_blocks()
}
