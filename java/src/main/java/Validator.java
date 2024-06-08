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

package validator;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;

public class Validator {

   private static class Row implements Cloneable {
      public long transaction;
      public ArrayList<Long> columns;
      
      public Row(long transaction) {
         this.transaction = transaction;
         this.columns = new ArrayList<Long>();
      }
      
      public Row clone() {
         Row cln = new Row(transaction);
         cln.columns = columns;
         return cln;
      }
   }
   
   private static class Relation {
      public int columns;
      public HashMap<Long, Row> inserted;
      public ArrayList<Row> deleted;
      
      public Relation(int columns) {
         this.columns = columns;
         this.inserted = new HashMap<Long, Validator.Row>();
         this.deleted = new ArrayList<Row>();
      }
   }
   
   private static class Validation {
      public long id;
      public boolean success;
      
      public Validation(long id) {
         success = true;
      }
   }
   
   private static class Predicate {
      public int column;
      public int operation;
      public long reference;
      
      public Predicate(int column, int operation, long reference) {
         this.column = column;
         this.operation = operation;
         this.reference = reference;
      }
   }
   
   private ArrayList<Relation> relations;
   private ArrayList<Validation> validations;

   private Validator() {
      relations = new ArrayList<Relation>();
      validations = new ArrayList<Validation>();
   }

   private static int readInt(DataInputStream in) throws IOException {
      return ByteBuffer.allocate(4)
         .order(ByteOrder.BIG_ENDIAN).putInt(in.readInt())
         .order(ByteOrder.LITTLE_ENDIAN).getInt(0);
   }

   private static long readLong(DataInputStream in) throws IOException {
       return ByteBuffer.allocate(8)
         .order(ByteOrder.BIG_ENDIAN).putLong(in.readLong())
         .order(ByteOrder.LITTLE_ENDIAN).getLong(0);
   }
   
   private void defineSchema(DataInputStream in) throws IOException {
      int count = readInt(in);
      for (int i = 0; i < count; i++) {
         int numColumns = readInt(in);
         relations.add(new Relation(numColumns));
      }
   }
   
   private void transaction(DataInputStream in) throws IOException {
      long transaction = readLong(in);
      int deletions = readInt(in);
      int insertions = readInt(in);
      
      for (int i = 0; i < deletions; i++) {
         int relation = readInt(in);
         int rows = readInt(in);
         for (int j = 0; j < rows; j++) {
            long key = readLong(in);
            deleteRow(transaction, relation, key);
         }
      }
      
      for (int i = 0; i < insertions; i++) {
         int relation = readInt(in);
         int rows = readInt(in);
         for (int j = 0; j < rows; j++) {
            insertRow(transaction, relation, in);
         }        
      }
   }
   
   private void flush(DataInputStream in) throws IOException {
      long referenceId = readLong(in);
      
      ArrayList<Validation> remaining = new ArrayList<Validation>();
      for (Validation validation : validations) {
         if(validation.id <= referenceId) {
            if(validation.success) {
               System.out.write('0');
            } else {
               System.out.write('1');
            }
         } else {
            remaining.add(validation);
         }
      }
      
      System.out.flush();
      validations = remaining;
   }
   
   private void forget(DataInputStream in) throws IOException {
      /*long forgetId =*/ readLong(in);
      // TODO: Implement forget
   }

   private void validate(DataInputStream in) throws IOException {
      long validationId = readLong(in);
      long fromTx = readLong(in);
      long toTx = readLong(in);
      long queries = readInt(in);
      
      Validation validation = new Validation(validationId);
      ArrayList<Predicate> predicates = new ArrayList<Predicate>();
      
      for (int i = 0; i < queries; i++) {
         int relation = readInt(in);
         int count = readInt(in);
         predicates.clear();
         
         if (count==0) {
            predicates.add(new Predicate(0, 6, 0));
         } else {
            for (int j = 0; j < count; j++) {
               int column = readInt(in);
               int operation = readInt(in);
               long reference = readLong(in);
               predicates.add(new Predicate(column, operation, reference));
            }
         }
         
         if (validation.success) {
            boolean result = validatePredicates(fromTx, toTx, relation, predicates);
            if (!result) {
               validation.success = false;
            }
         }
      }

      validations.add(validation);
   }
   
   private boolean validatePredicates(long fromTx, long toTx, int relationId, ArrayList<Predicate> predicates) {
      for (Row row : relations.get(relationId).inserted.values()) {
         if (row.transaction>=fromTx && row.transaction<=toTx) {
            boolean allMatch = true;
            for (Predicate predicate : predicates) {
               if (!validatePredicate(row, predicate)) {
                  allMatch = false;
                  break;
               }
            }
            if (allMatch) {
               return false;
            }
         }
      }
      
      for (Row row : relations.get(relationId).deleted) {
         if (row.transaction>=fromTx && row.transaction<=toTx) {
            boolean allMatch = true;
            for (Predicate predicate : predicates) {
               if (!validatePredicate(row, predicate)) {
                  allMatch = false;
                  break;
               }
            }
            if (allMatch) {
               return false;
            }
         }
      }
      
      return true;
   }

   private boolean validatePredicate(Row row, Predicate predicate) {
      switch (predicate.operation) {
      case 0: return row.columns.get(predicate.column)==predicate.reference;
      case 1: return row.columns.get(predicate.column)!=predicate.reference;
      case 2: return Long.compareUnsigned(row.columns.get(predicate.column),predicate.reference)<0;
      case 3: return Long.compareUnsigned(row.columns.get(predicate.column),predicate.reference)<=0;
      case 4: return Long.compareUnsigned(row.columns.get(predicate.column),predicate.reference)>0;
      case 5: return Long.compareUnsigned(row.columns.get(predicate.column),predicate.reference)>= 0;
      case 6: return true;
      default: throw new RuntimeException("Invalid predicate operator");
      }
   }

   private void deleteRow(long transaction, int relation, long key) {
      if (relations.get(relation).inserted.containsKey(key)) {
         Row row = relations.get(relation).inserted.remove(key);
         relations.get(relation).deleted.add(row.clone());
         row.transaction = transaction;
         relations.get(relation).deleted.add(row);
      }
   }
   
   private void insertRow(long transaction, int relation, DataInputStream in) throws IOException {
      Row row = new Row(transaction);
      for (int i = 0; i < relations.get(relation).columns; i++) {
         long value = readLong(in);
         row.columns.add(value);
      }
      
      relations.get(relation).inserted.put(row.columns.get(0), row);
   }
   
   public static void main(String[] args) throws IOException {
      Validator validator = new Validator();

      DataInputStream in = new DataInputStream(System.in);
      while(true) {
         int rlen = readInt(in);
         int rtype = readInt(in);

         switch (rtype) {
         case 0: return;
         case 1: validator.defineSchema(in); break;
         case 2: validator.transaction(in); break;
         case 3: validator.validate(in); break;
         case 4: validator.flush(in); break;
         case 5: validator.forget(in); break;
         default: throw new IOException("Invalid request type "+rtype);
         }
      }
   }
}
