package com.example.template.persistence.streamstore;

/** One stream row; {@code op} is "upsert" or "delete", {@code seqno} is assigned by the store. */
public record LsmRow(long id, String name, String value, long seqno, String op) {

    public boolean isDelete() {
        return "delete".equals(op);
    }

    LsmRow withSeqno(long newSeqno) {
        return new LsmRow(id, name, value, newSeqno, op);
    }
}
