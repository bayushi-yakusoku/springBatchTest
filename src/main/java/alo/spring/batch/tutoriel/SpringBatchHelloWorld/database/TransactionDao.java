package alo.spring.batch.tutoriel.SpringBatchHelloWorld.database;

import java.util.List;

public interface TransactionDao {
    public List<Transaction> getTransactionByAccountNumber(String accountNumber);
}
