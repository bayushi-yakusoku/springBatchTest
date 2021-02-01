package alo.spring.batch.tutoriel.SpringBatchHelloWorld.database;

import org.springframework.batch.item.ItemProcessor;

import java.util.List;

public class TransactionApplierProcessor implements ItemProcessor<AccountSummary, AccountSummary> {

    private TransactionDao transactionDao;

    public TransactionApplierProcessor(TransactionDao transactionDao) {
        this.transactionDao = transactionDao;
    }

    @Override
    public AccountSummary process(AccountSummary item) throws Exception {
        List<Transaction> transactions = transactionDao.getTransactionByAccountNumber(item.getAccountNumber());

        Double currentBalance = (double) 0;

        for (Transaction transaction :
                transactions) {
            currentBalance += transaction.getAmount();
        }

        item.setCurrentBalance(currentBalance);

        return item;
    }
}
