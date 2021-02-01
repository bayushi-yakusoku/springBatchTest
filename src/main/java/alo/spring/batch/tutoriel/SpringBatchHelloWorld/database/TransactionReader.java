package alo.spring.batch.tutoriel.SpringBatchHelloWorld.database;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.lang.Nullable;

public class TransactionReader  implements ItemStreamReader<Transaction> {
    private final ItemStreamReader<FieldSet> fieldSetReader;
    private Integer recordCount = 0;
    private Integer expectedRecordCount = 0;

    public TransactionReader(ItemStreamReader<FieldSet> fieldSetReader) {
        this.fieldSetReader = fieldSetReader;
    }

    @Nullable
    private Transaction process(FieldSet fieldSet) {
        Transaction result = null;

        if (fieldSet != null) {
            // fieldSet not null means that we read a record
            if (fieldSet.getFieldCount() > 1) {
                // more than one field means transaction record
                result = new Transaction();
                result.setAccountNumber(fieldSet.readString(0));
                result.setTimestamp(fieldSet.readDate(1, "yyyy-MM-DD HH:mm:ss"));
                result.setAmount(fieldSet.readDouble(2));

                recordCount ++;
            }
            else {
                // less than 1 field means footer record
                expectedRecordCount = fieldSet.readInt(0);
            }
        }

        return result;
    }

    @Override
    public Transaction read() throws Exception {
        return process(fieldSetReader.read());
    }

    @AfterStep
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (recordCount.equals(expectedRecordCount)) {
            return stepExecution.getExitStatus();
        }

        return ExitStatus.STOPPED;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.fieldSetReader.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        this.fieldSetReader.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        this.fieldSetReader.close();
    }
}
