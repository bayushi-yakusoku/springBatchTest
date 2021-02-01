package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import alo.spring.batch.tutoriel.SpringBatchHelloWorld.database.*;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughFieldSetMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
public class TransactionsProcessingJob {

    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Autowired
    @Qualifier("bankDataSource")
    public DataSource bankDataSource;

    public DefaultJobParametersValidator parameterValidator() {
        return new DefaultJobParametersValidator(
                new String[]{"transactionFile", "summaryFile"},
                new String[]{"inputFile", "outputFile", "run.time", "fileName", "executionDate"});
    }

    /* ********************************************************************************************************
       STEP IMPORTING TRANSACTIONS INTO DATABASE
     * ********************************************************************************************************/
    @Bean
    @StepScope
    public TransactionReader transactionReader() {
        return new TransactionReader(fileItemReader(null));
    }

    @Bean
    @StepScope
    public FlatFileItemReader<FieldSet> fileItemReader(@Value("#{jobParameters['transactionFile']}") Resource inputFile) {
        return new FlatFileItemReaderBuilder<FieldSet>()
                .name("fileItemReader")
                .resource(inputFile)
                .lineTokenizer(new DelimitedLineTokenizer())
                .fieldSetMapper(new PassThroughFieldSetMapper())
                .build();
    }

    @Bean
    public JdbcBatchItemWriter<Transaction> transactionWriter(@Qualifier("bankDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO TRANSACTION " +
                        "(ACCOUNT_SUMMARY_ID, TIMESTAMP, AMOUNT) " +
                        "VALUES ((SELECT ID FROM ACCOUNT_SUMMARY " +
                        "WHERE ACCOUNT_NUMBER = :accountNumber), " +
                        ":timestamp, :amount)")
                .build();
    }

    @Bean
    public Step stepImportTransactionFile() {
        return stepBuilderFactory
                .get("stepImportTransactionFile")
                .<Transaction, Transaction>chunk(10)
                .reader(transactionReader())
                .writer(transactionWriter(null))
                .allowStartIfComplete(true)
                .listener(transactionReader())
                .build();
    }

    /* ********************************************************************************************************
       STEP APPLY TRANSACTIONS TO ACCOUNT BALANCE
     * ********************************************************************************************************/
    @Bean
    @StepScope
    public JdbcCursorItemReader<AccountSummary> accountSummaryReader(@Qualifier("bankDataSource") DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<AccountSummary>()
                .name("accountSummaryReader")
                .dataSource(dataSource)
                .sql("SELECT ACCOUNT_NUMBER, CURRENT_BALANCE " +
                        "FROM ACCOUNT_SUMMARY A " +
                        "WHERE A.ID IN (" +
                        "	SELECT DISTINCT T.ACCOUNT_SUMMARY_ID " +
                        "	FROM TRANSACTION T) " +
                        "ORDER BY A.ACCOUNT_NUMBER")
                .rowMapper((rs, rowNum) -> {
                    AccountSummary summary = new AccountSummary();

                    summary.setAccountNumber(rs.getString("account_number"));
                    summary.setCurrentBalance(rs.getDouble("current_balance"));

                    return summary;
                })
                .build();
    }

    @Bean
    public TransactionDao transactionDao(@Qualifier("bankDataSource") DataSource dataSource) {
        return new TransactionDaoSupport(dataSource);
    }

    @Bean
    public TransactionApplierProcessor transactionApplierProcessor(@Qualifier("bankDataSource") DataSource dataSource) {
        return new TransactionApplierProcessor(transactionDao(dataSource));
    }

    @Bean
    public JdbcBatchItemWriter<AccountSummary> accountSummaryWriter(@Qualifier("bankDataSource") DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<AccountSummary>()
                .dataSource(dataSource)
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("UPDATE ACCOUNT_SUMMARY " +
                        "SET CURRENT_BALANCE = :currentBalance " +
                        "WHERE ACCOUNT_NUMBER = :accountNumber")
                .build();
    }

    @Bean
    public Step stepApplyTransaction() {
        return stepBuilderFactory
                .get("stepApplyTransaction")
                .<AccountSummary, AccountSummary>chunk(10)
                .reader(accountSummaryReader(null))
                .processor(transactionApplierProcessor(null))
                .writer(accountSummaryWriter(null))
                .build();
    }

    /* ********************************************************************************************************
       STEP GENERATE ACCOUNT SUMMARY FILE
     * ********************************************************************************************************/
    @Bean
    @StepScope
    public FlatFileItemWriter<AccountSummary> accountSummaryFileWriter(@Value("#{jobParameters['summaryFile']}") Resource outputFile) {
        DelimitedLineAggregator<AccountSummary> lineAggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<AccountSummary> fieldExtractor = new BeanWrapperFieldExtractor<>();

        fieldExtractor.setNames(new String[] {"accountNumber", "currentBalance"});
        fieldExtractor.afterPropertiesSet();

        lineAggregator.setFieldExtractor(fieldExtractor);

        return new FlatFileItemWriterBuilder<AccountSummary>()
                .name("accountSummaryFileWriter")
                .lineAggregator(lineAggregator)
                .resource(outputFile)
                .build();
    }

    @Bean
    public Step stepGenerateAccountSummary() {
        return stepBuilderFactory
                .get("stepGenerateAccountSummary")
                .<AccountSummary, AccountSummary>chunk(10)
                .reader(accountSummaryReader(null))
                .writer(accountSummaryFileWriter(null))
                .build();
    }

    /* ********************************************************************************************************
       STEP GENERATE ACCOUNT SUMMARY FILE
     * ********************************************************************************************************/
    @Bean
    public Job jobTransactionsProcessing() {
        return jobBuilderFactory
                .get("jobTransactionsProcessing")
                .validator(parameterValidator())
                .incrementer(new ParameterAddRunTime())

                // Import Transactions from file into database
                .start(stepImportTransactionFile())

                // Restart Import Transaction File step
                .on("STOPPED")
                .stopAndRestart(stepImportTransactionFile())

                // Update Accounts balance in database
                .from(stepImportTransactionFile())
                .on("*")
                .to(stepApplyTransaction())

                // Generate Accounts balance report file
                .from(stepApplyTransaction())
                .next(stepGenerateAccountSummary())

                .end()
                .build();
    }
}
