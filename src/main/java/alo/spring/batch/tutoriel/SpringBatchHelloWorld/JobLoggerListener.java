package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

public class JobLoggerListener implements JobExecutionListener {

    private static final Log logger = LogFactory.getLog(JobLoggerListener.class);

    private static String START_MESSAGE ="%s est lancé!";
    private static String END_MESSAGE = "%s est terminé!";

    @Override
    public void beforeJob(JobExecution jobExecution) {
        logger.info(String.format(START_MESSAGE, jobExecution.getJobInstance().getJobName()));

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        logger.info(String.format(END_MESSAGE, jobExecution.getJobInstance().getJobName()));
    }
}
