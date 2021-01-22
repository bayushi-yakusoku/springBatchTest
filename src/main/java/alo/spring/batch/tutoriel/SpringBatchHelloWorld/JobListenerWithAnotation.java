package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.annotation.AfterJob;
import org.springframework.batch.core.annotation.BeforeJob;

public class JobListenerWithAnotation {
    private static Logger logger = LoggerFactory.getLogger(JobListenerWithAnotation.class);

    private static String START_MESSAGE ="%s est lancé! (Annotation)";
    private static String END_MESSAGE = "%s est terminé! (Annotation)";

    @BeforeJob
    public void beforeJob(JobExecution jobExecution) {
        logger.info(String.format(START_MESSAGE, jobExecution.getJobInstance().getJobName()));

    }

    @AfterJob
    public void afterJob(JobExecution jobExecution) {
        logger.info(String.format(END_MESSAGE, jobExecution.getJobInstance().getJobName()));
    }
}
