package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FirstChunkJob {
    private static Logger logger = LoggerFactory.getLogger(SpringBatchHelloWorldApplication.class);

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

//    @Bean
    public Step stepReadFile() {
        return stepBuilderFactory
                .get("Step read file")
                .tasklet((contribution, chunkContext) -> {
                    String fileName = "";
                    fileName = (String) chunkContext.getStepContext().getJobParameters().get("fileName");

                    logger.info("Processing file: " + fileName);

                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    @Bean
    public Job jobReadFile() {
        return jobBuilderFactory
                .get("Job Read file")
                .start(stepReadFile())
                .incrementer(new ParameterAddRunTime())
                .build();
    }
}
