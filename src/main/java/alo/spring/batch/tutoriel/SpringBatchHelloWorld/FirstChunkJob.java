package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

@Configuration
public class FirstChunkJob {
    private static final Logger logger = LoggerFactory.getLogger(SpringBatchHelloWorldApplication.class);

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    /* ***********************************************************************************************************
       PARAMETERS VALIDATION
     * ***********************************************************************************************************/

    /**
     * Parameters validation
     *
     * @return Validator
     */
    public DefaultJobParametersValidator parameterValidator() {
        return new DefaultJobParametersValidator(
                new String[]{"inputFile", "outputFile"},
                new String[]{"run.time", "fileName", "executionDate"});
    }

    /* ***********************************************************************************************************
       STEP FOR PRE-PROCESSING THE FILE
     * ***********************************************************************************************************/

    @Bean
    public Tasklet taskletPreProcessingFile() {
        return (contribution, chunkContext) -> {
            logger.info("Pre-processing the file...");

                return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step stepPreProcessingFile() {
        return stepBuilderFactory
                .get("Pre-Processing File Step")
                .tasklet(taskletPreProcessingFile())
                .build();
    }

    /* ***********************************************************************************************************
       STEP FOR FAILURE
     * ***********************************************************************************************************/

    @Bean
    public Tasklet taskletOnFailure() {
        return (contribution, chunkContext) -> {
            logger.error("Error during processing...");

            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step stepOnFailure() {
        return stepBuilderFactory
                .get("On Failure Step")
                .tasklet(taskletOnFailure())
                .build();
    }

    /* ***********************************************************************************************************
       STEP THAT PROCESS A FILE
     * ***********************************************************************************************************/

    /**
     * Item Reader
     *
     * @param inputFile file to be read
     *
     * @return ItemReader
     */
    @Bean
    @StepScope
    public FlatFileItemReader<String> itemReader(@Value("#{jobParameters['inputFile']}") Resource inputFile) {
        logger.info("inputFile: " + inputFile.getFilename());

        return new FlatFileItemReaderBuilder<String>()
                .name("itemReader")
                .resource(inputFile)
                .lineMapper(new PassThroughLineMapper())
                .build();
    }

    /**
     * Item Writer
     *
     * @param outputFile file to be created
     *
     * @return ItemWriter
     */
    @Bean
    @StepScope
    public FlatFileItemWriter<String> itemWriter(@Value("#{jobParameters['outputFile']}") Resource outputFile) {
        logger.info("outputFile: " + outputFile.getFilename());

        return new FlatFileItemWriterBuilder<String>()
                .name("itemWriter")
                .resource(outputFile)
                .lineAggregator(new PassThroughLineAggregator<>())
                .build();
    }

    /**
     * Step for processing a file
     *
     * @return Step
     */
    public Step stepProcessingFile() {
        return stepBuilderFactory
                .get("Step read file")
                .<String, String>chunk(10)
                .reader(itemReader(null))
                .writer(itemWriter(null))
                .build();
    }

    /* ***********************************************************************************************************
       JOB FOR PROCESSING A FILE
     * ***********************************************************************************************************/

    /**
     * Job for processing File
     *
     * @return Job
     */
    @Bean
    public Job jobReadFile() {
        return jobBuilderFactory
                .get("Job Processing file")
                .validator(parameterValidator())
                .incrementer(new ParameterAddRunTime())

                // Pre-processing
                .start(stepPreProcessingFile())
                .on("FAILED").to(stepOnFailure())

                // Processing
                .from(stepPreProcessingFile())
                .on("*")
                .to(stepProcessingFile())
                .on("FAILED").to(stepOnFailure())

                .end()
                .build();
    }
}
