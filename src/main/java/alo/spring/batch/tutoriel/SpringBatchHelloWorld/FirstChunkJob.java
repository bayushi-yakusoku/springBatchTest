package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.transform.PassThroughLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

@Configuration
public class FirstChunkJob {
    private static Logger logger = LoggerFactory.getLogger(SpringBatchHelloWorldApplication.class);

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ResourceLoader resourceLoader;

    public DefaultJobParametersValidator parameterValidator() {
        return new DefaultJobParametersValidator(
                new String[]{"inputFile", "outputFile"},
                new String[]{"run.time", "fileName", "executionDate"});
    }

    @Bean
    @StepScope
    public FlatFileItemReader<String> itemReader(@Value("#{jobParameters['inputFile']}") Resource inputFile) {
        logger.info("inputFile: " + inputFile);

        return new FlatFileItemReaderBuilder<String>()
                .name("itemReader")
                .resource(inputFile)
                .lineMapper(new PassThroughLineMapper())
                .build();
    }

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

    public Step stepReadFile() {
        return stepBuilderFactory
                .get("Step read file")
                .<String, String>chunk(10)
                .reader(itemReader(null))
                .writer(itemWriter(null))
                .build();
    }

    @Bean
    public Job jobReadFile() {
        return jobBuilderFactory
                .get("Job Read file")
                .start(stepReadFile())
                .validator(parameterValidator())
                .incrementer(new ParameterAddRunTime())
                .build();
    }
}
