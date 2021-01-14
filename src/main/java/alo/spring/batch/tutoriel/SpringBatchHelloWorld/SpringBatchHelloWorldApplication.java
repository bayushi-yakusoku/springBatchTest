package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchHelloWorldApplication {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	/* ********************************************
	   STEPS
	   ******************************************** */

	//@Bean
	public Step stepSimple() {
		return this.stepBuilderFactory
				.get("step1")
				.tasklet((contribution, chunkContext) -> {
					System.out.println("Hello, World!");
					return RepeatStatus.FINISHED;
				})
				.build();
	}

	public Step stepWithParameter() {
		return stepBuilderFactory
				.get("Step1")
				.tasklet(new Tasklet() {
					@Override
					public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) {

						String name = (String) chunkContext.getStepContext().getJobParameters().get("name");

						System.out.println("Hello, World!, parameter Name:" + name);

						return RepeatStatus.FINISHED;
					}
				})
				.build();
	}

	/* ********************************************
	   JOBS
	   ******************************************** */

	@Bean
	public Job job() {
		return this.jobBuilderFactory.get("job")
				.start(stepSimple())
				.build();
	}

	@Bean
	public  Job job1() {
		return jobBuilderFactory
				.get("job1")
				.start(stepWithParameter())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchHelloWorldApplication.class, args);
	}
}
