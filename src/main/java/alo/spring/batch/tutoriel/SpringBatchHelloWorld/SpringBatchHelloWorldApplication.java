package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import javax.xml.validation.Validator;
import java.lang.reflect.Parameter;
import java.util.Arrays;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchHelloWorldApplication {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	/* ********************************************
	   PARAMETERS
	   ******************************************** */

	@Bean
	public CompositeJobParametersValidator customValidator() {
		CompositeJobParametersValidator customValidator = new CompositeJobParametersValidator();

		DefaultJobParametersValidator defaultValidator =
				new DefaultJobParametersValidator(
						// Required parameters:
						new String[] {"fileName", "name"},
						// Optional parameters:
						new String[] {"currentDate", "executionDate"});

		defaultValidator.afterPropertiesSet();

		customValidator.setValidators(
				Arrays.asList(
						new ParameterValidator(),
						defaultValidator));

		return customValidator;
	}

	/* ********************************************
	   STEPS
	   ******************************************** */

	/**
	 * Simple Step with a single print Hello, World!
	 *
	 * @return Step
	 */
	public Step stepSimple() {
		return this.stepBuilderFactory
				.get("step1")
				.tasklet((contribution, chunkContext) -> {
					System.out.println("Hello, World!");
					return RepeatStatus.FINISHED;
				})
				.build();
	}

	/**
	 * Step that deals with parameters and print the value of name
	 *
	 * @return Step
	 */
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

	/**
	 * Tasklet that deals with parameters and show how to use Spring EL
	 * with late binding to fill the method parameter
	 *
	 * @param name the value of the parameter 'name'
	 * @return Tasklet
	 */
	@StepScope
	@Bean
	public Tasklet taskletWithLateBindingParameter(@Value("#{jobParameters['name']}") String name) {
		return new Tasklet() {
			@Override
			public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) {

				System.out.println("Parameter name:" + name);
				return RepeatStatus.FINISHED;
			}
		};
	}

	/**
	 * Step to illustrate the use of late binding
	 *
	 * @return Step
	 */
	public Step StepWithParameterWithLateBinding() {
		return stepBuilderFactory
				.get("Step1")
				.tasklet(taskletWithLateBindingParameter(null))
				.build();
	}

	/* ********************************************
	   JOBS
	   ******************************************** */

	@Bean
	public Job job() {
		return this.jobBuilderFactory
				.get("job")
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

	@Bean
	public Job job2() {
		return jobBuilderFactory
				.get("job2")
				.start(StepWithParameterWithLateBinding())
				.validator(customValidator())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchHelloWorldApplication.class, args);
	}
}
