package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.CompositeJobParametersValidator;
import org.springframework.batch.core.job.DefaultJobParametersValidator;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.JobListenerFactoryBean;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.CallableTaskletAdapter;
import org.springframework.batch.core.step.tasklet.SimpleSystemProcessExitCodeMapper;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.concurrent.Callable;

@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchHelloWorldApplication {

	private static Logger logger = LoggerFactory.getLogger(SpringBatchHelloWorldApplication.class);

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
						new String[] {"fileName"},
						// Optional parameters:
						new String[] {"currentDate", "executionDate", "run.id", "transactionFile", "summaryFile"});

		defaultValidator.afterPropertiesSet();

		customValidator.setValidators(
				Arrays.asList(
						new ParameterValidator(),
						defaultValidator));

		return customValidator;
	}

	/* ********************************************
	   STEPS & TASKLETS
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
					logger.info("Hello, World!");

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
						logger.info("Hello, World!, parameter Name:" + name);

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
				logger.info("Parameter name:" + name);

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

	/**
	 * Step to illustrate the modification of the job context
	 * @return Step
	 */
	@Bean
	public Step stepWithContextModification() {
		return stepBuilderFactory
				.get("Step with Context modification")
				.tasklet(taskletWithContextModification())
				.build();
	}

	/**
	 * Tasklet to illustrate the modification of the Job Context
	 * @return Tasklet
	 */
	@Bean
	public Tasklet taskletWithContextModification() {
		return (new TaskletWithContextModification());
	}

	/**
	 * Callable will be executed in another threat
	 *
	 * @return Callable
	 */
	@Bean
	public Callable<RepeatStatus> callable() {
		return () -> {
			logger.info("Hello from callable object!");

			return RepeatStatus.FINISHED;
		};
	}

	/**
	 * Tasklet based on callable
	 *
	 * @return CallableAdapter
	 */
	@Bean
	public CallableTaskletAdapter tasklet() {
		CallableTaskletAdapter callableTaskletAdapter = new CallableTaskletAdapter();

		callableTaskletAdapter.setCallable(callable());

		return callableTaskletAdapter;
	}

	/**
	 * Step used for callable tasklet
	 *
	 * @return Step
	 */
	@Bean
	public Step callableStep() {
		return stepBuilderFactory
				.get("callableStep")
				.tasklet(tasklet())
				.build();
	}

	/**
	 * Tasklet running a simple system script and checking its returned code
	 *
	 * @return tasklet
	 */
	@Bean
	public Tasklet systemCommandTasklet() {
		logger.info("Entering system tasklet...");

		SystemCommandTasklet systemCommandTasklet = new SystemCommandTasklet();

		//systemCommandTasklet.setWorkingDirectory("/home/papa/Env/Dev/tmp");
		systemCommandTasklet.setCommand("/home/papa/Env/Dev/tmp/test.bash");
		systemCommandTasklet.setTimeout(10000);
		systemCommandTasklet.setInterruptOnCancel(true);
		systemCommandTasklet.setTerminationCheckInterval(5000);
		systemCommandTasklet.setTaskExecutor(new SimpleAsyncTaskExecutor());

		systemCommandTasklet.setSystemProcessExitCodeMapper((exitCode) -> {
			logger.info("Returned code: " + String.valueOf(exitCode));

			if (exitCode == 0) {
				return ExitStatus.COMPLETED;
			} else {
				return ExitStatus.FAILED;
			}
		});

		return systemCommandTasklet;
	}

	/**
	 * Step running a system script
	 *
	 * @return Step
	 */
	@Bean
	public Step stepSystem() {
		return stepBuilderFactory
				.get("Step System")
				.tasklet(systemCommandTasklet())
				.build();
	}

	/* ********************************************
	   JOBS
	   ******************************************** */

	/**
	 * Job with increment parameters
	 * @return job
	 */
	@Bean
	public Job job() {
		return this.jobBuilderFactory
				.get("job")
				.start(stepSimple())
				.incrementer(new ParameterAddRunTime())
				.build();
	}

	/**
	 * Job with listener using implements JobExecutionListener
	 * @return Job
	 */
	@Bean
	public  Job job1() {
		return jobBuilderFactory
				.get("job1")
				.start(stepWithParameter())
				.incrementer(new ParameterAddRunTime())
				.listener(new JobLoggerListener())
				.build();
	}

	/**
	 * Job with listener using annotations
	 * @return Job
	 */
	@Bean
	public Job job2() {
		return jobBuilderFactory
				.get("job2")
				.start(StepWithParameterWithLateBinding())
				.validator(customValidator())
				.incrementer(new RunIdIncrementer())
				.listener(JobListenerFactoryBean.getListener(new JobListenerWithAnotation()))
				.build();
	}

	/**
	 * Job that illustrates Context modifications
	 * @return Job
	 */
	@Bean
	public Job JobWithContextModification() {
		return jobBuilderFactory
				.get("JobWithContextModification")
				.start(stepWithContextModification())
				.incrementer(new ParameterAddRunTime())
				.build();
	}

	/**
	 * Local Class used to modify the Job Context
	 */
	public static class TaskletWithContextModification implements Tasklet {

		private static String fileNameKey = "fileName";
		private static String helloMsg = "Modification du contexte du job: {}";
		private static String contextMsg = "Ajout au contexte de la valeur: {}";

		@Override
		public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
			logger.info(helloMsg, chunkContext.getStepContext().getJobName());

			String fileName = (String) chunkContext.getStepContext().getJobParameters().get(fileNameKey);

			ExecutionContext stepContext = chunkContext.getStepContext().getStepExecution().getExecutionContext();

			ExecutionContext jobContext = chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();

			stepContext.put(fileNameKey, fileName);
			jobContext.put(fileNameKey, fileName);

			logger.info(contextMsg, fileName);

			return RepeatStatus.FINISHED;
		}
	}


	/**
	 * Simple job but with a callable Step
	 *
	 * @return Job
	 */
	@Bean
	public Job jobWithCallableStep() {
		return jobBuilderFactory
				.get("Job with Callable Step")
				.start(callableStep())
				.incrementer(new ParameterAddRunTime())
				.build();
	}

	/**
	 * Job running system command script and checking its returned code
	 *
	 * @return Job
	 */
	@Bean
	public Job jobWithSystemCommand() {
		return jobBuilderFactory
				.get("Job with System call")
				.start(stepSystem())
				.incrementer(new ParameterAddRunTime())
				.build();
	}

	/* ********************************************
	   MAIN
	   ******************************************** */

	private static ApplicationContext applicationContext;

	/**
	 * Run every Jobs
	 * @param args Parameters
	 */
	public static void main(String[] args) {
		applicationContext = SpringApplication.run(SpringBatchHelloWorldApplication.class, args);

		String[] allBeanNames = applicationContext.getBeanDefinitionNames();
	}
}
