package alo.spring.batch.tutoriel.SpringBatchHelloWorld;

import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.lang.Nullable;

import java.util.Date;

/**
 * Add current time as a job parameter
 */
public class ParameterAddRunTime implements JobParametersIncrementer {

    private static String runTimeKey = "run.time";

    /**
     * Update job's parameters with one called "run.time" and set its value
     * with current time
     *
     * @param parameters the job's original parameter
     * @return updated parameters
     */
    @Override
    public JobParameters getNext(@Nullable JobParameters parameters) {
        JobParameters nextParameters = (parameters == null) ? new JobParameters() : parameters;

        return new JobParametersBuilder(nextParameters).addDate(runTimeKey, new Date()).toJobParameters();
    }
}
