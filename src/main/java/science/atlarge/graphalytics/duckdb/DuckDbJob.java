package science.atlarge.graphalytics.duckdb;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.io.IOException;
import java.nio.file.Paths;


public abstract class DuckDbJob {

	private static final Logger LOG = LogManager.getLogger();

	protected CommandLine commandLine;
    private final String jobId;
	private final String logPath;
	private final String inputPath;
	private final String outputPath;

	protected final RunSpecification runSpecification;
	protected final Graph benchmarkGraph;

	protected final DuckDbConfiguration platformConfig;

	/**
     * Initializes the platform job with its parameters.
	 * @param runSpecification the benchmark run specification.
	 * @param platformConfig the platform configuration.
	 * @param inputPath the file path of the input graph dataset.
	 * @param outputPath the file path of the output graph dataset.
	 */
	public DuckDbJob(RunSpecification runSpecification, DuckDbConfiguration platformConfig,
					 String inputPath, String outputPath, Graph benchmarkGraph) {
		BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();

		this.jobId = benchmarkRun.getId();
		this.logPath = benchmarkRunSetup.getLogDir().resolve("platform").toString();

		this.inputPath = inputPath;
		this.outputPath = outputPath;

		this.platformConfig = platformConfig;
		this.runSpecification = runSpecification;
		this.benchmarkGraph = benchmarkGraph;
	}


	/**
	 * Executes the platform job with the pre-defined parameters.
	 *
	 * @return the exit code
	 * @throws IOException if the platform failed to run
	 */
	public int execute() throws Exception {
		String executableDir = platformConfig.getExecutablePath();
		commandLine = new CommandLine(Paths.get(executableDir).toFile());

		// List of benchmark parameters.
		String jobId = getJobId();
		String logDir = getLogPath();

		// List of dataset parameters.
		String inputPath = getInputPath();
		String outputPath = getOutputPath();

		// List of platform parameters.
		int numThreads = platformConfig.getNumThreads();

		appendBenchmarkParameters(jobId, logDir);
		appendAlgorithmParameters();
		appendDatasetParameters(inputPath, outputPath);
		appendPlatformConfigurations(numThreads);

		String commandString = StringUtils.toString(commandLine.toStrings(), " ");
		LOG.info(String.format("Execute benchmark job with command-line: [%s]", commandString));

		Executor executor = new DefaultExecutor();
		executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));
		executor.setExitValue(0);
		return executor.execute(commandLine);
	}


	/**
	 * Appends the benchmark-specific parameters for the executable to a CommandLine object.
	 */
	private void appendBenchmarkParameters(String jobId, String logPath) {
		commandLine.addArgument("--job-id");
		commandLine.addArgument(jobId);

		commandLine.addArgument("--log-path");
		commandLine.addArgument(logPath);

		commandLine.addArgument("--directed");
		commandLine.addArgument(Boolean.toString(benchmarkGraph.isDirected()));
	}

	/**
	 * Appends the dataset-specific parameters for the executable to a CommandLine object.
	 */
	private void appendDatasetParameters(String inputPath, String outputPath) {
		commandLine.addArgument("--input-path");
		commandLine.addArgument(Paths.get(inputPath).toAbsolutePath().toString());

		commandLine.addArgument("--output-path");
		commandLine.addArgument(Paths.get(outputPath).toAbsolutePath().toString());
	}


	/**
	 * Appends the platform-specific parameters for the executable to a CommandLine object.
	 */
	private void appendPlatformConfigurations(int numThreads) {
		commandLine.addArgument("--num-threads");
		commandLine.addArgument(String.valueOf(numThreads));
	}


	/**
	 * Appends the algorithm-specific parameters for the executable to a CommandLine object.
	 */
	protected abstract void appendAlgorithmParameters();

	private String getJobId() {
		return jobId;
	}

	public String getLogPath() {
		return logPath;
	}

	private String getInputPath() {
		return inputPath;
	}

	private String getOutputPath() {
		return outputPath;
	}

}
