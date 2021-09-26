package science.atlarge.graphalytics.duckdb;

import org.apache.commons.exec.CommandLine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.BenchmarkRunSetup;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.io.IOException;
import java.sql.Connection;


public abstract class DuckdbJob {

	private static final Logger LOG = LogManager.getLogger();

	protected CommandLine commandLine;
    private final String jobId;
	private final String logPath;
	private final String inputPath;
	private final String outputPath;

	protected final RunSpecification runSpecification;
	protected final Graph benchmarkGraph;
	protected final Connection connection;

	protected final DuckdbConfiguration platformConfig;

	/**
     * Initializes the platform job with its parameters.
	 * @param runSpecification the benchmark run specification.
	 * @param platformConfig the platform configuration.
	 * @param inputPath the file path of the input graph dataset.
	 * @param outputPath the file path of the output graph dataset.
	 */
	public DuckdbJob(RunSpecification runSpecification, DuckdbConfiguration platformConfig,
					 String inputPath, String outputPath, Graph benchmarkGraph, Connection connection) {
		BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();

		this.jobId = benchmarkRun.getId();
		this.logPath = benchmarkRunSetup.getLogDir().resolve("platform").toString();

		this.inputPath = inputPath;
		this.outputPath = outputPath;

		this.platformConfig = platformConfig;
		this.runSpecification = runSpecification;
		this.benchmarkGraph = benchmarkGraph;
		this.connection = connection;
	}

	/**
	 * Executes the platform job with the pre-defined parameters.
	 *
	 * @return the exit code
	 * @throws IOException if the platform failed to run
	 */
	 public abstract int execute() throws Exception;

//	/**
//	 * Executes the platform job with the pre-defined parameters.
//	 *
//	 * @return the exit code
//	 * @throws IOException if the platform failed to run
//	 */
//	public int execute() throws Exception {
////		String executableDir = platformConfig.getExecutablePath();
////		commandLine = new CommandLine(Paths.get(executableDir).toFile());
////
////		// List of benchmark parameters.
////		String jobId = getJobId();
////		String logDir = getLogPath();
////
////		// List of dataset parameters.
////		String inputPath = getInputPath();
////		String outputPath = getOutputPath();
////
////		// List of platform parameters.
////		int numThreads = platformConfig.getNumThreads();
////
////		appendBenchmarkParameters(jobId, logDir);
////		appendAlgorithmParameters();
////		appendDatasetParameters(inputPath, outputPath);
////		appendPlatformConfigurations(numThreads);
////
////
////		Executor executor = new DefaultExecutor();
////		executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));
////		executor.setExitValue(0);
////		return executor.execute(commandLine);
//	}

}
