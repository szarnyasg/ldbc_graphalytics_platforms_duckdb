package science.atlarge.graphalytics.duckdb.algorithms.sssp;

import science.atlarge.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.duckdb.DuckDbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckDbJob;

public final class SingleSourceShortestPathJob extends DuckDbJob {

	/**
	 * Creates a new SingleSourceShortestPathJob object with all mandatory parameters specified.
	 *  @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public SingleSourceShortestPathJob(RunSpecification runSpecification, DuckDbConfiguration platformConfig,
									   String inputPath, String outputPath, Graph benchmarkGraph) {
		super(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph);
	}

	@Override
	protected void appendAlgorithmParameters() {
		commandLine.addArgument("--algorithm");
		commandLine.addArgument("sssp");

		SingleSourceShortestPathsParameters params =
				(SingleSourceShortestPathsParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
		commandLine.addArgument("--source-vertex");
		commandLine.addArgument(Long.toString(params.getSourceVertex()));

	}
}
