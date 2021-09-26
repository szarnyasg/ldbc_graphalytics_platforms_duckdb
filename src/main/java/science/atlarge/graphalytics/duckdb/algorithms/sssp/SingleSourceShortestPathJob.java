package science.atlarge.graphalytics.duckdb.algorithms.sssp;

import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.duckdb.DuckdbConfiguration;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.duckdb.DuckdbJob;

import java.sql.Connection;

public final class SingleSourceShortestPathJob extends DuckdbJob {

	/**
	 * Creates a new SingleSourceShortestPathJob object with all mandatory parameters specified.
	 *  @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public SingleSourceShortestPathJob(RunSpecification runSpecification, DuckdbConfiguration platformConfig,
									   String inputPath, String outputPath, Graph benchmarkGraph, Connection connection) {
		super(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
	}

	@Override
	public int execute() throws Exception {
		return 0;
	}

//	@Override
//	protected void appendAlgorithmParameters() {
//		commandLine.addArgument("--algorithm");
//		commandLine.addArgument("sssp");
//
//		SingleSourceShortestPathsParameters params =
//				(SingleSourceShortestPathsParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
//		commandLine.addArgument("--source-vertex");
//		commandLine.addArgument(Long.toString(params.getSourceVertex()));
//
//	}
}
