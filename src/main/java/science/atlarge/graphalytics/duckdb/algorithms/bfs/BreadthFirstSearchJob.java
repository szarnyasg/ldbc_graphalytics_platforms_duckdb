package science.atlarge.graphalytics.duckdb.algorithms.bfs;

import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.duckdb.DuckdbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckdbJob;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.sql.Connection;

public final class BreadthFirstSearchJob extends DuckdbJob {

	/**
	 * Creates a new BreadthFirstSearchJob object with all mandatory parameters specified.
	 *  @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public BreadthFirstSearchJob(RunSpecification runSpecification, DuckdbConfiguration platformConfig,
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
//		commandLine.addArgument("bfs");
//
//		BreadthFirstSearchParameters params =
//				(BreadthFirstSearchParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
//		commandLine.addArgument("--source-vertex");
//		commandLine.addArgument(Long.toString(params.getSourceVertex()));
//
//	}
}
