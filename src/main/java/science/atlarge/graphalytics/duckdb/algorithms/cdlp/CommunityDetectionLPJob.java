package science.atlarge.graphalytics.duckdb.algorithms.cdlp;

import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.duckdb.DuckdbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckdbJob;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.sql.Connection;

public final class CommunityDetectionLPJob extends DuckdbJob {

	/**
	 * Creates a new LocalClusteringCoefficientJob object with all mandatory parameters specified.
	 *  @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public CommunityDetectionLPJob(RunSpecification runSpecification, DuckdbConfiguration platformConfig,
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
//		commandLine.addArgument("cdlp");
//
//		CommunityDetectionLPParameters params =
//				(CommunityDetectionLPParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
//		commandLine.addArgument("--max-iteration");
//		commandLine.addArgument(Integer.toString(params.getMaxIterations()));
//	}
}
