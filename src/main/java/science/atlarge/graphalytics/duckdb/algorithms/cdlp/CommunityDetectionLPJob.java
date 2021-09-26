package science.atlarge.graphalytics.duckdb.algorithms.cdlp;

import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.duckdb.DuckDbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckDbJob;

public final class CommunityDetectionLPJob extends DuckDbJob {

	/**
	 * Creates a new LocalClusteringCoefficientJob object with all mandatory parameters specified.
	 *  @param platformConfig the platform configuration.
	 * @param inputPath the path to the input graph.
	 */
	public CommunityDetectionLPJob(RunSpecification runSpecification, DuckDbConfiguration platformConfig,
                                   String inputPath, String outputPath, Graph benchmarkGraph) {
		super(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph);
	}

	@Override
	protected void appendAlgorithmParameters() {
		commandLine.addArgument("--algorithm");
		commandLine.addArgument("cdlp");

		CommunityDetectionLPParameters params =
				(CommunityDetectionLPParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
		commandLine.addArgument("--max-iteration");
		commandLine.addArgument(Integer.toString(params.getMaxIterations()));
	}
}
