package science.atlarge.graphalytics.duckdb.algorithms.pr;

import science.atlarge.graphalytics.domain.algorithms.PageRankParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.duckdb.DuckDbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckDbJob;

public final class PageRankJob extends DuckDbJob {

    /**
     * Creates a new PageRankJob object with all mandatory parameters specified.
     *
     * @param platformConfig the platform configuration.
     * @param inputPath      the path to the input graph.
     */
    public PageRankJob(RunSpecification runSpecification, DuckDbConfiguration platformConfig,
                       String inputPath, String outputPath, Graph benchmarkGraph) {
        super(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph);
    }

    @Override
    protected void appendAlgorithmParameters() {
        commandLine.addArgument("--algorithm");
        commandLine.addArgument("pr");

        PageRankParameters params =
                (PageRankParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
        commandLine.addArgument("--damping-factor");
        commandLine.addArgument(Float.toString(params.getDampingFactor()));
        commandLine.addArgument("--max-iteration");
        commandLine.addArgument(Integer.toString(params.getNumberOfIterations()));
    }
}
