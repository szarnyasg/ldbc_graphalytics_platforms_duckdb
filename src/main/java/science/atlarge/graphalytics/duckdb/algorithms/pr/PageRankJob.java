package science.atlarge.graphalytics.duckdb.algorithms.pr;

import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.duckdb.DuckdbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckdbJob;

import java.sql.Connection;

public final class PageRankJob extends DuckdbJob {

    /**
     * Creates a new PageRankJob object with all mandatory parameters specified.
     *
     * @param platformConfig the platform configuration.
     * @param inputPath      the path to the input graph.
     */
    public PageRankJob(RunSpecification runSpecification, DuckdbConfiguration platformConfig,
                       String inputPath, String outputPath, Graph benchmarkGraph, Connection connection) {
        super(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
    }

    @Override
    public int execute() throws Exception {
        return 0;
    }

//    @Override
//    protected void appendAlgorithmParameters() {
//        commandLine.addArgument("--algorithm");
//        commandLine.addArgument("pr");
//
//        PageRankParameters params =
//                (PageRankParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
//        commandLine.addArgument("--damping-factor");
//        commandLine.addArgument(Float.toString(params.getDampingFactor()));
//        commandLine.addArgument("--max-iteration");
//        commandLine.addArgument(Integer.toString(params.getNumberOfIterations()));
//    }
}
