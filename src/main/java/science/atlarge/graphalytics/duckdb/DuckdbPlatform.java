package science.atlarge.graphalytics.duckdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.domain.benchmark.BenchmarkRun;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.domain.graph.LoadedGraph;
import science.atlarge.graphalytics.execution.*;
import science.atlarge.graphalytics.duckdb.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.duckdb.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.duckdb.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.duckdb.algorithms.pr.PageRankJob;
import science.atlarge.graphalytics.duckdb.algorithms.sssp.SingleSourceShortestPathJob;
import science.atlarge.graphalytics.duckdb.algorithms.wcc.WeaklyConnectedComponents;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DuckdbPlatform implements Platform {

	protected static final Logger LOG = LogManager.getLogger();

	public static final String PLATFORM_NAME = "duckdb";
	public DuckdbLoader loader;

	public DuckdbPlatform() {
		System.out.println("init platform");
	}

	@Override
	public void verifySetup() throws Exception {

	}

	@Override
	public LoadedGraph loadGraph(FormattedGraph formattedGraph) throws Exception {
		DuckdbConfiguration platformConfig = DuckdbConfiguration.parsePropertiesFile();
		loader = new DuckdbLoader(formattedGraph, platformConfig);

		LOG.info("Loading graph " + formattedGraph.getName());
		Path loadedPath = Paths.get("./intermediate").resolve(formattedGraph.getName());

		try {
			int exitCode = loader.load(loadedPath.toString());
			if (exitCode != 0) {
				throw new PlatformExecutionException("DuckDB exited with an error code: " + exitCode);
			}
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to load a DuckDB dataset.", e);
		}
		LOG.info("Loaded graph " + formattedGraph.getName());
		return new LoadedGraph(formattedGraph, loadedPath.toString());
	}

	@Override
	public void deleteGraph(LoadedGraph loadedGraph) throws Exception {
		LOG.info("Unloading graph " + loadedGraph.getFormattedGraph().getName());
		try {
			System.out.println("TODO: unload");
			int exitCode = loader.unload(loadedGraph.getLoadedPath());
			if (exitCode != 0) {
				throw new PlatformExecutionException("DuckDB exited with an error code: " + exitCode);
			}
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to unload a DuckDB dataset.", e);
		}
		LOG.info("Unloaded graph " +  loadedGraph.getFormattedGraph().getName());
	}

	@Override
	public void prepare(RunSpecification runSpecification) throws Exception {

	}

	@Override
	public void startup(RunSpecification runSpecification) throws Exception {
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		Path logDir = benchmarkRunSetup.getLogDir().resolve("platform").resolve("runner.logs");
		DuckdbCollector.startPlatformLogging(logDir);
	}

	@Override
	public void run(RunSpecification runSpecification) throws PlatformExecutionException {
		BenchmarkRun benchmarkRun = runSpecification.getBenchmarkRun();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		RuntimeSetup runtimeSetup = runSpecification.getRuntimeSetup();

		Algorithm algorithm = benchmarkRun.getAlgorithm();
		DuckdbConfiguration platformConfig = DuckdbConfiguration.parsePropertiesFile();
		String inputPath = runtimeSetup.getLoadedGraph().getLoadedPath();
		String outputPath = benchmarkRunSetup.getOutputDir().resolve(benchmarkRun.getName()).toAbsolutePath().toString();
		Graph benchmarkGraph = benchmarkRun.getGraph();

		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:duckdb:/tmp/gx.duckdb");
		} catch (SQLException e) {
			e.printStackTrace();
			throw new PlatformExecutionException(e.getMessage());
		}

		DuckdbJob job;
		switch (algorithm) {
			case BFS:
				job = new BreadthFirstSearchJob(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
				break;
			case CDLP:
				job = new CommunityDetectionLPJob(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
				break;
			case LCC:
				job = new LocalClusteringCoefficientJob(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
				break;
			case PR:
				job = new PageRankJob(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
				break;
			case SSSP:
				job = new SingleSourceShortestPathJob(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
				break;
			case WCC:
				job = new WeaklyConnectedComponents(runSpecification, platformConfig, inputPath, outputPath, benchmarkGraph, connection);
				break;
			default:
				throw new PlatformExecutionException("Failed to load algorithm implementation.");
		}

		LOG.info("Executing benchmark with algorithm \"{}\" on graph \"{}\".",
				benchmarkRun.getAlgorithm().getName(),
				benchmarkRun.getFormattedGraph().getName());

		try {

			int exitCode = job.execute();
			if (exitCode != 0) {
				throw new PlatformExecutionException("DuckDB exited with an error code: " + exitCode);
			}
		} catch (Exception e) {
			throw new PlatformExecutionException("Failed to execute a DuckDB job.", e);
		}

		LOG.info("Executed benchmark with algorithm \"{}\" on graph \"{}\".",
				benchmarkRun.getAlgorithm().getName(),
				benchmarkRun.getFormattedGraph().getName());

	}

	@Override
	public BenchmarkMetrics finalize(RunSpecification runSpecification) throws Exception {
		DuckdbCollector.stopPlatformLogging();
		BenchmarkRunSetup benchmarkRunSetup = runSpecification.getBenchmarkRunSetup();
		Path logDir = benchmarkRunSetup.getLogDir().resolve("platform");

		BenchmarkMetrics metrics = new BenchmarkMetrics();
		metrics.setProcessingTime(DuckdbCollector.collectProcessingTime(logDir));
		return metrics;
	}

	@Override
	public void terminate(RunSpecification runSpecification) throws Exception {
		BenchmarkRunner.terminatePlatform(runSpecification);
	}

	@Override
	public String getPlatformName() {
		return PLATFORM_NAME;
	}
}
