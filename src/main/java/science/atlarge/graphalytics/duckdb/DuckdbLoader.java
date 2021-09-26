package science.atlarge.graphalytics.duckdb;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.exec.util.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;

import java.io.File;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;


public class DuckdbLoader {

	private static final Logger LOG = LogManager.getLogger();

	protected CommandLine commandLine;
	protected FormattedGraph formattedGraph;
	protected DuckdbConfiguration platformConfig;

	/**
	 *	Graph loader for DuckDB.
	 * @param formattedGraph
	 * @param platformConfig
	 */
	public DuckdbLoader(FormattedGraph formattedGraph, DuckdbConfiguration platformConfig) {
		this.formattedGraph = formattedGraph;
		this.platformConfig = platformConfig;
	}

	public int load(String loadedInputPath) throws Exception {
		String loaderDir = platformConfig.getLoaderPath();
		commandLine = new CommandLine(Paths.get(loaderDir).toFile());

		commandLine.addArgument("--graph-name");
		commandLine.addArgument(formattedGraph.getName());
		commandLine.addArgument("--input-vertex-path");
		commandLine.addArgument(formattedGraph.getVertexFilePath());
		commandLine.addArgument("--input-edge-path");
		commandLine.addArgument(formattedGraph.getEdgeFilePath());
		commandLine.addArgument("--output-path");
		commandLine.addArgument(loadedInputPath);
		commandLine.addArgument("--directed");
		commandLine.addArgument(formattedGraph.isDirected() ? "true" : "false");
		commandLine.addArgument("--weighted");
		commandLine.addArgument(formattedGraph.hasEdgeProperties() ? "true" : "false");

		String commandString = StringUtils.toString(commandLine.toStrings(), " ");
		LOG.info(String.format("Execute graph loader with command-line: [%s]", commandString));

		Executor executor = new DefaultExecutor();
		executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));
		executor.setExitValue(0);

		return executor.execute(commandLine);

//		File dbFile = new File("/tmp/gx.duckdb");
//		dbFile.delete();
//
//		String sqlCommands = "";
//		sqlCommands += "CREATE TABLE v(id INTEGER); ";
//		sqlCommands += String.format("COPY v (id) FROM '%s' (DELIMITER ' ', FORMAT csv); ",
//				formattedGraph.getVertexFilePath()
//		);
//		sqlCommands += String.format("CREATE TABLE e(source INTEGER, target INTEGER%s); ",
//				formattedGraph.hasEdgeProperties() ? ", weight DOUBLE" : ""
//		);
//		sqlCommands += String.format("COPY e (source, target%s) FROM '%s' (DELIMITER ' ', FORMAT csv); ",
//				formattedGraph.hasEdgeProperties() ? ", weight" : "",
//				formattedGraph.getEdgeFilePath()
//		);
//
//		String loaderDir = platformConfig.getLoaderPath();
//		commandLine = new CommandLine(Paths.get(loaderDir).toFile());
//		commandLine.addArgument(sqlCommands);
//
//		String commandString = StringUtils.toString(commandLine.toStrings(), " ");
//		LOG.info(String.format("Execute graph loader with command-line: [%s]", commandString));
//
//		System.out.println("XXXXX: " + sqlCommands);
//
//		Executor executor = new DefaultExecutor();
//		executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));
//		executor.setExitValue(0);
//
//		return executor.execute(commandLine);
	}

	public int unload(String loadedInputPath) throws Exception {
		String unloaderDir = platformConfig.getUnloaderPath();
		commandLine = new CommandLine(Paths.get(unloaderDir).toFile());

		commandLine.addArgument("--graph-name");
		commandLine.addArgument(formattedGraph.getName());

		commandLine.addArgument("--output-path");
		commandLine.addArgument(loadedInputPath);

		String commandString = StringUtils.toString(commandLine.toStrings(), " ");
		LOG.info(String.format("Execute graph unloader with command-line: [%s]", commandString));

		Executor executor = new DefaultExecutor();
		executor.setStreamHandler(new PumpStreamHandler(System.out, System.err));
		executor.setExitValue(0);

		return executor.execute(commandLine);
	}

}
