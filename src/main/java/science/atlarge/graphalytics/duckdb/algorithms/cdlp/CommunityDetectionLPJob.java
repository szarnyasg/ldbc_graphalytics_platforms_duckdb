package science.atlarge.graphalytics.duckdb.algorithms.cdlp;

import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.duckdb.DuckdbConfiguration;
import science.atlarge.graphalytics.duckdb.DuckdbJob;
import science.atlarge.graphalytics.execution.RunSpecification;

import java.sql.Connection;
import java.sql.Statement;

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
		Statement stmt = connection.createStatement();

		// todo: tic-toc style alternations (ALTER TABLE .. RENAME TO ..) could also work
		CommunityDetectionLPParameters params =
				(CommunityDetectionLPParameters) runSpecification.getBenchmarkRun().getAlgorithmParameters();
		int maxIterations = params.getMaxIterations();

		// for n iterations, we need n+1 tables
		for (int i = 0; i <= maxIterations; i++) {
			System.out.println(String.format(
					"CREATE TABLE cdlp%d(id INTEGER, label INTEGER)", i
			));
			stmt.execute(String.format(
					"CREATE TABLE cdlp%d(id INTEGER, label INTEGER)", i
			));
		}
		stmt.execute("INSERT INTO cdlp0\n" +
				"SELECT id, id\n" +
				"FROM v");

		if (benchmarkGraph.isDirected()) {
			// create undirected *multigraph*
			stmt.execute("CREATE TABLE u AS\n" +
					"SELECT source AS n1, target AS n2\n" +
					"FROM e\n" +
					"UNION ALL\n" +
					"SELECT target AS n1, source AS n2\n" +
					"FROM e");
			stmt.execute("SELECT * FROM u;");
			System.out.println(stmt.getResultSet());

		}
		for (int i = 0; i < maxIterations; i++) {
			if (benchmarkGraph.isDirected()) {
				stmt.execute(String.format("INSERT INTO cdlp%d\n" +
						"SELECT id, label FROM (\n" +
						"    SELECT\n" +
						"        u.n1 AS id,\n" +
						"        cdlp%d.label AS label,\n" +
						"        ROW_NUMBER() OVER (PARTITION BY u.n1 ORDER BY count(*) DESC, cdlp%d.label ASC) AS seqnum\n" +
						"    FROM u, cdlp%d\n" +
						"    WHERE cdlp%d.id = u.n2\n" +
						"    GROUP BY\n" +
						"        u.n1,\n" +
						"        cdlp%d.label\n" +
						"    ) most_frequent_labels\n" +
						"WHERE seqnum = 1",
						i+1, i, i, i, i, i));
			} else {
				stmt.execute(String.format("INSERT INTO cdlp%d\n" +
						"SELECT id, label FROM (\n" +
						"    SELECT\n" +
						"        e.source AS id,\n" +
						"        cdlp%d.label AS label,\n" +
						"        ROW_NUMBER() OVER (PARTITION BY e.source ORDER BY count(*) DESC, cdlp%d.label ASC) AS seqnum\n" +
						"    FROM e, cdlp%d\n" +
						"    WHERE cdlp%d.id = e.target\n" +
						"    GROUP BY\n" +
						"        e.source,\n" +
						"        cdlp%d.label\n" +
						"    ) most_frequent_labels\n" +
						"WHERE seqnum = 1",
						i+1, i, i, i, i, i));
			}
		}
		stmt.execute(String.format(
				"COPY (SELECT id, label FROM cdlp%d ORDER BY id) TO '/tmp/cdlp.csv' WITH (DELIMITER ' ')", maxIterations
		));
		return 0;
	}

}
